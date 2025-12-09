from __future__ import annotations

import pendulum
import pandas as pd
import io
import logging
import os
import json 
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 1. CONFIGURATION AND UTILS ---

# Define Paths and Connection ID
DATA_DIR = '/opt/tmp/' 
BANKING_CSV_LOCAL = os.path.join(DATA_DIR, 'banking_marketing.csv')
STUDENT_LOAN_CSV_LOCAL = os.path.join(DATA_DIR, 'student_loan_2009_2010.csv') 
WORLD_BANK_JSON_LOCAL = os.path.join(DATA_DIR, 'world_bank_indicators.json')
POSTGRES_CONN_ID = 'mdc2_pg' 

# --- WarehouseLoader (Using insert_rows for robust loading) ---

class WarehouseLoader:
    """Handles loading DataFrames into PostgreSQL using standard insert_rows."""
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)

    def _execute_insert_rows(self, df: pd.DataFrame, table_name: str):
        """Internal helper for standard insert_rows load."""
        
        # 1. Clean column names for PostgreSQL and prepare for insert
        df.columns = [col.replace('"', '').replace('.', '_') for col in df.columns] 
        # Handle the PostgreSQL reserved keyword 'default' if it exists in the DataFrame
        if 'default' in df.columns:
             df.rename(columns={'default': '"default"'}, inplace=True)
        
        target_columns = df.columns.tolist()
        
        # Convert DataFrame rows to a list of tuples for insert_rows
        data_to_insert = [tuple(row) for row in df.values]
        
        try:
            self.hook.insert_rows(
                table=table_name,
                rows=data_to_insert,
                target_fields=target_columns,
                commit_every=1000  # Commit in batches of 1000 rows
            )
            logger.info(f"Successfully loaded {len(df)} rows into table: {table_name} using insert_rows.")
        except Exception as e:
            logger.error(f"Error executing insert_rows into {table_name}. Check DDL and data types: {e}")
            raise 

    def load_dataframe(self, df: pd.DataFrame, table_name: str, write_disposition: str = "WRITE_TRUNCATE", **kwargs):
        """Main dispatcher for load operations."""
        if df.empty:
            logger.warning(f"Skipping load for empty DataFrame into {table_name}.")
            return
            
        if write_disposition == "WRITE_TRUNCATE":
            logger.info(f"Truncating table {table_name}...")
            # Use RESTART IDENTITY to reset SERIAL primary keys
            self.hook.run(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;") 
            self._execute_insert_rows(df, table_name) 
        else:
             # For other dispositions (like UPSERT, though insert_rows is used here)
             logger.warning(f"Unsupported write disposition '{write_disposition}'. Defaulting to TRUNCATE + INSERT.")
             self.hook.run(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;") 
             self._execute_insert_rows(df, table_name)


# --- 2. PYTHON TASK FUNCTIONS (ETL LOGIC) ---

def extract_data(**kwargs) -> Dict[str, str]:
    """Task to extract raw data from local CSV and JSON files."""
    logger.info("Starting extraction from local directory.")
    try:
        banking_df = pd.read_csv(BANKING_CSV_LOCAL, sep=',')
        student_loan_df = pd.read_csv(STUDENT_LOAN_CSV_LOCAL) 
        with open(WORLD_BANK_JSON_LOCAL, 'r') as f:
            world_bank_raw = json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}. Ensure all files are in {DATA_DIR}")
        raise
    
    return {
        'banking_raw': banking_df.to_json(),
        'student_raw': student_loan_df.to_json(),
        'world_bank_raw': json.dumps(world_bank_raw) 
    }
        
def clean_banking_data(**kwargs) -> Dict[str, str]:
    """Cleans and prepares Banking Marketing data."""
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(task_ids='extract_data')['banking_raw'])
    df.columns = df.columns.str.lower().str.replace('.', '_', regex=False)
    df.rename(columns={'y': 'subscribed'}, inplace=True)
    df['subscribed'] = df['subscribed'].map({'yes': True, 'no': False})
    
    for col in ['job', 'education', 'contact', 'poutcome']:
        df[col] = df[col].replace('unknown', 'unknown_group').fillna('unknown_group')
        
    # 1. Create datetime object 
    df['contact_date'] = pd.to_datetime(
        df['day'].astype(str) + '-' + df['month'], 
        format='%d-%b', 
        errors='coerce'
    ).fillna(pd.to_datetime('2024-01-01'))

    # 2. CRITICAL FIX: Convert datetime object to ISO-format string (YYYY-MM-DD)
    df['contact_date'] = df['contact_date'].dt.strftime('%Y-%m-%d')
    
    df = df.drop(columns=['day', 'month', 'housing', 'loan'])
    return {'banking_clean': df.to_json()}


def clean_student_loan_data(**kwargs) -> Dict[str, str]:
    """Cleans and transforms aggregated student loan data (2009-2010)."""
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(task_ids='extract_data')['student_raw'])
    
    id_vars = ['OPE ID', 'School', 'State', 'Zip Code', 'School Type']
    all_quarter_cols = df.columns.difference(id_vars).tolist()
    
    value_vars = [col for col in all_quarter_cols if not col.startswith('Disbursements Q')]
    
    # Melt/Unpivot
    df_long = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='Metric_Quarter',
        value_name='Value'
    ).dropna(subset=['Value'])

    # Clean and Convert 'Value' to Numeric
    df_long['Value'] = df_long['Value'].astype(str).str.replace(r'[$,]', '', regex=True)
    df_long['Value'] = pd.to_numeric(df_long['Value'], errors='coerce') 
    df_long.dropna(subset=['Value'], inplace=True) 

    # Extract Quarter Number and Metric Type
    df_long['quarter_n'] = df_long['Metric_Quarter'].str.extract(r'Q(\d+)').astype(int)
    df_long['metric_type'] = df_long['Metric_Quarter'].str.replace(r' Q\d+', '', regex=True).str.strip()
    
    # Final Pivot
    df_agg = df_long.pivot_table(
        index=id_vars + ['quarter_n'],
        columns='metric_type',
        values='Value',
        aggfunc='sum' 
    ).reset_index()
    
    df_agg.columns.name = None
    final_rename_map = {
        'OPE ID': 'ope_id', 'School': 'school_name', 'State': 'state_code', 
        'Zip Code': 'zip_code', 'School Type': 'school_type',
        'Recipients': 'recipients_n', 'Loans Originated': 'loans_originated_n',
        'Loan Amount (USD)': 'loan_amount_usd', 'Disbursement Amount (USD)': 'disbursement_amount_usd'
    }
    
    df_agg.rename(columns=final_rename_map, inplace=True)
    df_agg['academic_year'] = '2009-2010'
    
    # Separate Dimension and Fact tables
    dim_school = df_agg[['ope_id', 'school_name', 'state_code', 'zip_code', 'school_type']].drop_duplicates()
    
    fact_loan_agg = df_agg[['ope_id', 'academic_year', 'quarter_n', 
                            'recipients_n', 'loans_originated_n', 
                            'loan_amount_usd', 'disbursement_amount_usd']]
    
    return {
        'dim_school': dim_school.to_json(),
        'fact_loan_agg': fact_loan_agg.to_json()
    }

def flatten_and_clean_world_bank_data(**kwargs) -> Dict[str, str]:
    """Flattens the deeply nested World Bank data, separating Facts and Dimensions."""
    ti = kwargs['ti']
    raw_data_list = json.loads(ti.xcom_pull(task_ids='extract_data')['world_bank_raw'])
    
    records = []
    
    for country_record in raw_data_list:
        country_code = country_record.get("Country Code")
        country_name = country_record.get("Country Name")
        for indicator_record in country_record.get("Indicators", []):
            indicator_code = indicator_record.get("Indicator Code")
            indicator_name = indicator_record.get("Indicator Name")
            for year_record in indicator_record.get("Years", []):
                value = year_record.get("Indicator Value for Year")
                records.append({
                    'country_code': country_code,
                    'country_name': country_name,
                    'indicator_code': indicator_code,
                    'indicator_name': indicator_name,
                    'year': year_record.get("Year"),
                    'value': value if value != "" else pd.NA
                })
    
    world_bank_flat_df = pd.DataFrame(records)
    
    # Cleaning and Type Conversion
    world_bank_flat_df['value'] = pd.to_numeric(world_bank_flat_df['value'], errors='coerce')
    world_bank_flat_df.dropna(subset=['year', 'value', 'country_code', 'indicator_code'], inplace=True)
    world_bank_flat_df['year'] = world_bank_flat_df['year'].astype(int)

    # Separate Dimensions and Facts
    dim_country = world_bank_flat_df[['country_code', 'country_name']].drop_duplicates()
    dim_indicator = world_bank_flat_df[['indicator_code', 'indicator_name']].drop_duplicates()
    fact_indicator_value = world_bank_flat_df[['country_code', 'indicator_code', 'year', 'value']]
    
    return {
        'fact_indicator_value': fact_indicator_value.to_json(),
        'dim_country': dim_country.to_json(),
        'dim_indicator': dim_indicator.to_json()
    }

def run_analytical_transformations(**kwargs):
    """Runs analytical aggregations (Marketing Metrics) and prepares features."""
    ti = kwargs['ti']
    banking_data = pd.read_json(ti.xcom_pull(task_ids='clean_banking_data')['banking_clean'])
 
    
    # 1. Marketing Metrics (for REPORT_MARKETING_METRICS)
    banking_metrics = banking_data.groupby(['job', 'marital'])['subscribed'].agg(
        ['size', 'sum', 'mean'] 
    ).reset_index()

    banking_metrics.columns = [
        'job', 'marital', 
        'total_contacts', 
        'total_subscribed', 
        'conversion_rate'
    ]
    ti.xcom_push(key='banking_metrics', value=banking_metrics.to_json())
    
 

def load_all_data_to_warehouse(**kwargs):
    """Loads all data into the PostgreSQL DW using TRUNCATE/INSERT."""
    ti = kwargs['ti']
    loader = WarehouseLoader(POSTGRES_CONN_ID)
    
    # --- 1. Retrieve Data ---
    dim_country = pd.read_json(ti.xcom_pull(task_ids='flatten_and_clean_world_bank_data')['dim_country'])
    dim_indicator = pd.read_json(ti.xcom_pull(task_ids='flatten_and_clean_world_bank_data')['dim_indicator'])
    dim_school = pd.read_json(ti.xcom_pull(task_ids='clean_student_loan_data')['dim_school'])
    
    banking_raw_data = pd.read_json(ti.xcom_pull(task_ids='clean_banking_data')['banking_clean']) # Source for fact/dim
    
    fact_indicator_value_raw = pd.read_json(ti.xcom_pull(task_ids='flatten_and_clean_world_bank_data')['fact_indicator_value'])
    loan_agg_data = pd.read_json(ti.xcom_pull(task_ids='clean_student_loan_data')['fact_loan_agg'])
    report_marketing_metrics = pd.read_json(ti.xcom_pull(task_ids='run_analytical_transformations', key='banking_metrics'))
    # ml_features = pd.read_json(ti.xcom_pull(task_ids='run_analytical_transformations', key='ml_features')) # REMOVED
    
    # --- 2. Load Dimensions (WRITE_TRUNCATE) ---
    
    loader.load_dataframe(dim_country, 'ml.dim_country', write_disposition='WRITE_TRUNCATE')
    loader.load_dataframe(dim_indicator, 'ml.dim_indicator', write_disposition='WRITE_TRUNCATE')
    loader.load_dataframe(dim_school, 'ml.dim_school', write_disposition='WRITE_TRUNCATE')
    
    # dim_banking_client (Uses TRUNCATE/LOAD)
    dim_client = banking_raw_data[['age', 'job', 'marital', 'education', 'default']].drop_duplicates()
    dim_client = dim_client.rename(columns={'default': '"default"'}).drop_duplicates() 
    loader.load_dataframe(dim_client, 'ml.dim_banking_client', write_disposition='WRITE_TRUNCATE')
    
    # --- 3. Prepare and Load fact_banking_campaign (CRITICAL FILTERING) ---
    
    # List all columns expected in the fact table DDL
    FACT_CAMPAIGN_COLUMNS = [
        'balance', 'contact', 'duration', 'campaign', 'pdays', 'previous', 'poutcome', 'subscribed',
        'contact_date',
    ]

    fact_banking_campaign_final = banking_raw_data[FACT_CAMPAIGN_COLUMNS].copy()

    # Load the fact table
    loader.load_dataframe(fact_banking_campaign_final, 'ml.fact_banking_campaign', write_disposition='WRITE_TRUNCATE')
    
    # --- 4. Load Remaining Facts and Reports (TRUNCATE/LOAD) ---
    
    # Fact tables
    loader.load_dataframe(loan_agg_data, 'ml.fact_student_loan', write_disposition='WRITE_TRUNCATE') 
    loader.load_dataframe(fact_indicator_value_raw, 'ml.fact_indicator_value', write_disposition='WRITE_TRUNCATE') 

    # Reporting & ML Tables
    loader.load_dataframe(report_marketing_metrics, 'ml.report_marketing_metrics', write_disposition='WRITE_TRUNCATE')
    # loader.load_dataframe(ml_features, 'ml.ml_client_features', write_disposition='WRITE_TRUNCATE') # REMOVED
    
    logger.info("All data loading complete.")


# --- 3. AIRFLOW DAG DEFINITION (Flow) ---

with DAG(
    dag_id='etl_data_challenge_pipeline',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'challenge', 'postgres', 'data_engineering', 'local_source'],
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    # E: Extraction Stage 
    extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data)

    # T1: Cleaning & Transformation Stage (Parallel)
    clean_banking_task = PythonOperator(task_id='clean_banking_data', python_callable=clean_banking_data)
    clean_student_task = PythonOperator(task_id='clean_student_loan_data', python_callable=clean_student_loan_data)
    flatten_world_bank_task = PythonOperator(task_id='flatten_and_clean_world_bank_data', python_callable=flatten_and_clean_world_bank_data)
    
    # T2: Analytical Transformation & ML Feature Prep (Depends on clean data)
    analytical_transform_task = PythonOperator(task_id='run_analytical_transformations', python_callable=run_analytical_transformations)
    
    # L: Loading Stage (Depends on all preceding tasks)
    load_warehouse_task = PythonOperator(task_id='load_all_data_to_warehouse', python_callable=load_all_data_to_warehouse)
    
    # Define the DAG Dependencies 
    extract_task >> [
        clean_banking_task, 
        clean_student_task, 
        flatten_world_bank_task
    ]
    
    # Analytical task depends on clean_banking and clean_student (though only banking is used now)
    [clean_banking_task, clean_student_task, flatten_world_bank_task] >> analytical_transform_task 

    # Load task depends on all previous tasks
    [clean_banking_task, clean_student_task, flatten_world_bank_task, analytical_transform_task] >> load_warehouse_task