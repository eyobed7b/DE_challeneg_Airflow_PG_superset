-- ml.dim_banking_client definition

-- Drop table

-- DROP TABLE ml.dim_banking_client;

CREATE TABLE ml.dim_banking_client (
	client_skey serial4 NOT NULL,
	age int4 NOT NULL,
	job varchar(50) NOT NULL,
	marital varchar(15) NOT NULL,
	education varchar(20) NULL,
	"default" bool NULL,
	CONSTRAINT dim_banking_client_age_job_marital_education_default_key UNIQUE (age, job, marital, education, "default"),
	CONSTRAINT dim_banking_client_pkey PRIMARY KEY (client_skey)
);


-- ml.dim_country definition

-- Drop table

-- DROP TABLE ml.dim_country;

CREATE TABLE ml.dim_country (
	country_skey serial4 NOT NULL,
	country_code bpchar(3) NOT NULL,
	country_name varchar(100) NOT NULL,
	CONSTRAINT dim_country_country_code_key UNIQUE (country_code),
	CONSTRAINT dim_country_pkey PRIMARY KEY (country_skey)
);


-- ml.dim_indicator definition

-- Drop table

-- DROP TABLE ml.dim_indicator;

CREATE TABLE ml.dim_indicator (
	indicator_skey serial4 NOT NULL,
	indicator_code varchar(50) NOT NULL,
	indicator_name varchar(255) NOT NULL,
	CONSTRAINT dim_indicator_indicator_code_key UNIQUE (indicator_code),
	CONSTRAINT dim_indicator_pkey PRIMARY KEY (indicator_skey)
);


-- ml.dim_school definition

-- Drop table

-- DROP TABLE ml.dim_school;

CREATE TABLE ml.dim_school (
	school_skey serial4 NOT NULL,
	ope_id varchar(10) NOT NULL,
	school_name varchar(255) NOT NULL,
	school_type varchar(50) NULL,
	zip_code varchar(10) NULL,
	state_code varchar(10) NULL,
	CONSTRAINT dim_school_ope_id_key UNIQUE (ope_id),
	CONSTRAINT dim_school_pkey PRIMARY KEY (school_skey)
);


-- ml.fact_banking_campaign definition

-- Drop table

-- DROP TABLE ml.fact_banking_campaign;

CREATE TABLE ml.fact_banking_campaign (
	campaign_skey serial4 NOT NULL,
	client_skey int4 NULL,
	contact_date date NOT NULL,
	duration_seconds int4 NULL,
	campaign_contacts int2 NULL,
	pdays_since_last_contact int4 NULL,
	previous_contacts int2 NULL,
	target_subscribed bool NULL,
	contact_method varchar(15) NULL,
	poutcome_result varchar(50) NULL,
	balance numeric(18, 2) NULL,
	contact varchar(20) NULL,
	duration int4 NULL,
	campaign int4 NULL,
	poutcome varchar(50) NULL,
	previous int4 NULL,
	pdays int4 NULL,
	subscribed bool DEFAULT false NOT NULL,
	CONSTRAINT fact_banking_campaign_pkey PRIMARY KEY (campaign_skey)
);


-- ml.fact_student_loan definition

-- Drop table

-- DROP TABLE ml.fact_student_loan;

CREATE TABLE ml.fact_student_loan (
	loan_agg_skey int4 DEFAULT nextval('ml.fact_student_loan_agg_loan_agg_skey_seq'::regclass) NOT NULL,
	ope_id varchar(10) NOT NULL,
	academic_year varchar(10) NOT NULL,
	quarter_n int4 NOT NULL,
	recipients_n int8 NOT NULL,
	loans_originated_n int8 NOT NULL,
	loan_amount_usd numeric(18, 2) NOT NULL,
	disbursement_amount_usd numeric(18, 2) NOT NULL,
	CONSTRAINT fact_student_loan_agg_ope_id_academic_year_quarter_n_key UNIQUE (ope_id, academic_year, quarter_n),
	CONSTRAINT fact_student_loan_agg_pkey PRIMARY KEY (loan_agg_skey)
);


-- ml.report_marketing_metrics definition

-- Drop table

-- DROP TABLE ml.report_marketing_metrics;

CREATE TABLE ml.report_marketing_metrics (
	job varchar(50) NULL,
	marital varchar(15) NULL,
	total_contacts int4 NULL,
	total_subscribed int4 NULL,
	conversion_rate numeric NULL
);