# STEDI Human Balance Analytics: A Data Lakehouse Solution for Machine Learning

This repository documents the design, implementation, and operationalization of a data lakehouse solution on the AWS platform. The primary objective of this project is to build a robust, scalable, and privacy-compliant data pipeline that transforms raw sensor and mobile application data into a curated, governed dataset. This dataset serves as a feature store for a machine learning model designed to accurately detect user steps from motion sensor data.

## Project Overview & Objective

The STEDI data science team requires a reliable and governed dataset to train and deploy their machine learning model. This solution addresses that business need by establishing a comprehensive ETL (Extract, Transform, Load) pipeline that ensures data quality, governance, and accessibility.

The key objectives of this project were to:

- **Ingest** semi-structured data from disparate sources (IoT sensors, mobile applications) into a centralized, highly-scalable data lake.
- **Apply ETL logic** to cleanse, validate, and enrich the raw data, ensuring its fitness for purpose.
- **Enforce data governance** by implementing strict access controls and ensuring only data from customer-consented sources is processed for machine learning purposes.
- **Curate** a final, joined dataset optimized for direct consumption by machine learning algorithms and business intelligence tools.

## Solution Architecture

![Architecture](images/diagrams/Architecture_diagram.drawio.svg)

The data pipeline is designed as a three-zone data lakehouse architecture, leveraging key AWS services for ingestion, transformation, and storage. The flow is as follows:

1. **Landing Zone (Raw Data):** Raw data streams are ingested in their original JSON format and stored in a designated S3 bucket. Metadata is registered in the AWS Glue Data Catalog, allowing for immediate discoverability and ad-hoc querying via Amazon Athena.

   - **Customer Data:** Raw customer records from fulfillment and the STEDI website.
     - `customer_landing` Table
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/customer/landing/`
   - **Accelerometer Data:** Sensor data from the mobile application.
     - `accelerometer_landing` Table
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/accelerometer/landing/`
   - **Step Trainer Data:** Motion sensor data from the physical device.
     - `step_trainer_landing` Table
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/step_trainer/landing/`

2. **Trusted Zone (Validated & Sanitized Data):** Data is moved from the Landing Zone to the Trusted Zone after undergoing initial data quality and privacy validation. This zone serves as the "single source of truth" for consented data, ensuring all subsequent processing adheres to governance policies. The data in this zone is also stored in JSON format.

   - **`customer_trusted` Table:** Filters the raw customer data to include only records from individuals who have explicitly consented to share their data for research (where `sharewithresearchasofdate` is not blank).
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/customer/trusted/`
   - **`accelerometer_trusted` Table:** Only includes accelerometer readings from customers who have consented to data sharing.
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/accelerometer/trusted/`
   - **`step_trainer_trusted` Table:** A table of step trainer records that only includes data for customers in the `customers_curated` table.
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/step_trainer/trusted/`

3. **Curated Zone (Aggregated & Optimized Data):** This final layer acts as the machine learning feature store. Data is aggregated, joined, and optimized for high-performance read access by data scientists. All personally identifiable information (PII) is removed to ensure privacy compliance. The data in this zone is also stored in JSON format.

   - **`customers_curated` Table:** A refined list of customers who have both accelerometer data and have consented to data sharing.
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/customer/curated/`
   - **`machine_learning_curated` Table:** The final dataset, created by joining step trainer readings with corresponding accelerometer data on a shared timestamp. This table is anonymized and ready for model training.
     - AWS S3 Bucket URI: `s3://eligero-stedi-data-lakehouse/step_trainer/curated/`

### Implementation

This repository contains the core technical files of the project, organized by function.

#### 1. SQL DDL Scripts

These SQL DDL (Data Definition Language) scripts are used to create the external tables in the AWS Glue Data Catalog, enabling schema-on-read for the raw JSON data.

- `SQL/customer_landing.sql`: Defines the schema for the raw customer data ingested into the landing zone.
- `SQL/accelerometer_landing.sql`: Defines the schema for raw accelerometer data from the mobile app.
- `SQL/step_trainer_landing.sql`: Defines the schema for raw motion sensor data from the step trainer device.

#### 2. AWS Glue Scripts (PySpark)

These Python scripts, utilizing PySpark, orchestrate the ETL jobs within the AWS Glue environment.

- `Glue_scripts/customer_landing_to_trusted.py`: Filters customer data to include only records from individuals who have consented to research.
- `Glue_scripts/accelerometer_landing_to_trusted.py`: Joins accelerometer data with consented customer records to move validated data into the trusted zone.
- `Glue_scripts/customer_trusted_to_curated.py`: Creates a curated customer dataset containing only records with corresponding accelerometer data, ensuring data integrity.
- `Glue_scripts/step_trainer_trusted.py`: Joins step trainer data with the curated customer dataset to prepare it for the trusted zone.
- `Glue_scripts/machine_learning_curated.py`: Creates the final, aggregated dataset for the machine learning model by joining accelerometer and step trainer data on a common timestamp.

#### 3. Data Validation & Audit

As part of the quality assurance process, Athena queries were executed at each stage of the pipeline to validate data counts and transformations. The following screenshots serve as a visual audit trail of the data's journey through the lakehouse.

1. **Landing Zone**
   - `customer_landing` Table
     - _Ingested Count:_ 956 records
     - Screenshoot: `customer_landing.jpeg`
   - `accelerometer_landing` Table
     - _Ingested Count:_ 81,273 records
     - Screenshoot: `accelerometer_landing.jpeg`
   - `step_trainer_landing` Table
     - _Ingested Count:_ 28,680 records
     - Screenshoot: `step_trainer_landing.jpeg`

2. **Trusted Zone**

   - **`customer_trusted` Table**
     - _Expected Count:_ 482 records
     - Screenshoot: `customer_trusted.jpeg`
   - **`accelerometer_trusted` Table**
     - _Expected Count:_ 40,981 records
     - Screenshoot: `accelerometer_trusted.jpeg`
   - **`step_trainer_trusted` Table**
     - _Expected Count:_ 14,460 records
     - Screenshoot: `step_trainer_trusted.jpeg`

3. **Curated Zone**

   - **`customers_curated` Table**
     - _Expected Count:_ 482 records
     - Screenshoot: `customer_curated.jpeg`
   - **`machine_learning_curated` Table**
     - _Expected Count:_ 43,681 records
     - Screenshoot: `machine_learning_curated.jpeg`

### Technology Stack

- **Data Lake Storage:** AWS S3
- **ETL & Data Processing:** AWS Glue (PySpark)
- **Data Catalog & Querying:** AWS Glue Data Catalog & Amazon Athena
- **Programming Language:** Python, Spark SQL

### Repository Contents

- `SQL/`: SQL DDL scripts for creating the initial Glue tables in the Landing Zone.
- `Glue_scripts/`: Python scripts for each of the AWS Glue ETL jobs.
- `images/screenshoots`: Screenshoots of Data Validation and Audit.
- `images/diagrams`: A visual representation of the overall data pipeline, from raw data ingestion in the Landing Zone to the final curated data ready for machine learning.
