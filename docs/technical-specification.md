# RetailPulse - Technical specification

## Section - 1: Problem Statement

### What problem does RetailPulse solve?
RetailPulse is a cloud-native data lakehouse platform built on GCP that ingests, transforms and serves WooCommerce e-commerce data for analytics and reporting. It centralizes the data coming from WooCommerce platform and without this, analysts query the WooCommerce transactional database causing slower reports, production db load and no historical view of how the data has changed overtime.

### Stakeholders and their needs
|--------------------------------------------------------------------------------------------------------------------------|
| Stakeholder   | What they need from RetailPulse                                                                          |
|---------------|----------------------------------------------------------------------------------------------------------|
| Product Owner | They will be the owner of the report created by using data from RetailPulse and they will coordinate with|
|               | regional analytics teams of every country as per the data provided by RetailPulse                        |
| Data Analyst  | They need clean and processed data in BigQuery to build Power BI report                                  |
| Data Scientist| They need historical data for churn and LTV modelling                                                    |
| Data Engineer | They need RetailPulse as one the clean data source for other pipeline formation project                  |
|--------------------------------------------------------------------------------------------------------------------------|

### Success metrics
- Data freshness SLA: Data in gold layer will not be stale for more than 24 hrs relative to previous day's WooCommerce activity and will be refreshed everyday at 1:30 pm CET
- Pipeline reliability SLA: 99% of daily run will be completed without manual intervention

### Technology choices
**GCP:** It provides ingestion, transformation, orchestration and warehousing solution under one umbrella with cost effectiveness.

**PySpark:** A powerful python framework for large scale data processing with robust optimization strategies and can be used easily with GCP services.

## Section 2: Scope

### In scope for v1
- We will ingest data related to Customers, orders, products and refunds
- We will be simulating CDC from WooCommerce
- Data drift monitoring will be implemented to identify unexpected changes in content, structure or meaning of data
- Schema evolution handling  will be implemented to handle schema changes while ingestion
- SCD Type 1 will be implemented on dim_products to overwrite changed product attributes without preserving history
- SCD Type 2 will be implemented on dim_customers to preserve full history of customer attribute changes with effective_from, effective_to and is_current flags
- Unit testing will be done by Pytest to ensure the isolation of individual component and to identify and mitigate the bugs 
- Medallion architecture (bronze/silver/gold) will be implemented as the core pipeline design
- Processed data will be stored in Bigquery (gold layer) in form of table and views
- Dataset from Bigquery will be consumed by Power Bi for Business reporting

### Out of scope for v1
- Real time streaming is out of scope - batch ingestion satisfies the 24 hour freshness SLA at significantly lower operational complexity and cost
- ML model training is out of scope - Data will be used for Business report and to calculate churn and LTV modelling
- Multi store WooCommerce support is out of scope - v1 is designed for a single store instance; multi-tenancy adds schema and auth complexity that is not required for the current use case
- Self-serve BI dashboard development is out of scope - RetailPulse serves clean data to BigQuery; dashboard design and maintenance is owned by the Data Analyst team

## Section 3: Architecture Overview

### Narrative
The data flows through REST API from WooCommerce platform to GCP and landing area is the GCS buckets where it gets stored in raw form. In parallel, WooCommerce webhooks capture real-time change event, order updates and customer modifications, which are consumed as CDC events and landed in GCS before flowing through the same Bronze/Silver/Gold pipeline. Dataproc writes cleaned data back to GCS as the silver layer and loads curated data into BigQuery as the gold layer. Inside BigQuery, Dataform models the curated data into Star schema, Data Vault 2.0 and OBT structures which are then consumed by Power BI for business reporting. The whole pipeline is orchestrated by Cloud Composer.

### Layer definition
|-----------------------------------------------------------------------------------------------------------------------|
|  Layer  |  Location  |  Format  |  GCP Services  |                    Purpose                                         |
|---------|------------|----------|----------------|--------------------------------------------------------------------|
| Bronze  | GCS        | Parquet  |  Dataproc      | Landing of raw data for back-up and further transformation         |
| Silver  | GCS        | ORC      |  Dataproc      | Cleansed, conformed and historised data. SCD Type 1 applied to     |
|         |            |          |                | dim_products, SCD Type 2 applied to dim_customers. Schema evolution|
|         |            |          |                | handled here. Data quality check enforced before promotion         |
| Gold    | BigQuery   | Columnar |  Dataform      | Analytics ready, modelled datasets serving Star Schema, Data Vault |
|         |            |          |                | 2.0 and OBT structure. Optimized with partitioning and clustering  |
|         |            |          |                | for query performance                                              |
|-----------------------------------------------------------------------------------------------------------------------|

### GCP services used
|-----------------------------------------------------------------------------------------------------------------------  |
| Service       |  Role in RetailPulse                     | Why this service                                             |
|-----------------------------------------------------------------------------------------------------------------------  |
| GCS           | It stores raw data and CDC data coming    |This is a cost optimised service which provides raw backup   |
|               | from WooCommerce REST API and Webhooks    |with very minimal cost of storage                            |
| Dataproc      | It runs Pyspark jobs for Bronze ingestion,|Managed spark service on GCP, no cluster setup or            |
|               | Silver transformation, SCD processing,    |maintenance needed. Integrates natively with GCS and BigQuery|
|               |                                           |Cost effective as cluster spins up only when a job runs      |
| BigQuery      | It stores transformed and curated datasets|An efficient warehousing service which not only stores       |
|               | for modelling and business reporting      |curated data in form of table/views but also enable user to  |
|               |                                           |querying data by writing SQL                                 |
| Dataform      | used for data modelling/ELT on stored data|It gives a flexibility to user to perform ELT and data       |
|               | in BigQuery                               |modelling on curated datasets and then fulfill the           |
|               |                                           |purpose such as business reporting and ML model training     |
| Cloud Composer| It's used for pipeline orchestration      |supports Airflow DAGs to orchestrate the full pipeline       |
|               |                                           |ensuring execution orders, automatic retries on failure      |
|               |                                           |and alerting when something goes wrong                       | 
|-----------------------------------------------------------------------------------------------------------------------  |

### Tools used alongside GCP

|-----------------------------------------------------------------------------------------------------------------------|
| Tools              | Purpose                                                                                          |
|-----------------------------------------------------------------------------------------------------------------------|
| Great Expectations |  It's Python library which checks the data quality while data movement from bronze to silver     |
| Evidently AI       |  It's Python library for data drift detection which compares data today against data from        |
|                    |  previous period and tell if something unexpected has changed                                    |
| Delta Lake         |  It's an Open table format for schema evolution in GCS                                           |
| pytest             |  It's used to write unit testing for all pipeline code                                           |
| Power BI           |  It consumes curated data for business reporting                                                 |
| GitHub Actions     |  Runs automated pytest suite on every push to develop branch. Blocks merge if any test fails     |
|                    |  ensuring code quality is maintained throughout development                                      |
|-----------------------------------------------------------------------------------------------------------------------|