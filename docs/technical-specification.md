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