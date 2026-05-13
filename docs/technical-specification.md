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

## Section 4: API Contract

### Authentication

**Mechanism:** 
The data pipeline authenticate with WooCommerece in two different ways:
- https query parameters: WooCommerce REST API uses Consumer key and Consumer secret, two long string that act like username and password.
   Both of them are passed in URL and can appear in logs.(less secure and preferred in development) 
- OAuth 1.0a: This mechanism signs every request with a cryptographic signature using consumer key and secret. The credentials themselves never 
   travels in URL, only signature does. WooCommerce get that signature varified on server side.(More secured and preferred for production)

**Credentials storage:** 
Credentials are stored in a .env file and read at runtime using python-dotenv. The .env file is added to .gitignore and never committed to GitHub. 
The .env.example file serves as a template showing required variables without exposing real values.

**Error Handling:** 
A HTTP 401 response indicates invalid or expired credentials. 
The pipeline will raise an authentication exception and halt — no partial data will be written to GCS.

### Pagination strategy

WooCommerce returns 10 items per page by default and the threshold can go upto 100 records per request. In HTTP headers, total number of resource and 
pages are always included in the X-WP-Total and X-WP-TotalPages and accordingly retrival of complete records can be set up in below parameters which
controls pagination.
- GET /orders?per_page = 15 (item per page can be specified with ?per_page parameter)
- GET /orders?page = 2 (further pages can be specified  with ?page parameter)
- GET /orders?offset = 5 (offset from first resource can be specified using offset parameter)

### Rate limiting

WooCommerce itself doesn't enforce rate limits at the application level. However the pipeline uses a combine strategy of Proactive Throttling and exponential 
back off to ensure compatibility with the production hosting environment which typically enforces 60-120 requests per minute.

**Strategy:**
- Primary: throttle to one request per second between paginated API calls
- Fallback: exponential back off on http 429 responses, statring from 1 second wait time to doubling down to 16 seconds wait time
- Maximum: 5 retries before raising an alert to airflow


### Incremental loading strategy

The date_modified field is used as the pipeline watermark. 
On each run the pipeline passes ?after=last_successful_run_timestamp to fetch only records modified since the last successful ingestion. 
The watermark timestamp is stored in GCS as a small JSON file and updated after each successful run. 
On first run a full historical load is performed.

### Endpoints and fields extracted

#### Orders endpoint

| Field | API path | Data type | Nullable | PII | Notes |
|-------|----------|-----------|----------|-----|-------|
| order_id | id | INTEGER | No | No | Primary key |
| order_status | status | STRING | No | No | analytics (order funnel analysis) |
| order_placed_date | date_created | TIMESTAMP | No | No | analytics (when order placed) |
| order_update_date | date_modified | TIMESTAMP | No | No  | pipeline logic (CDC watermark) |
| customer_id | customer_id | INTEGER | No | No | analytics + pipeline (SCD 2 Join Key) |
| total | total | STRING  | No | No | analytics (revenue) |
| subtotal | subtotal | STRING | No | No | analytics |
| currency | currency | STRING | No | No | analytics |
| total_discount_provided | discount_total | STRING | Yes | No  | analytics |
| billing_email | billing.email | STRING | Yes  | Yes | PII (mask in silver layer) |
| billing_phone | billing.phone | STRING | Yes  | Yes | PII (mask in silver layer) |
| billing_first_name | billing.first_name | STRING | No | Yes | PII (mask in silver layer) |
| billing_last_name | billing.last_name | STRING | No | Yes | PII (mask in silver layer) |
| billing_city | billing.city | STRING | Yes | No | analytics (regional reporting) |
| billing_state | billing.state | STRING | No | No | analytics (regional reporting) |
| billing_country | billing.country | STRING | No | No | analytics (regional reporting) |
| line_items_id | line_items[].id | INTEGER | No | No | pipeline logic |
| line_items_product_id | line_items[].product_id | INTEGER | No | No | analytics (product performance) |
| line_items_quantity | line_items[].quantity | INTEGER | Yes | No | analytics (unit sold) | 
| line_items_total |line_items[].total | STRING | No | No | analytics (line revenue) |

#### Customers endpoint

| Field | API path | Data type | Nullable | PII | Notes |
|-------|----------|-----------|----------|-----|-------|
| customer_id | id | INTEGER | No | No | Primary Key |
| account_created_date | date_created | TIMESTAMP | Yes | No | Account creation date |
| account_modification_date | date_modified | TIMESTAMP | Yes | No | Account modification date |
| customer_email | email | STRING | No | Yes | PII (mask in silver layer) |
| customer_firstname | first_name | STRING | No | Yes | PII (mask in silver layer) |
| customer_lastname | last_name | STRING | No | Yes | PII (mask in silver layer) |
| billing_email | billing.email | STRING | Yes  | Yes | PII (mask in silver layer) |
| billing_phone | billing.phone | STRING | Yes  | Yes | PII (mask in silver layer) |
| billing_first_name | billing.first_name | STRING | No | Yes | PII (mask in silver layer) |
| billing_last_name | billing.last_name | STRING | No | Yes | PII (mask in silver layer) |
| billing_city | billing.city | STRING | Yes | No | analytics (regional reporting) |
| billing_state | billing.state | STRING | No | No | analytics (regional reporting) |
| billing_country | billing.country | STRING | No | No | analytics (regional reporting) |
| shipping_first_name | shipping.first_name | STRING | No | Yes | PII (mask in silver layer) |
| shipping_last_name | shipping.last_name | STRING | No | Yes | PII (mask in silver layer) |
| shipping_city | shipping.city | STRING | Yes | No | analytics (regional reporting) |
| shipping_state | shipping.state | STRING | No | No | analytics (regional reporting) |
| shipping_country | shipping.country | STRING | No | No | analytics (regional reporting) |
| customer_type | is_paying_customer | BOOLEAN | No | No | analytics (distinguishes paying customers from registered non-buyers) |


#### Products endpoint

| Field | API path | Data type | Nullable | PII | Notes |
|-------|----------|-----------|----------|-----|-------|
| product_id | id | INTEGER | No | No | product identification (Primary Key) |
| product_name | name | STRING | No | No | analytics (products available in stock) |
| product_added_date | date_created | STRING | No | No | analytics (when product got added) |
| product_modification_date | date_modified | STRING | No | No | analytics (change in product stock) |
| product_type | type | STRING | No | No | simple/variable product distinction |
| product_status | status | STRING | No | No | publish/draft/private for filtering |
| stock_keeping_unit | sku | STRING | No | No | stock keeping unit, used in inventory |
| product_price | price | STRING | No | No | analytics (cast to decimal in silver layer) |
| product_original_price | regular_price | STRING | No | No | analytics(original price) |
| product_discounted_price | sale_price | STRING | Yes | No | analytics (discount tracking) |
| sale_start_date | date_on_sale_from | DATETIME | Yes | No | analytics (promotion tracking) |
| sale_end_date | date_on_sale_to | DATETIME | Yes | No | analytics (promotion tracking) |
| product_on_sale | on_sale | BOOLEAN | No | No | quick filter tag |
| total_sales_value | total_sales | INTEGER | No | No | analytics (product popularity) |
| manage_stock    | manage_stock      | BOOLEAN  | No  | No | pipeline logic |
| stock_quantity  | stock_quantity    | INTEGER  | Yes | No | null when unmanaged |
| stock_status    | stock_status      | STRING   | No  | No | instock/outofstock |
| average_rating  | average_rating    | DECIMAL  | No  | No | product quality signal |
| rating_count    | rating_count      | INTEGER  | No  | No | review volume |
| parent_id       | parent_id         | INTEGER  | No  | No | variant products |
| category_id     | categories[].id   | INTEGER  | No  | No | category reporting |
| category_name   | categories[].name | STRING   | No  | No | category reporting |


### Known quirks

1. All monetary fields return as strings — must cast to DECIMAL
2. line_items is a nested array — must explode in Silver not Bronze
3. customer_id = 0 for guest orders — map to -1 unknown member
4. date_modified updates on system touches — combine with 
   event_type for reliable CDC
5. sale_price returns as empty string "" when product is not on sale — must treat empty string as null before casting to DECIMAL

### Webhook events

| Event | Trigger | Payload fields used |
|-------|---------|---------------------|
| woocommerce_new_order | New order placed | id, status, customer_id, total, line_items |
| woocommerce_order_status_changed | Order status update | id, status, date_modified |
| woocommerce_customer_created | New customer registered | id, email, billing |
| woocommerce_product_updated | Product details changed | id, name, price, stock_status |