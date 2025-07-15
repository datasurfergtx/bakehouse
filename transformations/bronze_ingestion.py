#This is the bronze ingestion DLT script. It reads in the bronze tables from the databricks_cookies_dataset_dais_2024 catalog and performs expectations on the data without making any other changes on the dataset.

import dlt

catalog = "databricks_cookies_dataset_dais_2024"
sales_schema = "sales"
media_schema = "media"
# media_tables = ["customer_reviews","gold_reviews_chunked"]
# sales_tables = ["customers", "franchises", "suppliers", "transactions"]

#Sales Schema
#Customers table
@dlt.table
@dlt.expect("valid PK customerID", "customerID is not null")
def customers():
  return spark.read.table(f"{catalog}.{sales_schema}.customers")

#Franchises table
@dlt.table
@dlt.expect("valid PK franchiseID", "franchiseID is not null")
def franchises():
  return spark.read.table(f"{catalog}.{sales_schema}.franchises")

#Suppliers table
@dlt.table
@dlt.expect("valid PK supplierID", "supplierID is not null")
def suppliers():
  return spark.read.table(f"{catalog}.{sales_schema}.suppliers")

#Transactions table
@dlt.table
@dlt.expect("valid PK transactionID", "transactionID is not null")
@dlt.expect("valid CustomerID FK", "customerID is not null")
@dlt.expect("valid FranchiseID FK", "franchiseID is not null")
def transactions():
  return spark.read.table(f"{catalog}.{sales_schema}.transactions")

#Media Schema
#Customer Reviews table
@dlt.table
@dlt.expect("valid PK new_id", "new_id is not null")
@dlt.expect("valid FK franchiseID", "franchiseID is not null")
def customer_reviews():
  return spark.read.table(f"{catalog}.{media_schema}.customer_reviews")

#Gold Reviews Chunked table
@dlt.table
@dlt.expect("valid PK chunk_id", "chunk_id is not null")
@dlt.expect("valid FK franchiseID", "franchiseID is not null")
def gold_reviews_chunked():
  return spark.read.table(f"{catalog}.{media_schema}.gold_reviews_chunked")