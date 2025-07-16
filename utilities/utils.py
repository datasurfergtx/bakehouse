import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W, types as T

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

def create_dlt_table(catalog, schema, table_name):
    """
    Dynamically register a DLT table from the Databricks Bakehouse Cookies Dataset. This function will go into <catalog>.<schema>.<table_name> and name the table as <table_name> in the bakehouse_bronze schema.
    """
    @dlt.table(name=table_name)
    def _(catalog=catalog, schema=schema, table_name=table_name):
        return (
          spark.read.table(f"{catalog}.{schema}.{table_name}")
        )