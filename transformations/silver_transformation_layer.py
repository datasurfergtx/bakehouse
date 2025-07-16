import dlt
from pyspark.sql import functions as F, Window as W, types as T

import dlt
import pyspark.sql.functions as F

@dlt.table
def sil_customers():
    df = dlt.read_stream("brz_customers")

    df = (
        df.dropDuplicates(["customerID"])  # ensure customerID is unique
          .withColumnRenamed("customerID", "id")  # rename customerID to id
          .withColumn(
              "phone_number",
              F.regexp_replace(
                  F.regexp_replace(
                      F.regexp_replace(
                          F.col("phone_number"),
                          r"[\s\-\(\)\.]", ""  # remove spaces, dashes, (), dots
                      ),
                      r"^00", "+"  # replace 00 with +
                  ),
                  r"^011", "+"   # US international dial prefix
              )
          )
          .withColumn(
              "postal_zip_code",
              F.lpad(F.col("postal_zip_code").cast("string"), 5, "0")  # pad to 5 digits
          )
    )

    return df

@dlt.table
def sil_franchises():
    brz_franchises = dlt.read_stream("brz_franchises")
    brz_suppliers = dlt.read("brz_suppliers")\
        .selectExpr("supplierID",
                    "name as supplier_name",
                    "ingredient as supplier_ingredient",
                    "city as supplier_city",
                    "district as supplier_district",
                    "size as supplier_size")
    brz_franchises = (
        brz_franchises.dropDuplicates(["franchiseID"])  # ensure franchiseID is unique
    ).join(brz_suppliers,"supplierID",how="left")
    franchises_suppliers= brz_franchises.selectExpr(
        "franchiseID as franchise_id",
        "supplierID as supplier_id",
        "name as franchise_name",
        "city as franchise_city",
        "district as franchise_district",
        "zipcode as franchise_zipcode",
        "country as franchise_country",
        "size as franchise_size",
        "longitude as franchise_longitude",
        "latitude as franchise_latitude",
        "supplier_name",
        "supplier_ingredient",
        "supplier_city",
        "supplier_district",
        "supplier_size"
    )
    return franchises_suppliers
