import dlt

@dlt.table()
def fact_transactions():
    sil_customers = dlt.read("sil_customers")
    sil_franchises = dlt.read("sil_franchises")
    brz_transactions = dlt.read_stream("brz_transactions")\
        .selectExpr("transactionID as id",
                    "customerID as customer_id",
                    "franchiseID as franchise_id",
                    "dateTime as order_datetime",
                    "to_date(dateTime) as order_date",
                    "product",
                    "quantity",
                    "unitPrice as unit_price",
                    "totalPrice as total_price",
                    "paymentMethod as payment_method"
                    )

    #alias tables for easier calls
    tx = brz_transactions.alias("tx")
    cust = sil_customers.alias("cust")
    fran = sil_franchises.alias("fran")
    
    #join tables to create fact table
    fact_transactions = tx\
        .join(cust, on="customer_id",how="left")\
        .join(fran, on="franchise_id",how="left")\
        .selectExpr(
            "tx.id",
            "tx.customer_id",
            "tx.franchise_id",
            "fran.supplier_id",
            "tx.order_datetime",
            "tx.order_date",
            "tx.product",
            "tx.quantity",
            "tx.unit_price",
            "tx.total_price",
            "tx.payment_method",
            "cust.first_name",
            "cust.last_name",
            "cust.email_address",
            "cust.phone_number",
            "cust.address",
            "cust.city",
            "cust.state",
            "cust.country",
            "cust.continent",
            "cust.postal_zip_code",
            "cust.gender",
            "fran.franchise_name",
            "fran.franchise_city",
            "fran.franchise_district",
            "fran.franchise_zipcode",
            "fran.franchise_country",
            "fran.franchise_size",
            "fran.franchise_longitude",
            "fran.franchise_latitude",
            "fran.supplier_name",
            "fran.supplier_ingredient",
            "fran.supplier_city",
            "fran.supplier_district",
            "fran.supplier_size"
            )
    return fact_transactions
