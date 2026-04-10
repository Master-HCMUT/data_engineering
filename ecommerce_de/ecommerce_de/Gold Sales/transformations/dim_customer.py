from pyspark import pipelines as dp
@dp.table(
    name="dev.gold.dim_customer",
    schema="""
        customer_key INT,
        CustomerID INT,
        CustomerType STRING,
        AccountNumber STRING,
        PersonID INT,
        StoreID INT,
        TerritoryID INT,
        CustomerName STRING,
        PersonName STRING,
        StoreName STRING,
        CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
    """,
)
@dp.expect("valid_pk", "customer_key IS NOT NULL")
def gold_dim_customer():
    return spark.read.table("dev.gold.dim_customer_history").filter("__END_AT IS NULL").select(
        "customer_key",
        "CustomerID",
        "CustomerType",
        "AccountNumber",
        "PersonID",
        "StoreID",
        "TerritoryID",
        "CustomerName",
        "PersonName",
        "StoreName",
    )
