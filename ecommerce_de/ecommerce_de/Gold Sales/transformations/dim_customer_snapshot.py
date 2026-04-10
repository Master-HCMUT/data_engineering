from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(name="dev.gold.dim_customer_snapshot")
def gold_dim_customer_snapshot():
    customer = spark.read.table("dev.silver.sales_customer_clean").alias("c")
    person = spark.read.table("dev.silver.sales_person_clean").alias("p")
    store = spark.read.table("dev.silver.sales_store_clean").alias("s")

    person_name = F.trim(
        F.concat_ws(
            " ",
            F.col("p.Title"),
            F.col("p.FirstName"),
            F.col("p.MiddleName"),
            F.col("p.LastName"),
            F.col("p.Suffix"),
        )
    )

    return (
        customer.join(
            person,
            F.col("c.PersonID") == F.col("p.BusinessEntityID"),
            "left",
        )
        .join(store, F.col("c.StoreID") == F.col("s.StoreID"), "left")
        .select(
            F.col("c.CustomerID").alias("customer_key"),
            F.col("c.CustomerID"),
            F.when(F.col("c.StoreID").isNotNull(), F.lit("Store"))
            .otherwise(F.lit("Individual"))
            .alias("CustomerType"),
            F.col("c.AccountNumber").alias("AccountNumber"),
            F.col("c.PersonID"),
            F.col("c.StoreID"),
            F.col("c.TerritoryID"),
            F.when(F.col("c.StoreID").isNotNull(), F.col("s.StoreName"))
            .otherwise(person_name)
            .alias("CustomerName"),
            person_name.alias("PersonName"),
            F.col("s.StoreName").alias("StoreName"),
        )
    )
