from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.gold.dim_product",
    schema="""
        product_key INT,
        ProductID INT,
        ProductName STRING,
        ProductNumber STRING,
        ProductCategoryID INT,
        ProductCategoryName STRING,
        ProductSubcategoryID INT,
        ProductSubcategoryName STRING,
        MakeFlag BOOLEAN,
        FinishedGoodsFlag BOOLEAN,
        Color STRING,
        Size STRING,
        Weight DECIMAL(19,4),
        StandardCost DECIMAL(19,4),
        ListPrice DECIMAL(19,4),
        DaysToManufacture INT,
        ProductLine STRING,
        ProductLineName STRING,
        ProductClass STRING,
        ProductClassName STRING,
        Style STRING,
        StyleName STRING,
        SellStartDate TIMESTAMP,
        SellEndDate TIMESTAMP,
        DiscontinuedDate TIMESTAMP,
        CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
    """,
)
@dp.expect("valid_pk", "product_key IS NOT NULL")
def gold_dim_product():
    return (
        spark.read.table("dev.gold.dim_product_history")
        .filter(F.col("__END_AT").isNull())
        .select(
        F.col("ProductID").alias("product_key"),
        F.col("ProductID"),
        F.col("ProductName"),
        F.col("ProductNumber"),
        F.col("ProductCategoryID"),
        F.col("ProductCategoryName"),
        F.col("ProductSubcategoryID"),
        F.col("ProductSubcategoryName"),
        F.col("MakeFlag"),
        F.col("FinishedGoodsFlag"),
        F.col("Color"),
        F.col("Size"),
        F.col("Weight"),
        F.col("StandardCost"),
        F.col("ListPrice"),
        F.col("DaysToManufacture"),
        F.col("ProductLine"),
        F.when(F.col("ProductLine") == "R", F.lit("Road"))
        .when(F.col("ProductLine") == "M", F.lit("Mountain"))
        .when(F.col("ProductLine") == "T", F.lit("Touring"))
        .when(F.col("ProductLine") == "S", F.lit("Standard"))
        .otherwise(F.lit(None))
        .alias("ProductLineName"),
        F.col("ProductClass"),
        F.when(F.col("ProductClass") == "H", F.lit("High"))
        .when(F.col("ProductClass") == "M", F.lit("Medium"))
        .when(F.col("ProductClass") == "L", F.lit("Low"))
        .otherwise(F.lit(None))
        .alias("ProductClassName"),
        F.col("Style"),
        F.when(F.col("Style") == "W", F.lit("Women"))
        .when(F.col("Style") == "M", F.lit("Men"))
        .when(F.col("Style") == "U", F.lit("Universal"))
        .otherwise(F.lit(None))
        .alias("StyleName"),
        F.col("SellStartDate"),
        F.col("SellEndDate"),
        F.col("DiscontinuedDate"),
        )
    )
