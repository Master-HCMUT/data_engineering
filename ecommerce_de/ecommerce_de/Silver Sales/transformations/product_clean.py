from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.parsing import parse_decimal, parse_ts


def _parse_flag(column_name: str):
    return (
        F.when(F.col(column_name).isin("1", "true", "TRUE"), F.lit(True))
        .when(F.col(column_name).isin("0", "false", "FALSE"), F.lit(False))
        .otherwise(F.lit(None))
    )


@dp.table(name="dev.silver.product_clean")
@dp.expect_or_drop("valid_pk", "ProductID IS NOT NULL")
@dp.expect_or_drop("valid_name", "ProductName IS NOT NULL")
def silver_product():
    product = spark.read.table("dev.bronze.product").alias("p")
    subcategory = spark.read.table("dev.bronze.product_subcategory").alias("ps")
    category = spark.read.table("dev.bronze.product_category").alias("pc")

    return (
        product.join(
            subcategory,
            F.col("p.ProductSubcategoryID").cast("int")
            == F.col("ps.ProductSubcategoryID").cast("int"),
            "left",
        )
        .join(
            category,
            F.col("ps.ProductCategoryID").cast("int")
            == F.col("pc.ProductCategoryID").cast("int"),
            "left",
        )
        .select(
            F.col("p.ProductID").cast("int").alias("ProductID"),
            F.col("p.Name").cast("string").alias("ProductName"),
            F.col("p.ProductNumber").cast("string").alias("ProductNumber"),
            _parse_flag("p.MakeFlag").alias("MakeFlag"),
            _parse_flag("p.FinishedGoodsFlag").alias("FinishedGoodsFlag"),
            F.col("p.Color").cast("string").alias("Color"),
            F.col("p.SafetyStockLevel").cast("int").alias("SafetyStockLevel"),
            F.col("p.ReorderPoint").cast("int").alias("ReorderPoint"),
            parse_decimal("p.StandardCost").alias("StandardCost"),
            parse_decimal("p.ListPrice").alias("ListPrice"),
            F.col("p.Size").cast("string").alias("Size"),
            F.col("p.SizeUnitMeasureCode").cast("string").alias("SizeUnitMeasureCode"),
            F.col("p.WeightUnitMeasureCode").cast("string").alias("WeightUnitMeasureCode"),
            parse_decimal("p.Weight").alias("Weight"),
            F.col("p.DaysToManufacture").cast("int").alias("DaysToManufacture"),
            F.col("p.ProductLine").cast("string").alias("ProductLine"),
            F.col("p.Class").cast("string").alias("ProductClass"),
            F.col("p.Style").cast("string").alias("Style"),
            F.col("p.ProductSubcategoryID").cast("int").alias("ProductSubcategoryID"),
            F.col("ps.Name").cast("string").alias("ProductSubcategoryName"),
            F.col("ps.ProductCategoryID").cast("int").alias("ProductCategoryID"),
            F.col("pc.Name").cast("string").alias("ProductCategoryName"),
            parse_ts("p.SellStartDate").alias("SellStartDate"),
            parse_ts("p.SellEndDate").alias("SellEndDate"),
            parse_ts("p.DiscontinuedDate").alias("DiscontinuedDate"),
            parse_ts("p.ModifiedDate").alias("ModifiedDate"),
        )
    )
