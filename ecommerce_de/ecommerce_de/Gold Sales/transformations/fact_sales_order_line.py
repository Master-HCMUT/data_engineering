from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.gold.fact_sales_order_line",
    schema="""
        sales_order_line_key STRING,
        SalesOrderID INT,
        SalesOrderDetailID INT,
        SalesOrderNumber STRING,
        PurchaseOrderNumber STRING,
        order_date_key INT,
        due_date_key INT,
        ship_date_key INT,
        customer_key INT,
        product_key INT,
        territory_key INT,
        salesperson_key INT,
        SpecialOfferID INT,
        OrderStatus INT,
        OnlineOrderFlag BOOLEAN,
        OrderQty INT,
        UnitPrice DECIMAL(19,4),
        UnitPriceDiscount DECIMAL(19,4),
        GrossSalesAmount DECIMAL(19,4),
        DiscountAmount DECIMAL(19,4),
        NetSalesAmount DECIMAL(19,4),
        CONSTRAINT pk_fact_sales_order_line PRIMARY KEY (sales_order_line_key),
        CONSTRAINT fk_fact_sales_order_line_order_date FOREIGN KEY (order_date_key)
            REFERENCES dev.gold.dim_date(date_key),
        CONSTRAINT fk_fact_sales_order_line_due_date FOREIGN KEY (due_date_key)
            REFERENCES dev.gold.dim_date(date_key),
        CONSTRAINT fk_fact_sales_order_line_ship_date FOREIGN KEY (ship_date_key)
            REFERENCES dev.gold.dim_date(date_key),
        CONSTRAINT fk_fact_sales_order_line_customer FOREIGN KEY (customer_key)
            REFERENCES dev.gold.dim_customer(customer_key),
        CONSTRAINT fk_fact_sales_order_line_product FOREIGN KEY (product_key)
            REFERENCES dev.gold.dim_product(product_key),
        CONSTRAINT fk_fact_sales_order_line_territory FOREIGN KEY (territory_key)
            REFERENCES dev.gold.dim_sales_territory(territory_key),
        CONSTRAINT fk_fact_sales_order_line_salesperson FOREIGN KEY (salesperson_key)
            REFERENCES dev.gold.dim_salesperson(salesperson_key)
    """,
)
@dp.expect("valid_pk", "sales_order_line_key IS NOT NULL")
@dp.expect("valid_grain", "SalesOrderID IS NOT NULL AND SalesOrderDetailID IS NOT NULL")
@dp.expect("valid_dimensions", "customer_key IS NOT NULL AND product_key IS NOT NULL")
def gold_fact_sales_order_line():
    detail = spark.read.table("dev.silver.sales_order_detail_clean").alias("d")
    header = spark.read.table("dev.silver.sales_order_header_clean").alias("h")

    gross_sales_amount = (
        F.col("d.OrderQty").cast("decimal(19,4)") * F.col("d.UnitPrice")
    ).cast("decimal(19,4)")
    discount_amount = (
        gross_sales_amount * F.col("d.UnitPriceDiscount")
    ).cast("decimal(19,4)")

    return detail.join(header, F.col("d.SalesOrderID") == F.col("h.SalesOrderID"), "inner").select(
        F.concat_ws(
            "-",
            F.col("d.SalesOrderID").cast("string"),
            F.col("d.SalesOrderDetailID").cast("string"),
        ).alias("sales_order_line_key"),
        F.col("d.SalesOrderID").alias("SalesOrderID"),
        F.col("d.SalesOrderDetailID").alias("SalesOrderDetailID"),
        F.col("h.SalesOrderNumber").alias("SalesOrderNumber"),
        F.col("h.PurchaseOrderNumber").alias("PurchaseOrderNumber"),
        F.date_format(F.to_date("h.OrderDate"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.date_format(F.to_date("h.DueDate"), "yyyyMMdd").cast("int").alias("due_date_key"),
        F.date_format(F.to_date("h.ShipDate"), "yyyyMMdd").cast("int").alias("ship_date_key"),
        F.col("h.CustomerID").alias("customer_key"),
        F.col("d.ProductID").alias("product_key"),
        F.col("h.TerritoryID").alias("territory_key"),
        F.col("h.SalesPersonID").alias("salesperson_key"),
        F.col("d.SpecialOfferID").alias("SpecialOfferID"),
        F.col("h.Status").alias("OrderStatus"),
        F.col("h.OnlineOrderFlag").alias("OnlineOrderFlag"),
        F.col("d.OrderQty").alias("OrderQty"),
        F.col("d.UnitPrice").alias("UnitPrice"),
        F.col("d.UnitPriceDiscount").alias("UnitPriceDiscount"),
        gross_sales_amount.alias("GrossSalesAmount"),
        discount_amount.alias("DiscountAmount"),
        F.col("d.LineTotal").alias("NetSalesAmount"),
    )
