from pyspark import pipelines as dp


dp.create_streaming_table(
    name="dev.gold.dim_product_history",
    comment="SCD Type 2 history for product attributes generated from silver snapshots.",
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dev.gold.dim_product_history",
    source="dev.silver.product_clean",
    keys=["ProductID"],
    stored_as_scd_type=2,
    track_history_except_column_list=["ModifiedDate"],
)
