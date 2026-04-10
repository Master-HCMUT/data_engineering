from pyspark import pipelines as dp


dp.create_streaming_table(
    name="dev.gold.dim_customer_history",
    comment="SCD Type 2 history for customer attributes generated from silver snapshots.",
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dev.gold.dim_customer_history",
    source="dev.gold.dim_customer_snapshot",
    keys=["customer_key"],
    stored_as_scd_type=2,
)
