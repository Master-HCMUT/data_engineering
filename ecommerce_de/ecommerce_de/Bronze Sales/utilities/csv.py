def read_csv_no_header(spark, file_path, headers, delimeter = "\t"):
    df = spark.read.format("csv") \
        .option("header", "false") \
        .option("delimiter", delimeter) \
        .load(file_path)

    df = df.toDF(*headers)
    return df