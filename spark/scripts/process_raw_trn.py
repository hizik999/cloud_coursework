from pyspark.sql import SparkSession

def get_latest_folder(spark, bucket, prefix):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(f"s3a://{bucket}/{prefix}")
    fs  = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{prefix}")
    statuses = fs.listStatus(path)
    dirs = [(st.getPath().getName(), st.getModificationTime())
            for st in statuses if st.isDirectory()]
    if not dirs:
        return None
    return max(dirs, key=lambda x: x[1])[0]

def convert_to_parquet(spark, src_bucket, src_prefix, folder, src_ext, tgt_bucket, tgt_prefix):
    source_path = f"s3a://{src_bucket}/{src_prefix}{folder}/{src_ext}"
    df = spark.read \
        .option("header", True) \
        .option("sep", "|") \
        .csv(source_path)

    # Оставляем только те колонки, которые НЕ начинаются с '#'
    good_cols = [c for c in df.columns if not c.startswith("#") or c == 'LS Bright']
    df2 = df.select(*good_cols)

    target_path = f"s3a://{tgt_bucket}/{tgt_prefix}"
    df2.write.mode("overwrite").parquet(target_path)
    print(f"Written to {target_path}")

def main():
    spark = SparkSession.builder.appName("CSV2Parquet").getOrCreate()

    SRC_BUCKET  = "raw"
    SRC_PREFIX  = "mrp_colors/"
    SRC_EXT     = "*.csv"
    TGT_BUCKET  = "trn"
    TGT_PREFIX  = "mrp_colors/"

    latest = get_latest_folder(spark, SRC_BUCKET, SRC_PREFIX)
    if not latest:
        print("Папки не найдены, выходим.")
    else:
        print(f"Самая новая папка: {latest}")
        convert_to_parquet(
            spark,
            SRC_BUCKET, SRC_PREFIX,
            latest, SRC_EXT,
            TGT_BUCKET, TGT_PREFIX
        )

    spark.stop()

if __name__ == "__main__":
    main()
