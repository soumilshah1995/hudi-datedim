# hudi-datedim
hudi-datedim


```
spark-sql \
    --packages 'org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.2' \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' \
    --conf "spark.hadoop.fs.s3a.access.key=admin" \
    --conf "spark.hadoop.fs.s3a.secret.key=password" \
    --conf "spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "fs.s3a.signing-algorithm=S3SignerType" \
    --conf "spark.sql.catalogImplementation=hive" \
    --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083"





CREATE TABLE date_dim (
    date_key BIGINT,
    year INT,
    month INT,
    day INT,
    quarter INT,
    weekday STRING,
    weekday_number INT
) USING hudi
OPTIONS (
    primaryKey = 'date_key',
    preCombineField = 'date_key',
    path 's3a://warehouse/default/table_name=date_dim'
);


WITH date_range AS (
    SELECT
        explode(sequence(
            to_date('2023-01-01'),
            to_date('2024-12-31'),
            interval 1 day
        )) AS date
)
INSERT INTO date_dim
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS BIGINT) AS date_key,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day,
    QUARTER(date) AS quarter,
    DATE_FORMAT(date, 'EEEE') AS weekday,
    CASE DATE_FORMAT(date, 'EEEE')
        WHEN 'Monday' THEN 0
        WHEN 'Tuesday' THEN 1
        WHEN 'Wednesday' THEN 2
        WHEN 'Thursday' THEN 3
        WHEN 'Friday' THEN 4
        WHEN 'Saturday' THEN 5
        WHEN 'Sunday' THEN 6
    END AS weekday_number
FROM date_range;


```
