from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, col, expr


def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "8g")

    spark_session = SparkSession\
        .builder\
        .master("local[4]")\
        .config(conf=conf)\
        .appName("Aggregate Transform Tutorial") \
        .config("spark.jars", "sparkudftutorial-1.0-SNAPSHOT-jar-with-dependencies.jar") \
        .getOrCreate()

    return spark_session


if __name__ == '__main__':
    spark = create_spark_session()
    # This doesn't work if UDAF is a subclass of Aggregator
    # spark.udf.registerJavaFunction(name="LatestValuesOfKeysMapUdaf", javaClassName="com.tutorial.LatestValuesOfKeysMapUdaf")

    java_udf = spark.sparkContext._jvm.com.tutorial.LatestValuesOfKeysMapUdaf.register(spark._jsparkSession)


    data = [
        ("2022-03-11 09:00:00", "location_1", {"temperature": 23.1, "air_quality": 12.0}),
        ("2022-03-11 09:01:00", "location_1", {"temperature": 23.5, "air_quality": 10.0, "humidity": 68.0}),
        ("2022-03-11 09:02:00", "location_1", {"temperature": 23.8, "air_quality": 13.0}),
        ("2022-03-11 09:03:00", "location_1", {"temperature": 23.2, "air_quality": 11.0, "humidity": 65.0, "light_intensity": 1200.0}),
        ("2022-03-11 09:04:00", "location_1", {"temperature": 23.4, "air_quality": 9.0}),
        ("2022-03-11 09:05:00", "location_1", {"temperature": 23.9, "air_quality": 14.0, "humidity": 72.0}),
        ("2022-03-11 09:00:00", "location_2", {"temperature": 19.8, "air_quality": 15.0}),
        ("2022-03-11 09:01:00", "location_2", {"temperature": 20.1, "air_quality": 17.0, "humidity": 63.0}),
        ("2022-03-11 09:02:00", "location_2", {"temperature": 20.3, "air_quality": 18.0}),
        ("2022-03-11 09:03:00", "location_2", {"temperature": 19.7, "air_quality": 16.0, "humidity": 75.0, "light_intensity": 900.0}),
        ("2022-03-11 09:04:00", "location_2", {"temperature": 20.0, "air_quality": 14.0}),
        ("2022-03-11 09:05:00", "location_2", {"temperature": 20.4, "air_quality": 19.0, "humidity": 60.0})
    ]

    test_df = spark.createDataFrame(data, ["timestamp", "location", "readings"]) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .repartition(2, col("location"))

    test_df.show(truncate=False)
    test_df.printSchema()

    res_df = test_df\
        .groupBy("location")\
        .agg(expr("LatestValuesOfKeysMapUdaf(timestamp, readings)"))

    res_df.show(truncate=False)
