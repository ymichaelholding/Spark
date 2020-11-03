from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("Hello RDD")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext
    logger = Log4J(spark)

linesRDD = sc.textFile(r"C:\Users\Michael\PycharmProjects\Spark\data\employees.csv")
logger.info(linesRDD.collect())