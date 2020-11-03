import sys

from pyspark.sql import *
import pandas as pd

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survery_df

print("testt")

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf = conf) \
            .getOrCreate()

    logger = Log4J(spark)
    logger.info("test")
    #conf_out =spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())

    sample_df =  spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(r"C:\Users\Michael\PycharmProjects\Spark\data\employees.csv")
    sample_df.show()

    sample_df.createOrReplaceTempView("sample_tbl")
    count = spark.sql("select department_id,max(salary) from sample_tbl group by department_id")
    count.show()

    spark.stop()