import sys

from pyspark.sql import *
import pandas as pd

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survery_df

print("testt")
print(sys.argv[1])

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf = conf) \
            .getOrCreate()

    logger = Log4J(spark)
    logger.info("test")
    #conf_out =spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())

    sample_df = load_survery_df(spark, sys.argv[1])
    sample_df.show()

    spark.stop()


