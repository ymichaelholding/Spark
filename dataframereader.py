import sys

from keyring.backends import Windows
from pyspark.sql import *
from pyspark.sql import functions as f
import pandas as pd

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survery_df

print("testt")

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)
    logger.info("test")
    # conf_out =spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    employees_schema = 'EMPLOYEE_ID INT, FIRST_NAME STRING,  LAST_NAME  STRING, \
                      EMAIL STRING,  PHONE_NUMBER STRING,  HIRE_DATE DATE,  JOB_ID INT, \
                      SALARY INT, COMMISSION_PCT INT, MANAGER_ID INT, DEPARTMENT_ID INT'

    sample_df = spark.read \
        .option("header", "true") \
        .schema(employees_schema) \
        .csv(r"C:\Users\Michael\PycharmProjects\Spark\data\employees.csv")


    #sample_df.write \
    #    .format("csv") \
    #    .mode("overwrite") \
    #    .option("path",r"C:\Users\Michael\PycharmProjects\Spark\data\employees.csv" ) \
    #    .partitionBy("DEPARTMENT_ID") \
    #    .option("maxRecordsPerFile",10000)

    #sample_df.write \
    #    .mode("overwrite") \
    #    .saveAsTable("employees")

    sample_df.show()
    sample_df.printSchema()

    logger.info("Num partitions " + str(sample_df.rdd.getNumPartitions()))

    sample_df.selectExpr('count(1) as cnt').show()

    sample_df.groupBy("DEPARTMENT_ID").agg(f.max("SALARY")).show()

    running_total_window = Window.partitionBy("DEPARTMENT_ID") \
                           .orderBy("EMPLOYEE_ID") \
                            .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    sample_df.withColumn("running_total",f.sum("SALARY").over(running_total_window)).show()

    lag_test = Window.partitionBy("DEPARTMENT_ID") \
        .orderBy("EMPLOYEE_ID")

    sample_df.withColumn("lag_salary", f.lag("SALARY").over(lag_test)).show()