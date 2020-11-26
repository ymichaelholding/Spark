
#system packages
import cx_Oracle
import json as json
import sys
from pyspark.sql import *
from json import JSONDecodeError
import random

#user defined packages
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survery_df
from lib.db_connections import DBconnections

conf = get_spark_app_config()

spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

logger = Log4J(spark)

sequence= random.randint(1000001,2000001)

logger.info("sequence number" + ':' + str(sequence))

db = DBconnections('ORACLE')

def get_job_config_dtl(job_name):
    conn = db.oracle_set_connections()
    cur = conn.cursor()
    input = [job_name]
    result_set = cur.execute(
    "SELECT JOB_CONFIG FROM JOB_CONFIGURATIONS WHERE JOB_NAME = :1",
    input)
    for i, in result_set.fetchall():
        return str(i)


try:
   job_def = get_job_config_dtl('EMPLOYEE_AUDIT')
   json_job_def = json.loads(job_def)
   print(json_job_def)
except JSONDecodeError as e:
    raise(e)
except ValueError as e:
    raise(e)

logger.info("job definition" + ':' + str(json_job_def))

#extract source configurations
source_config_sql = "SELECT source_name,source_type,user_id,password,jdbc_url,driver,inpound_directory," \
                    "archive_directory,in_progress_directory " \
                    "FROM source_config where source_name ="

logger.info("source config sql" + ':' + source_config_sql)

job_config = {}
source_details_tmp={}
for keys, values in json_job_def['Sources'].items():
    config_result = db.query_result_set(source_config_sql + "'%s'" % values)
    column_name = []
    for cols in config_result.description:
        column_name.append(cols[0])
    rows = list(config_result.fetchall())
    source_details = [dict(zip(column_name, row)) for row in rows]
    # job_config1 = dict(zip(source_details))
    #print(type(source_details))
    for source_values in source_details:
        job_config[keys] = source_values
    #print(job_config)

query_dtls = json_job_def['Query']

logger.info("Soures queries" + ':' + str(query_dtls))

for keys,values in query_dtls.items():
     job_config[keys]['Query']=values

#print(job_config['Source_connector_t1'])



for keys,values in job_config.items():

     table_name=job_config[keys]['Query'].split(' ')[-1]

     if job_config[keys]['SOURCE_TYPE']=="JDBC":

         tempdf = spark.read \
             .format("jdbc") \
             .option("url", job_config[keys]['JDBC_URL']) \
             .option("query",job_config[keys]['Query']) \
             .option("user", job_config[keys]['USER_ID']) \
             .option("password", job_config[keys]['PASSWORD']) \
             .option("driver", job_config[keys]['DRIVER']) \
             .load()

         tempdf.createOrReplaceTempView(table_name + '_' + str(sequence))

         logger.info("table names" + ':' + table_name + '_' + str(sequence))

     elif job_config[keys]['SOURCE_TYPE']=="FILE":

         data_file=job_config[keys]['INPOUND_DIRECTORY'] + '/' + str.lower(table_name + '.' + 'csv')

         filedf =spark.read \
             .option("header", "true") \
             .option("inferSchema", "true") \
             .csv(data_file)


         filedf.createOrReplaceTempView(table_name + '_' + str(sequence))

         logger.info("table names" + ':' + table_name + '_' + str(sequence))

         print(filedf.show())
         file_sql ='SELECT * FROM ' + str.lower(table_name) + '_' + str(sequence)
         spark.sql(file_sql).show()

     else:
          print("please contact admin to add sources or correct source in source")
i=0
#extract join conditions
tbl_alias_seq=0
join_dtls_dict = json_job_def['JoinCondition']
for keys,values in join_dtls_dict.items():
      for key,value in values.items():
          i=i+1
          if key == 'PrimaryEntity':
                tbl_alias_seq=tbl_alias_seq+1
                from_clause='FROM ' + value + '_' + str(sequence) +' t'+ str(tbl_alias_seq)
                i=0
          elif key[4:10] == "Method" :
                from_clause = from_clause + ' ' + value
          elif key[4:10] == "Entity":
                tbl_alias_seq = tbl_alias_seq + 1
                from_clause = from_clause + '  ' + value + '_' + str(sequence) +' t'+ str(tbl_alias_seq)
          elif key[0:9] == "Condition":
                from_clause = from_clause + ' ' + value

print(from_clause)
select_clause=""
target_table=""
insert_clause=""
for select_list in json_job_def['mapping']:
      for select_key,select_value in select_list.items():
           if select_key=='targettable':
               target_table=select_value
           elif select_key=='sourcecolumn':
                select_clause=select_clause  + select_value +','
           elif select_key=='targetcolumn':
                insert_clause=insert_clause  + select_value + ','


def capture_request(job_request_id, job_name,no_of_records, status,error_message):
    conn = db.oracle_set_connections()
    cur = conn.cursor()
    rows = [job_request_id, job_name,no_of_records, status,error_message]
    cur.execute(
        "insert into job_request(job_request_id,job_name, no_of_records,status,error_message) "
        "values (:1, :2, :3, :4,:5)",
        rows)
    conn.commit()

print(select_clause[:-1])
print(target_table)
print(insert_clause[:-1])
final_sql ='SELECT ' + select_clause[:-1] + ' ' + from_clause

logger.info("final_sql" + ':' + final_sql)
print(final_sql)
targetdf=spark.sql(final_sql)

targetdf.show()

targetdf.write \
   .format("jdbc") \
   .option("url", "jdbc:oracle:thin:@DESKTOP-VVUKHPU:49674:xe") \
   .option("driver", "oracle.jdbc.driver.OracleDriver") \
   .option("dbtable", target_table) \
   .option("user", "hr") \
   .option("password", "Spark123$") \
   .mode('append').save()

