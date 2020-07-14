import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import concat_ws

from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
from pyspark.sql.functions import udf
from datetime import date, datetime, timedelta
import pytz
import json
import random

is_dev = True

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session


    ####
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    print('today: ', today)
    yesterday = today - timedelta(1)
    print('yesterday: ', yesterday)
    today_id = long(today.strftime("%Y%m%d"))
    yesterday_id = long(yesterday.strftime("%Y%m%d"))
    print('today_id: ', today_id)
    print('yesterday_id: ', yesterday_id)

    week_day = yesterday.weekday()
    print('yesterday_week_day: ' +  str(week_day))
    weekend_date = yesterday + timedelta(6 - week_day)
    weekend_date_id = long(weekend_date.strftime("%Y%m%d"))
    print('weekend_date: ' +  str(weekend_date_id))


    dyf_student_knowledge_bloom_history = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "student_knowledge_bloom_history_currently",
            "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_lo_student_01/"}
    )
    # dyf_student_knowledge_bloom_history = Filter.apply(frame=dyf_student_knowledge_bloom_history, f=lambda x: x["created_date_id"] == yesterday_id)

    if is_dev:
        print("dyf_student_knowledge_bloom_history")
        dyf_student_knowledge_bloom_history.printSchema()
        dyf_student_knowledge_bloom_history.show(3)

    resolvechoice1 = ResolveChoice.apply(frame=dyf_student_knowledge_bloom_history, choice="make_cols",
                                         transformation_ctx="resolvechoice1")
    dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                               catalog_connection="nvn_knowledge",
                                                               connection_options={
                                                                   "dbtable": "student_knowledge_bloom_history_v2",
                                                                   "database": "nvn_knowledge_v2"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/temp1/student_knowledge_bloom_history/",
                                                               transformation_ctx="datasink1")

    ################################
    dyf_student_knowledge_number_history = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "student_knowledge_number_history",
            "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_lo_student_01/"}
    )
    dyf_student_knowledge_number_history = Filter.apply(frame=dyf_student_knowledge_number_history,
                                                       f=lambda x: x["created_date_id"] == yesterday_id)

    resolvechoice1 = ResolveChoice.apply(frame=dyf_student_knowledge_number_history, choice="make_cols",
                                         transformation_ctx="resolvechoice1")
    dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                               catalog_connection="nvn_knowledge",
                                                               connection_options={
                                                                   "dbtable": "student_knowledge_number_history_v2",
                                                                   "database": "nvn_knowledge_v2"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/temp1/student_knowledge_bloom_history/",
                                                               transformation_ctx="datasink1")


    ################################
    dyf_student_phonetic_number_history = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "student_phonetic_number_history",
            "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_lo_student_01/"}
    )
    dyf_student_phonetic_number_history = Filter.apply(frame=dyf_student_phonetic_number_history,
                                                        f=lambda x: x["created_date_id"] <= yesterday_id)

    resolvechoice1 = ResolveChoice.apply(frame=dyf_student_phonetic_number_history, choice="make_cols",
                                         transformation_ctx="resolvechoice1")
    dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
    datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                               catalog_connection="nvn_knowledge",
                                                               connection_options={
                                                                   "dbtable": "student_phonetic_number_history_v2",
                                                                   "database": "nvn_knowledge_v2"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/temp1/student_knowledge_bloom_history/",
                                                               transformation_ctx="datasink1")

    # log_student_package.printSchema()
    # log_student_package.show()

if __name__ == "__main__":
    main()
