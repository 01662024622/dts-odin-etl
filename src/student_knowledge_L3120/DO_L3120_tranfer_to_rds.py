import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from datetime import date, datetime, timedelta
import pytz
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


####
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')

IS_DEV = True
IS_LOAD_FULL = False


knowledge_bool_flag_file = 's3a://dts-odin/flag/student_knowledge/knowledge_bool_flag_file.parquet'
knowledge_number_flag_file = 's3a://dts-odin/flag/student_knowledge/knowledge_number_flag_file.parquet'
knowledge_phonetic_flag_file = 's3a://dts-odin/flag/student_knowledge/knowledge_phonetic_flag_file.parquet'

def get_start_from_flag(flag_file):
    if IS_LOAD_FULL:
        return 0
    try:
        df_flag = spark.read.parquet(flag_file)
        read_from_index = df_flag.collect()[0]['flag']
        start = read_from_index
        return start
    except:
        print('read flag file error ')
        return 0

def save_end_date_as_flag(end_date, flag_file):
    print('save_end_date_as_flag::')
    print('flag_file: ' + flag_file)
    print('end_date: ' + str(end_date))
    flag_data = [end_date]
    df = spark.createDataFrame(flag_data, "long").toDF('flag')
    df.write.parquet(flag_file, mode="overwrite")

def main():

    today = datetime.now(ho_chi_minh_timezone)
    print('today: ', today)
    yesterday = today - timedelta(1)
    print('yesterday: ', yesterday)
    today_id = int(today.strftime("%Y%m%d"))
    yesterday_id = int(yesterday.strftime("%Y%m%d"))
    print('today_id: ', today_id)
    print('yesterday_id: ', yesterday_id)


    start_date_transform = get_start_from_flag(knowledge_bool_flag_file);
    print('bloom::start_date_transform: ' + str(start_date_transform))

    dyf_student_knowledge_bloom_history = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "student_knowledge_bloom_history_for_transfer_rds",
            "redshiftTmpDir": "s3n://dts-odin/temp1/student_knowledge_bloom_history_for_transfer_rds/"}
    )
    dyf_student_knowledge_bloom_history = Filter.apply(frame=dyf_student_knowledge_bloom_history,
                                                       f=lambda x: x["created_date_id"] > start_date_transform )

    number_new_student_knowledge_bloom_history = dyf_student_knowledge_bloom_history.count()
    print('number_new_student_knowledge_bloom_history: ' + str(number_new_student_knowledge_bloom_history))

    if number_new_student_knowledge_bloom_history > 0:
        resolvechoice1 = ResolveChoice.apply(frame=dyf_student_knowledge_bloom_history, choice="make_cols",
                                             transformation_ctx="resolvechoice1")
        dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                   catalog_connection="nvn_knowledge",
                                                                   connection_options={
                                                                       "dbtable": "student_knowledge_bloom_history",
                                                                       "database": "nvn_knowledge_v2"
                                                                   },
                                                                   redshift_tmp_dir="s3a://dts-odin/temp1/student_knowledge_bloom_history/",
                                                                   transformation_ctx="datasink1")

        df_student_knowledge_bloom_history = dyf_student_knowledge_bloom_history.toDF()
        flag = df_student_knowledge_bloom_history.agg({"created_date_id": "max"}).collect()[0][0]
        save_end_date_as_flag(flag, knowledge_bool_flag_file)


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

    start_date_transform = get_start_from_flag(knowledge_number_flag_file)

    dyf_student_knowledge_number_history = Filter.apply(frame=dyf_student_knowledge_number_history,
                                                       f=lambda x: x["created_date_id"] > start_date_transform)


    number_student_knowledge_number_history = dyf_student_knowledge_number_history.count()
    print('number_student_knowledge_number_history: ' + str(number_student_knowledge_number_history))

    if number_student_knowledge_number_history > 0:
        resolvechoice1 = ResolveChoice.apply(frame=dyf_student_knowledge_number_history, choice="make_cols",
                                             transformation_ctx="resolvechoice1")
        dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                   catalog_connection="nvn_knowledge",
                                                                   connection_options={
                                                                       "dbtable": "student_knowledge_number_history",
                                                                       "database": "nvn_knowledge_v2"
                                                                   },
                                                                   redshift_tmp_dir="s3a://dts-odin/temp1/student_knowledge_number_history/",
                                                                   transformation_ctx="datasink1")

    df_student_knowledge_number_history = dyf_student_knowledge_number_history.toDF()
    flag = df_student_knowledge_number_history.agg({"created_date_id": "max"}).collect()[0][0]
    save_end_date_as_flag(flag, knowledge_number_flag_file)


    ################################
    dyf_student_phonetic_number_history = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "student_phonetic_number_history_for_transfer_rds",
            "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_lo_student_01/"}
    )


    start_date_transform = get_start_from_flag(knowledge_phonetic_flag_file)


    dyf_student_phonetic_number_history = Filter.apply(frame=dyf_student_phonetic_number_history,
                                                        f=lambda x: x["created_date_id"] > start_date_transform)

    number_dyf_student_phonetic_number_history = dyf_student_phonetic_number_history.count()

    print('number_dyf_student_phonetic_number_history: ' + str(number_dyf_student_phonetic_number_history))

    if number_dyf_student_phonetic_number_history > 0:

        resolvechoice1 = ResolveChoice.apply(frame=dyf_student_phonetic_number_history, choice="make_cols",
                                             transformation_ctx="resolvechoice1")
        dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                   catalog_connection="nvn_knowledge",
                                                                   connection_options={
                                                                       "dbtable": "student_phonetic_number_history",
                                                                       "database": "nvn_knowledge_v2"
                                                                   },
                                                                   redshift_tmp_dir="s3a://dts-odin/temp1/student_phonetic_number_history/",
                                                                   transformation_ctx="datasink1")
    df_student_phonetic_number_history = dyf_student_phonetic_number_history.toDF()
    flag = df_student_phonetic_number_history.agg({"created_date_id": "max"}).collect()[0][0]
    save_end_date_as_flag(flag, knowledge_phonetic_flag_file)

    # log_student_package.printSchema()
    # log_student_package.show()

if __name__ == "__main__":
    main()
