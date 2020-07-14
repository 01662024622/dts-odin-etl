import sys

from pyspark import StorageLevel

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql.functions import when
from datetime import date, datetime, timedelta
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pytz
import hashlib
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sqlContext = SQLContext(sc)
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = True
REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log"

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
CURRENT_DATE = datetime.now(ho_chi_minh_timezone)
print('current_date: ', CURRENT_DATE)
current_week_id = long(CURRENT_DATE.strftime("%Y%W"))

week_fake = 99999999999L
WEEK_PERIOD_ID = 1L
DAILY_PERIOD_ID = 2L
REPORT_ROLE_MANAGER_ID = 1L
DURATION_LS_SC_SUCCESS = 2160
DURATION_LT_SUCCESS = 1500

BEHAVIOR_ID_LS = '11'
BEHAVIOR_ID_SC = '12'
BEHAVIOR_ID_LT = '13'

STUDENT_LEVEL_DATA = [
    ['starter', 1L],
    ['sbasic', 2L],
    ['basic', 3L],
    ['preinter', 4L],
    ['inter', 5L],
    ['UNAVAILABLE', 6L]
]
PACKAGE_DATA = [
    ['TAAM-TT', 1L],
    ['TENUP', 2L],
    ['TAAM-TC', 3L],
    ['VIP3-TT', 4L],
    ['DEMO', 5L],
    ['TRAINING', 6L],
    ['UNAVAILABLE', 7L]
]

PACKAGE_STATUS_DATA = [
    ['ACTIVED', 1L],
    ['SUSPENDED', 2L],
    ['DEACTIVED', 3L],
    ['CANCELLED', 4L],
    ['EXPIRED', 5L],
    ['UNAVAILABLE', 6L]
]


def is_ls_sc_lt_success(behavior_id, duration):
    if behavior_id in [BEHAVIOR_ID_LS, BEHAVIOR_ID_SC]:
        if duration >= DURATION_LS_SC_SUCCESS:
            return 1L
    if behavior_id == BEHAVIOR_ID_LT:
        if duration >= DURATION_LT_SUCCESS:
            return 1L
    return 0L


is_ls_sc_lt_success = f.udf(is_ls_sc_lt_success, LongType())


def find(lists, key):
    for items in lists:
        if key.startswith(items[0]):
            return items[1]
    return 0L


def get_student_level_id(student_level):
    return find(STUDENT_LEVEL_DATA, student_level)


get_student_level_id = f.udf(get_student_level_id, LongType())


def get_package_id(package_code):
    return find(PACKAGE_DATA, package_code)


get_package_id = f.udf(get_package_id, LongType())


def get_package_status_id(package_status_code):
    return find(PACKAGE_STATUS_DATA, package_status_code)


get_package_status_id = f.udf(get_package_status_id, LongType())


def get_week_id(time_v):
    date_v = datetime.fromtimestamp(float(time_v), ho_chi_minh_timezone)
    return long(date_v.strftime("%Y%W"))


get_week_id = f.udf(get_week_id, LongType())


def get_weeks_level(a, b):
    if a is None or b is None:
        return [week_fake]

    weeks = []
    a_t = datetime.fromtimestamp(float(a), ho_chi_minh_timezone)
    # a_t_week_id = long(a_t.strftime("%Y%W"))

    if b == 99999999999L:
        b_t = CURRENT_DATE
    else:
        b_t = datetime.fromtimestamp(b, ho_chi_minh_timezone)
    b_t_week_id = long(b_t.strftime("%Y%W"))

    date_item = a_t
    while long(date_item.strftime("%Y%W")) < b_t_week_id:
        weeks.append(long(date_item.strftime("%Y%W")))
        date_item += timedelta(7)

    if len(weeks) == 0:
        weeks = [week_fake]

    return weeks

TimeStructType = StructType([
    StructField("time_id", LongType(), False),
    StructField("time_name", StringType(), False),
    StructField("time_code", LongType(), False)
])

def get_weeks(a, b):
    # print('get_weeks::a: ', a)
    # print('get_weeks::b: ', b)

    if a is None or b is None:
        return [Row('time_id', 'time_name')(week_fake, str(week_fake))]

    weeks = []
    a_t = datetime.fromtimestamp(float(a), ho_chi_minh_timezone)
    # a_t_week_id = long(a_t.strftime("%Y%W"))

    if b == 99999999999L:
        b_t = CURRENT_DATE
    else:
        b_t = datetime.fromtimestamp(b, ho_chi_minh_timezone)
    b_t_week_id = long(b_t.strftime("%Y%m%d"))

    date_item = a_t
    while long(date_item.strftime("%Y%W")) <= b_t_week_id:
        time_id = long(date_item.strftime("%Y%W"))
        weeK_id_view = long(date_item.strftime("%W")) + 1L
        time_name = 'Year: ' + date_item.strftime("%Y") + ' _ Week: ' + str(weeK_id_view)
        time_item = Row('time_id', 'time_name', 'time_code')(time_id, time_name, time_id + 1L)
        weeks.append(time_item)
        date_item += timedelta(7)

    return weeks


get_weeks = f.udf(get_weeks, ArrayType(TimeStructType))



def get_dates(start_date_timestamp, end_date_timestamp):
    # print('get_weeks::a: ', a)
    # print('get_weeks::b: ', b)

    if start_date_timestamp is None or end_date_timestamp is None:
        return [Row('time_id', 'time_name')(week_fake, str(week_fake))]

    date_ids = []
    start_date_timestamp_t = datetime.fromtimestamp(float(start_date_timestamp), ho_chi_minh_timezone)
    if end_date_timestamp == 99999999999L:
        end_date_timestamp_t = CURRENT_DATE
    else:
        end_date_timestamp_t = datetime.fromtimestamp(end_date_timestamp, ho_chi_minh_timezone)
    end_date_id = long(end_date_timestamp_t.strftime("%Y%m%d"))

    date_item = start_date_timestamp_t
    while long(date_item.strftime("%Y%m%d")) <= end_date_id:
        time_id = long(date_item.strftime("%Y%m%d"))
        time_name = date_item.strftime("%Y-%m-%d")
        time_item = Row('time_id', 'time_name', 'time_code')(time_id, time_name, time_id)
        date_ids.append(time_item)
        date_item += timedelta(1)

    return date_ids


udf_get_dates = f.udf(get_dates, ArrayType(TimeStructType))


def insert_week_id(start_date_id, end_date_id):
    start_date = datetime.strptime(str(start_date_id), "%Y%m%d")
    print('start_date: ', start_date)
    start_date_timestamp = long(start_date.strftime("%s"))
    print('start_date_timestamp: ', start_date_timestamp)

    end_date = datetime.strptime(str(end_date_id), "%Y%m%d")
    print('end_date: ', end_date)
    end_date_timestamp = long(end_date.strftime("%s"))
    print('end_date_timestamp: ', end_date_timestamp)

    time_dr = [{'period_id': WEEK_PERIOD_ID}]

    df_time_dr = sqlContext.createDataFrame(time_dr)
    print('df_time_dr')
    df_time_dr.printSchema()
    df_time_dr.show(3)
    df_time_dr = df_time_dr \
        .withColumn('week_items', get_weeks(f.lit(start_date_timestamp), f.lit(end_date_timestamp)))

    print('df_time_dr')
    df_time_dr.printSchema()
    df_time_dr.show(3)

    df_time_dr_list = df_time_dr.select(
        'period_id',
        f.explode('week_items').alias('week_item')
    )

    print('df_time_dr_list after explode')
    df_time_dr_list.printSchema()
    df_time_dr_list.show(3)

    df_time_dr_list = df_time_dr_list.select(
        'period_id',
        f.col("week_item").getItem("time_id").alias("time_id").cast('long'),
        f.col("week_item").getItem("time_name").alias("time_name"),
        f.col("week_item").getItem("time_code").alias("time_code").cast('long')
    )

    print('df_time_dr_list final')
    df_time_dr_list.printSchema()
    df_time_dr_list.show(3)

    dyf_time_dr_list = DynamicFrame \
        .fromDF(df_time_dr_list, glueContext, 'dyf_time_dr_list')

    apply_ouput = ApplyMapping \
        .apply(frame=dyf_time_dr_list,
               mappings=[("period_id", "long", "period_id", "long"),
                         ("time_id", "long", "time_id", "long"),
                         ("time_name", "string", "time_name", "string"),
                         ("time_code", "long", "time_code", "long")
                         ])
    #
    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "bc200.time_dim",
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/time_dim",
                                                   transformation_ctx="datasink4")



def insert_date_id(start_date_id, end_date_id):
    start_date = datetime.strptime(str(start_date_id), "%Y%m%d")
    print('start_date: ', start_date)
    start_date_timestamp = long(start_date.strftime("%s"))
    print('start_date_timestamp: ', start_date_timestamp)

    end_date = datetime.strptime(str(end_date_id), "%Y%m%d")
    print('end_date: ', end_date)
    end_date_timestamp = long(end_date.strftime("%s"))
    print('end_date_timestamp: ', end_date_timestamp)

    time_dr = [{'period_id': DAILY_PERIOD_ID}]

    df_time_dr = sqlContext.createDataFrame(time_dr)
    print('df_time_dr')
    df_time_dr.printSchema()
    df_time_dr.show(3)
    df_time_dr = df_time_dr \
        .withColumn('date_ids', udf_get_dates(f.lit(start_date_timestamp), f.lit(end_date_timestamp)))

    print('df_time_dr')
    df_time_dr.printSchema()
    df_time_dr.show(3)

    df_time_dr_list = df_time_dr.select(
        'period_id',
        f.explode('date_ids').alias('date_id')
    )

    print('df_time_dr_list after explode')
    df_time_dr_list.printSchema()
    df_time_dr_list.show(3)

    df_time_dr_list = df_time_dr_list.select(
        'period_id',
        f.col("date_id").getItem("time_id").alias("time_id").cast('long'),
        f.col("date_id").getItem("time_name").alias("time_name"),
        f.col("date_id").getItem("time_code").alias("time_code").cast('long')
    )

    print('df_time_dr_list final')
    df_time_dr_list.printSchema()
    df_time_dr_list.show(3)

    dyf_time_dr_list = DynamicFrame \
        .fromDF(df_time_dr_list, glueContext, 'dyf_time_dr_list')

    apply_ouput = ApplyMapping \
        .apply(frame=dyf_time_dr_list,
               mappings=[("period_id", "long", "period_id", "long"),
                         ("time_id", "long", "time_id", "long"),
                         ("time_name", "string", "time_name", "string"),
                         ("time_code", "long", "time_code", "long")
                         ])
    #
    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "bc200.time_dim",
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/time_dim",
                                                   transformation_ctx="datasink4")


def main():

    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    start_date_id = 20190101
    end_date_id = 20200501

    # insert_week_id(start_date_id, end_date_id)

    insert_date_id(start_date_id, end_date_id)



if __name__ == "__main__":
    main()
