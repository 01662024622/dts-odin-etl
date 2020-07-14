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
import pytz
import hashlib
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField
from pyspark.sql import Row

is_dev = True
REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/"
DATABASE_ADVX = 'student_native_report'
TABLE_ADVX_CHANGE_STATUS_HISTORY = 'advx.advx_change_status_history'

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_week = datetime.now(ho_chi_minh_timezone)
print('current_week: ', current_week)
current_week_id = long(current_week.strftime("%Y%W"))

week_fake = 99999999999L
WEEK_PERIOD_ID = 1L


sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")




def get_df_change_status_history():
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_DATABASE + DATABASE_ADVX,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": TABLE_ADVX_CHANGE_STATUS_HISTORY,
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/" + DATABASE_ADVX}
    )

    df_change_status_history = dynamic_frame.toDF()
    df_change_status_history = df_change_status_history.withColumn(
        'date_id', f.from_unixtime(f.unix_timestamp('change_time', "yyyy-MM-dd"), "yyyyMMdd")
    )

    df_change_status_history = df_change_status_history.select(
        'id',
        'contact_id',
        f.col('new_status').alias('status'),
        'date_id'
    )
    return df_change_status_history

STATUS_S0 = 'S0'
STATUS_S1 = 'S1'

STATUS_S2A = 'S2E'
STATUS_S2B = 'S2B'
STATUS_S2C = 'S2C'
STATUS_S2D = 'S2D'
STATUS_S2E = 'S2E'

STATUS_S3 = 'S3'
STATUS_S4 = 'S4'
STATUS_S5 = 'S5'
STATUS_S6 = 'S6'


DateStatusStructType = StructType([
    StructField("s0_date_id", LongType(), False),
    StructField("s1_date_id", LongType(), False),

    StructField("s2a_date_id", LongType(), False),
    StructField("s2b_date_id", LongType(), False),
    StructField("s2c_date_id", LongType(), False),
    StructField("s2d_date_id", LongType(), False),
    StructField("s2e_date_id", LongType(), False),

    StructField("s3_date_id", LongType(), False),
    StructField("s4_date_id", LongType(), False),
    StructField("s5_date_id", LongType(), False),
    StructField("s6_date_id", LongType(), False)
])


def get_status_date_list(pair_status_date_id_list):
    s0_date_id = None
    s1_date_id = None

    s2a_date_id = None
    s2b_date_id = None
    s2c_date_id = None
    s2d_date_id = None
    s2e_date_id = None

    s3_date_id = None
    s4_date_id = None
    s5_date_id = None
    s6_date_id = None

    for pair_status_date_id in pair_status_date_id_list:
        status = pair_status_date_id[0]
        date_id = long(pair_status_date_id[1])
        if STATUS_S0 == status:
            s0_date_id = date_id
            continue
        if STATUS_S1 == status:
            s1_date_id = date_id
            continue

        if STATUS_S2A == status:
            s2a_date_id = date_id
            continue
        if STATUS_S2B == status:
            s2b_date_id = date_id
            continue
        if STATUS_S2C == status:
            s2c_date_id = date_id
            continue
        if STATUS_S2D == status:
            s2d_date_id = date_id
            continue
        if STATUS_S2E == status:
            s2e_date_id = date_id
            continue

        if STATUS_S3 == status:
            s3_date_id = date_id
            continue
        if STATUS_S4 == status:
            s4_date_id = date_id
            continue
        if STATUS_S5 == status:
            s5_date_id = date_id
            continue
        if STATUS_S6 == status:
            s6_date_id = date_id


    StructField("s0_date_id", LongType(), False),
    StructField("s1_date_id", LongType(), False),

    StructField("s2a_date_id", LongType(), False),
    StructField("s2b_date_id", LongType(), False),
    StructField("s2c_date_id", LongType(), False),
    StructField("s2d_date_id", LongType(), False),
    StructField("s2e_date_id", LongType(), False),

    StructField("s3_date_id", LongType(), False),
    StructField("s4_date_id", LongType(), False),
    StructField("s5_date_id", LongType(), False),
    StructField("s6_date_id", LongType(), False)

    return Row('s0_date_id',
               's1_date_id',

               's2a_date_id',
               's2b_date_id',
               's2c_date_id',
               's2d_date_id',
               's2e_date_id',

               's3_date_id',
               's4_date_id',
               's5_date_id',
               's6_date_id'
               )(s0_date_id,
                 s1_date_id,

                 s2a_date_id,
                 s2b_date_id,
                 s2c_date_id,
                 s2d_date_id,
                 s2e_date_id,

                 s3_date_id,
                 s4_date_id,
                 s5_date_id,
                 s6_date_id
                )

udf_get_status_date_list = f.udf(get_status_date_list, DateStatusStructType)

def main():

    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    df_status_history = get_df_change_status_history()
    print('df_status_history')
    df_status_history.show(3)

    #convert from
    df_status_history_pair_status_date_id = df_status_history.select(
        'id',
        'contact_id',
        f.struct('status', 'date_id').alias('pair_status_date_id')
    )
    print('df_status_history_pair_status_date_id')
    df_status_history_list_status_date_id = df_status_history_pair_status_date_id\
        .groupBy('contact_id').agg(
            f.collect_list('pair_status_date_id').alias('pair_status_date_id_list')
        )

    print('df_status_history_list_status_date_id')
    df_status_history_list_status_date_id.show(3)

    df_status_history_list_status_date_id = df_status_history_list_status_date_id\
        .withColumn('status_date_list', udf_get_status_date_list('pair_status_date_id_list'))

    print('df_status_history_list_status_date_id')
    df_status_history_list_status_date_id.show(3)

    df_status_history_status_date_id_detail = df_status_history_list_status_date_id.select(
        'contact_id',
        f.col("status_date_list").getItem("s0_date_id").alias("s0_date_id"),
        f.col("status_date_list").getItem("s1_date_id").alias("s1_date_id"),
        f.col("status_date_list").getItem("s2a_date_id").alias("s2a_date_id"),
        f.col("status_date_list").getItem("s2b_date_id").alias("s2b_date_id"),
        f.col("status_date_list").getItem("s2c_date_id").alias("s2c_date_id"),
        f.col("status_date_list").getItem("s2d_date_id").alias("s2d_date_id"),
        f.col("status_date_list").getItem("s2e_date_id").alias("s2e_date_id"),

        f.col("status_date_list").getItem("s3_date_id").alias("s3_date_id"),
        f.col("status_date_list").getItem("s4_date_id").alias("s4_date_id"),
        f.col("status_date_list").getItem("s5_date_id").alias("s5_date_id"),
        f.col("status_date_list").getItem("s6_date_id").alias("s6_date_id")

    )

    print('df_status_history_status_date_id_detail')
    df_status_history_status_date_id_detail.show(3)




if __name__ == "__main__":
    main()
