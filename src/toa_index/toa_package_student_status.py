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

is_dev = True
REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log"

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_week = datetime.now(ho_chi_minh_timezone)
print('current_week: ', current_week)
current_week_id = long(current_week.strftime("%Y%W"))

week_fake = 99999999999L
WEEK_PERIOD_ID = 1L


def get_weeks(a, b):
    if a is None or b is None:
        return [week_fake]

    weeks = []
    a_t = datetime.fromtimestamp(float(a), ho_chi_minh_timezone)
    # a_t_week_id = long(a_t.strftime("%Y%W"))

    if b == 99999999999L:
        b_t = current_week
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


get_weeks = f.udf(get_weeks, ArrayType(LongType()))


def get_df_student_package(glueContext):
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_DATABASE,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_package",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )

    # | -- student_package_id: long
    # | -- user_id: long
    # | -- contact_id: string
    # | -- student_id: long
    # | -- package_code: string
    # | -- package_status_code: string
    # | -- package_start_time: long
    # | -- package_end_time: long
    # | -- created_at: long
    # | -- updated_at: long

    dyf_student_package = dyf_student_package.select_fields(
        ['contact_id', 'package_code', 'package_status_code',
         'package_start_time', 'package_end_time'])

    if is_dev:
        print('dyf_student_package__original')
        dyf_student_package.printSchema()
        dyf_student_package.show(3)

    return dyf_student_package.toDF()


def get_df_student_level(glue_context):
    dyf_student_level = glue_context.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_DATABASE,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_level",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )
    dyf_student_level = dyf_student_level.select_fields(
        ['contact_id', 'level_code', 'start_date', 'end_date'])

    if is_dev:
        print('dyf_student_level__original')
        dyf_student_level.printSchema()
        dyf_student_level.show(5)

    return dyf_student_level.toDF()


def split_student_package_week(df_student_package):
    df_student_package_week = df_student_package \
        .select(
        'contact_id',
        'package_code',
        'package_status_code',
        get_weeks('package_start_time', 'package_end_time').alias('week_id_s')
    )

    df_student_package_week = df_student_package_week \
        .select(
        'contact_id',
        'package_code',
        'package_status_code',
        f.explode('week_id_s').alias('week_id')
    )

    df_student_package_week = df_student_package_week \
        .filter(df_student_package_week.week_id != week_fake)

    if is_dev:
        print ('df_student_package_week')
        df_student_package_week.printSchema()
        df_student_package_week.show(3)

    return df_student_package_week


def split_student_package_level(df_student_level):
    df_student_levelweek = df_student_level \
        .select(
        'contact_id',
        'level_code',
        get_weeks('start_date', 'end_date').alias('week_id_s')
    )

    df_student_levelweek = df_student_levelweek \
        .select(
        'contact_id',
        'level_code',
        f.explode('week_id_s').alias('week_id')
    )

    df_student_levelweek = df_student_levelweek \
        .filter(df_student_levelweek.week_id != week_fake)

    if is_dev:
        print ('df_student_levelweek')
        df_student_levelweek.printSchema()
        df_student_levelweek.show(3)

    return df_student_levelweek


def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    # =============
    df_student_package = get_df_student_package(glueContext)
    df_student_package.cache()

    student_package_number = df_student_package.count()
    if is_dev:
        print ('student_package_number: ', student_package_number)
    if student_package_number < 1:
        return

    # =============
    df_student_level = get_df_student_level(glueContext)
    df_student_level.cache()

    df_student_level_number = df_student_level.count()
    if is_dev:
        print ('student_level_number: ', df_student_level_number)
    if df_student_level_number < 1:
        return

    # =============
    df_student_package_week = split_student_package_week(df_student_package)
    df_student_level_week = split_student_package_level(df_student_level)

    df_student_package_status_by_date = df_student_package_week.join(
        df_student_level_week,
        on=['contact_id', 'week_id'],
        how='left'
    )

    dyf_student_package_status_by_date = DynamicFrame \
        .fromDF(df_student_package_status_by_date, glueContext, 'df_student_package_status_by_date')

    if is_dev:
        print ('dyf_student_package_status_by_date')
        dyf_student_package_status_by_date.printSchema()
        dyf_student_package_status_by_date.show(20)

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_package_status_by_date,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "df_student_package_status_by_date_testing_v3",
                                                                   "database": "student_native_report"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/df_student_package_status_by_date_testing_v3",
                                                               transformation_ctx="datasink4")

    df_student_package_staus_group_week = df_student_package_status_by_date \
        .groupBy('week_id', 'package_code', 'package_status_code', 'level_code').agg(
        f.count('contact_id').alias('number_student')
    ).withColumn('period_id', f.lit(WEEK_PERIOD_ID))

    print('df_student_package_staus_group_week')
    df_student_package_staus_group_week.printSchema()
    df_student_package_staus_group_week.show(3)

    dyf_student_package_staus_group_week = DynamicFrame \
        .fromDF(df_student_package_staus_group_week, glueContext, 'dyf_student_package_staus_group_week')

    apply_ouput = ApplyMapping.apply(frame=dyf_student_package_staus_group_week,
                                     mappings=[("period_id", "long", "period_id", "long"),
                                               ("week_id", "long", "time_id", "long"),
                                               ("package_code", "string", "package_code", "string"),
                                               ("package_status_code", "string", "package_status_code", "string"),
                                               ("level_code", "string", "student_level_code", "string"),
                                               ("number_student", "long", "number_student", "long")
                                               ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    if is_dev:
        print ('dfy_output')
        dfy_output.printSchema()
        dfy_output.show(20)

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "toa_student_package_status_v3",
                                                                   "database": "student_native_report"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/student_native_report",
                                                               transformation_ctx="datasink4")


if __name__ == "__main__":
    main()
