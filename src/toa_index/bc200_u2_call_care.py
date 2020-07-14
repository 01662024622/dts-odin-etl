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
from pyspark.sql.types import StringType, Row
from pyspark.sql.functions import when
from datetime import date, datetime, timedelta
import pytz
import hashlib
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField
import calendar

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
REPORT_ROLE_MANAGER_ID = 1L
DURATION_LS_SC_SUCCESS = 2160
DURATION_LT_SUCCESS = 1500

BEHAVIOR_ID_LS = '11'
BEHAVIOR_ID_SC = '12'
BEHAVIOR_ID_LT = '13'

STUDENT_LEVEL_TOA = {
    'starter': 1L,
    'sbasic': 2L,
    'basic': 3L,
    'preinter': 4L,
    'inter': 5L
}

MAPPING_LEVEL_TOA_LEVEL = [
    ['$starter$', STUDENT_LEVEL_TOA['starter']],

    ['$sbasic$', STUDENT_LEVEL_TOA['sbasic']],

    ['$basic$', STUDENT_LEVEL_TOA['basic']],
    ['$basic100$', STUDENT_LEVEL_TOA['basic']],

    ['$basic200$', STUDENT_LEVEL_TOA['preinter']],
    ['$basic300$', STUDENT_LEVEL_TOA['preinter']],

    ['$inter100$', STUDENT_LEVEL_TOA['inter']],
    ['$inter300$', STUDENT_LEVEL_TOA['inter']],
    ['$inter200$', STUDENT_LEVEL_TOA['inter']],
    ['$inter$', STUDENT_LEVEL_TOA['inter']],

    ['$advan$', STUDENT_LEVEL_TOA['inter']],
    ['$advan300$', STUDENT_LEVEL_TOA['inter']],
    ['$advan100$', STUDENT_LEVEL_TOA['inter']]
]

# advan
# advan300
# advan100


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
PACKAGE_STATUS_DATA_ACTIVED = 1L
PACKAGE_STATUS_DATA_SUSPENDED = 2L
PACKAGE_STATUS_DATA_DEACTIVED = 3L
PACKAGE_STATUS_DATA_CANCELLED = 4L
PACKAGE_STATUS_DATA_EXPIRED = 5L
PACKAGE_STATUS_DATA_UNAVAILABLE = 6L


def is_active(status_id):
    if status_id in [PACKAGE_STATUS_DATA_ACTIVED, PACKAGE_STATUS_DATA_SUSPENDED, PACKAGE_STATUS_DATA_EXPIRED]:
        return 1L
    return 0L


is_active = f.udf(is_active, LongType())


def is_ls_sc_lt_success(behavior_id, duration):
    if behavior_id in [BEHAVIOR_ID_LS, BEHAVIOR_ID_SC]:
        if duration >= DURATION_LS_SC_SUCCESS:
            return 1L
    if behavior_id == BEHAVIOR_ID_LT:
        if duration >= DURATION_LT_SUCCESS:
            return 1L
    return 0L


is_ls_sc_lt_success = f.udf(is_ls_sc_lt_success, LongType())


def is_ls_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_LS \
            and duration >= DURATION_LS_SC_SUCCESS:
        return 1L
    return 0L


is_ls_success = f.udf(is_ls_success, LongType())


def is_sc_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_SC \
            and duration >= DURATION_LS_SC_SUCCESS:
        return 1L
    return 0L


is_sc_success = f.udf(is_sc_success, LongType())


def is_lt_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_LT \
            and duration >= DURATION_LT_SUCCESS:
        return 1L
    return 0L


is_lt_success = f.udf(is_lt_success, LongType())


def find(lists, key):
    for items in lists:
        if key.startswith(items[0]):
            return items[1]
    return 0L


def get_student_level_id(student_level):
    return find(MAPPING_LEVEL_TOA_LEVEL, '$' + student_level.lower() + '$')
    # return find(STUDENT_LEVEL_DATA, student_level)


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


TimeStructType = StructType([
    StructField("week_id", LongType(), False),
    StructField("next_week_id", LongType(), False),
])


def get_weeks(a, b):
    if a is None or b is None:
        return [Row("week_id", "next_week_id")(week_fake, week_fake)]

    weeks = []
    a_t = datetime.fromtimestamp(float(a), ho_chi_minh_timezone)
    # a_t_week_id = long(a_t.strftime("%Y%W"))

    if b == 99999999999L:
        b_t = current_week
    else:
        b_t = datetime.fromtimestamp(b, ho_chi_minh_timezone)
    b_t_week_id = long(b_t.strftime("%Y%W"))

    # get first date of week
    date_item = a_t
    # date_item = a_t - timedelta(a_t.weekday())
    while long(date_item.strftime("%Y%W")) < b_t_week_id:
        # weeks.append(long(date_item.strftime("%Y%W")))
        text_week_time = date_item + timedelta(7)
        week_id = long(date_item.strftime("%Y%W"))
        next_week_id = long(text_week_time.strftime("%Y%W"))
        week_and_next_week = Row("week_id", "next_week_id")(week_id, next_week_id)
        weeks.append(week_and_next_week)
        date_item += timedelta(7)

    if len(weeks) == 0:
        weeks = [Row("week_id", "next_week_id")(week_fake, week_fake)]

    return weeks


get_weeks = f.udf(get_weeks, ArrayType(TimeStructType))


# def get_week_timestamp_from_week_id(week_id):
#     return 0L
#
# get_week_timestamp_from_week_id = f.udf(get_week_timestamp_from_week_id, LongType())


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
        ['contact_id', 'student_id', 'package_code', 'package_status_code',
         'package_start_time', 'package_end_time'])

    dyf_student_package = Filter.apply(frame=dyf_student_package,
                                       f=lambda x: x["student_id"] is not None and x["student_id"] != 0)

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


def split_student_package_week(df_student_package, start_year_week_id, end_year_week_id):
    if is_dev:
        print('split_student_package_week')
        print('start_year_week_id: ', start_year_week_id)
        print('end_year_week_id: ', end_year_week_id)
    df_student_package_week = df_student_package \
        .select(
        'contact_id',
        'package_code',
        'package_status_code',
        get_weeks('package_start_time', 'package_end_time').alias('week_next_week_s')
    )

    if is_dev:
        print ('df_student_package_week____1')
        df_student_package_week.printSchema()
        df_student_package_week.show(3)

    df_student_package_week = df_student_package_week \
        .select(
        'contact_id',
        'package_code',
        'package_status_code',
        f.explode('week_next_week_s').alias('week_next_week')
    )

    df_student_package_week = df_student_package_week.select(
        'contact_id',
        get_package_id('package_code').alias('package_id'),
        get_package_status_id('package_status_code').alias('package_status_id'),
        f.col("week_next_week").getItem("week_id").alias("week_id"),
        f.col("week_next_week").getItem("next_week_id").alias("next_week_id")
    )

    df_student_package_week = df_student_package_week \
        .filter(df_student_package_week.week_id != week_fake)

    # just get packge
    # ['TAAM-TT', 1L],
    # ['TENUP', 2L],
    # ['TAAM-TC', 3L],

    # df_student_package_week = df_student_package_week\
    #     .filter(
    #         df_student_package_week.package_id in (1L, 2L, 3L)
    # )

    df_student_package_week = df_student_package_week \
        .filter(((df_student_package_week.package_id == 1L)
                 | (df_student_package_week.package_id == 2L)
                 | (df_student_package_week.package_id == 3L))
                & ((df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_ACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_SUSPENDED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_DEACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_EXPIRED)
                   )
                & (df_student_package_week.week_id >= start_year_week_id)
                & (df_student_package_week.week_id <= end_year_week_id)
                )

    if is_dev:
        print ('df_student_package_week__________________________')
        df_student_package_week.printSchema()
        df_student_package_week.show(3)

    df_student_package_week_focus = df_student_package_week.select(
        'contact_id',
        'package_id',
        'package_status_id',
        "week_id"
    )

    df_student_package_week_temp = df_student_package_week.select(
        'contact_id',
        df_student_package_week.package_status_id.alias('package_prev_status_id'),
        df_student_package_week.next_week_id.alias('week_id')
    )

    df_student_package_status_and_prev_status = df_student_package_week_focus \
        .join(df_student_package_week_temp, ['contact_id', 'week_id'], 'left')

    if is_dev:
        print ('df_student_package_status_and_prev_status____________')
        df_student_package_status_and_prev_status.printSchema()
        df_student_package_status_and_prev_status.show(3)

    # | -- contact_id: string(nullable=true)
    # | -- week_id: long(nullable=true)
    # | -- package_id: long(nullable=true)
    # | -- package_status_id: long(nullable=true)
    # | -- package_prev_status_id: long(nullable=true)

    # remove expire last week
    df_student_package_status_and_prev_status = df_student_package_status_and_prev_status \
        .filter((df_student_package_status_and_prev_status.package_status_id != PACKAGE_STATUS_DATA_EXPIRED)
                | (df_student_package_status_and_prev_status.package_prev_status_id != PACKAGE_STATUS_DATA_EXPIRED))

    # get top
    # w2 = Window.partitionBy('contact_id', 'package_id', 'week_id').orderBy(f.col("package_status_id").asc())
    # df_student_package_week_prioritize_active = df_student_package_week \
    #     .withColumn("row", f.row_number().over(w2)) \
    #     .where(f.col('row') <= 1)
    #
    # df_student_package_week_prioritize_active = df_student_package_week_prioritize_active.drop('row')

    # if is_dev:
    #     print ('df_student_package_week')
    #     df_student_package_week.printSchema()
    #     df_student_package_week.show(3)



    return df_student_package_status_and_prev_status


def convert_start_end_to_week_id(df_student_level):
    # 'contact_id', 'level_code', 'start_date', 'end_date'
    # df_student_level = df_student_level.select(
    #     df_student_level.contact_id.alias('contact_id_level'),
    #     get_student_level_id('level_code').alias('student_level_id'),
    #     'start_date',
    #     get_week_id('start_date').alias('start_week_id'),
    #     get_week_id('end_date').alias('end_week_id')
    # )
    #
    # df_student_level = df_student_level.orderBy('contact_id_level', 'start_week_id', f.desc('start_date'))
    #
    # df_student_level = df_student_level.groupBy('contact_id_level', 'start_week_id').agg(
    #     f.first('student_level_id').alias('student_level_id'),
    #     f.first('end_week_id').alias('student_level_id')
    # )

    df_student_level = df_student_level.select(
        df_student_level.contact_id.alias('contact_id_level'),
        get_student_level_id('level_code').alias('student_level_id'),
        'start_date',
        'end_date'
    )

    # df_student_level = df_student_level.orderBy('contact_id_level', 'start_week_id', f.desc('start_date'))
    #
    # df_student_level = df_student_level.groupBy('contact_id_level', 'start_week_id').agg(
    #     f.first('student_level_id').alias('student_level_id'),
    #     f.first('end_week_id').alias('student_level_id')
    # )

    return df_student_level


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

    df_student_levelweek = df_student_levelweek.select(
        'contact_id',
        get_student_level_id('level_code').alias('student_level_id'),
        'week_id'
    )

    df_student_levelweek = df_student_levelweek \
        .filter((df_student_levelweek.student_level_id == 1L)
                | (df_student_levelweek.student_level_id == 2L)
                | (df_student_levelweek.student_level_id == 3L)
                | (df_student_levelweek.student_level_id == 4L)
                | (df_student_levelweek.student_level_id == 5L)
                )

    if is_dev:
        print ('df_student_levelweek')
        df_student_levelweek.printSchema()
        df_student_levelweek.show(3)

    return df_student_levelweek


def get_dyf_sb_student_behavior(glueContext, push_down_predicate):
    # push_down_predicate = '"' + '(behavior_id in ( \'' + BEHAVIOR_ID_LS + '\' , \'' + BEHAVIOR_ID_SC + '\' , \'' + BEHAVIOR_ID_LT + '\')'\
    #                        + ' and year_month_id >= ' + str(start_year_month_id)\
    #                        + ' and year_month_id <= ' + str(end_year_month_id) + ')' + '"'

    print('push_down_predicate')
    print(push_down_predicate)

    dfy_sb_student_behavior = glueContext.create_dynamic_frame.from_catalog(
        database="olap_student_behavior",
        table_name="sb_student_behavior",
        push_down_predicate=push_down_predicate
        # push_down_predicate="((behavior_id == '11' or behavior_id == '12' or behavior_id == '13') and year_month_id >= '201901' and year_month_id < '201905')"
        # additional_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior/*/*"}
    )

    if is_dev:
        print ('dfy_sb_student_behavior')
        dfy_sb_student_behavior.printSchema()
        dfy_sb_student_behavior.show(3)

    dfy_sb_student_behavior = dfy_sb_student_behavior \
        .select_fields(['student_behavior_id',
                        'student_behavior_date',
                        'contact_id',
                        'behavior_id'
                        ])

    return dfy_sb_student_behavior


def get_dyf_sb_student_learning(glueContext, push_down_predicate):
    # push_down_predicate = '(behavior_id in (' + BEHAVIOR_ID_LS + ',' + BEHAVIOR_ID_SC + ',' + BEHAVIOR_ID_LT + ')' \
    #                       + 'and year_month_id >= ' + str(start_year_month_id) \
    #                       + 'and year_month_id <= ' + str(end_year_month_id) + ')'

    dfy_sb_student_learning = glueContext.create_dynamic_frame.from_catalog(
        database="olap_student_behavior",
        table_name="sb_student_learning",
        push_down_predicate=push_down_predicate
        # push_down_predicate="((behavior_id == '11' or behavior_id == '12' or behavior_id == '13') and year_month_id >= '201901' and year_month_id < '201905')"
    )

    # if is_dev:
    #     print ('dfy_sb_student_learning')
    #     dfy_sb_student_learning.printSchema()
    #     dfy_sb_student_learning.show(3)

    dfy_sb_student_learning = dfy_sb_student_learning \
        .select_fields(['student_behavior_id',
                        'duration'
                        ])

    return dfy_sb_student_learning


def get_ls_sc_lt_student_lerning_and_duration(glueContext, start_year_month_id, end_year_month_id):
    push_down_predicate = '((behavior_id == \'' + BEHAVIOR_ID_LS + '\' ' \
                          + ' or behavior_id == \'' + BEHAVIOR_ID_SC + '\' ' \
                          + ' or behavior_id == \'' + BEHAVIOR_ID_LT + '\') ' \
                          + ' and  year_month_id >= \'' + str(start_year_month_id) + '\' ' \
                          + ' and  year_month_id <= \'' + str(end_year_month_id) + '\')'
    dyf_sb_student_behavior = get_dyf_sb_student_behavior(glueContext, push_down_predicate)
    dyf_sb_student_learning = get_dyf_sb_student_learning(glueContext, push_down_predicate)
    dyf_sb_student_behavior_learning = Join.apply(dyf_sb_student_behavior, dyf_sb_student_learning,
                                                  'student_behavior_id', 'student_behavior_id')

    if is_dev:
        print ('dyf_sb_student_behavior_learning _ a _ b _ c _ d ')
        dyf_sb_student_behavior_learning.printSchema()
        dyf_sb_student_behavior_learning.show(3)

        # -- student_behavior_id: string
        # | -- student_behavior_date: long
        # | --.student_behavior_id: string
        # | -- duration: long
        # | -- contact_id: string
        # | -- behavior_id: string

    df_sb_student_behavior_learning = dyf_sb_student_behavior_learning.toDF()
    # df_sb_student_behavior_learning.cache()

    df_sb_student_behavior_learning = df_sb_student_behavior_learning.dropDuplicates(['student_behavior_id'])

    df_sb_student_behavior_learning = df_sb_student_behavior_learning.select(
        'behavior_id',
        'contact_id',
        get_week_id('student_behavior_date').alias('week_id'),
        'duration'
    )

    return df_sb_student_behavior_learning


def get_ls_sc__lt_total_student_lerning_and_duration(glueContext, start_year_month_id, end_year_month_id):
    df_ls_sc_lt_student_lerning_and_duration = \
        get_ls_sc_lt_student_lerning_and_duration(glueContext, start_year_month_id, end_year_month_id)
    df_ls_sc_student_learning_and_duration = df_ls_sc_lt_student_lerning_and_duration \
        .withColumn('ls_sc_lt_success', is_ls_sc_lt_success('behavior_id', 'duration')) \
        .withColumn('ls_success', is_ls_success('behavior_id', 'duration')) \
        .withColumn('sc_success', is_sc_success('behavior_id', 'duration')) \
        .withColumn('lt_success', is_lt_success('behavior_id', 'duration'))

    df_ls_sc_group_contact_week_total_learning = df_ls_sc_student_learning_and_duration \
        .groupBy('contact_id', 'week_id').agg(
        f.count('duration').alias('total_learning_ls_sc_lt_week'),
        f.sum('ls_sc_lt_success').alias('total_learning_ls_sc_lt_success_week'),
        f.sum('ls_success').alias('total_learning_ls_success_week'),
        f.sum('sc_success').alias('total_learning_sc_success_week'),
        f.sum('lt_success').alias('total_learning_lt_success_week'),
        f.sum('duration').alias('total_duration_ls_sc_lt_week')
    )

    return df_ls_sc_group_contact_week_total_learning


def get_year_month_id_from_date(start_date_id, end_date_id):
    start_date_time_timestamp = datetime.strptime(str(start_date_id), "%Y%m%d")
    end_date_time_timestamp = datetime.strptime(str(end_date_id), "%Y%m%d")

    start_year_month_id = long(start_date_time_timestamp.strftime("%Y%m"))
    end_year_month_id = long(end_date_time_timestamp.strftime("%Y%m"))

    return start_year_month_id, end_year_month_id


def get_year_week_id_from_date(start_date_id, end_date_id):
    start_date_time_timestamp = datetime.strptime(str(start_date_id), "%Y%m%d")
    end_date_time_timestamp = datetime.strptime(str(end_date_id), "%Y%m%d")

    start_year_week_id = long(start_date_time_timestamp.strftime("%Y%W"))
    end_year_week_id = long(end_date_time_timestamp.strftime("%Y%W"))

    return start_year_week_id, end_year_week_id


def get_weekend_timestamp(year_week_id):
    date_time = datetime.strptime(str(year_week_id), "%Y%W")
    # print('date_in_week: ', date_time.weekday())
    weekend_timedelta = (6 - date_time.weekday())
    weekend = date_time + timedelta(weekend_timedelta)
    weekend_timestamp = long(calendar.timegm(weekend.utctimetuple()))
    # year_week_id = long(date_time.strftime("%s%f"))
    # print('date_time: ', weekend)
    # print('year_week_id: ', weekend_timestamp)
    return weekend_timestamp

get_weekend_timestamp = f.udf(get_weekend_timestamp, LongType())

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

    start_date_id = 20190101
    end_date_id = 20191231
    print('start_date_id: ', start_date_id)
    print('end_date_id: ', end_date_id)

    start_year_month_id, end_year_month_id = get_year_month_id_from_date(start_date_id, end_date_id)
    start_year_week_id, end_year_week_id = get_year_week_id_from_date(start_date_id, end_date_id)

    print('start_year_month_id: ', start_year_month_id)
    print('end_year_month_id: ', end_year_month_id)

    print('start_year_week_id: ', start_year_week_id)
    print('end_year_week_id: ', end_year_week_id)

    df_student_package = get_df_student_package(glueContext)
    # df_student_package.cache()

    student_package_number = df_student_package.count()
    if is_dev:
        print ('student_package_number: ', student_package_number)
    if student_package_number < 1:
        return

    # =============
    df_student_level = get_df_student_level(glueContext)
    # df_student_level.cache()

    df_student_level_number = df_student_level.count()
    if is_dev:
        print ('student_level_number: ', df_student_level_number)
        print df_student_level.printSchema()
        df_student_level.show(3)
    if df_student_level_number < 1:
        return

    # =============
    df_student_package_week = split_student_package_week(df_student_package, start_year_week_id, end_year_week_id)



if __name__ == "__main__":
    main()
