import time
from datetime import datetime, timedelta

import pyspark.sql.functions as f
import pytz
from pyspark.context import SparkContext
from pyspark.sql.types import ArrayType, LongType, StructType, StructField
from pyspark.sql.types import Row

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = False

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/"

FLAG_BC200_FILE = 's3://toxd-olap/olap/flag/flag_bc200.parquet'

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_week = datetime.now(ho_chi_minh_timezone)
print('current_week: ', current_week)
current_week_id = long(current_week.strftime("%Y%W"))
current_date_id = long(current_week.strftime("%Y%m%d"))

week_fake = 99999999999L
WEEK_PERIOD_ID = 1L
REPORT_ROLE_MANAGER_ID = 1L
DURATION_LS_SC_SUCCESS = 2160
DURATION_LT_SUCCESS = 1500

BEHAVIOR_ID_LS = '11'
BEHAVIOR_ID_SC = '12'
BEHAVIOR_ID_LT = '13'

BEHAVIOR_ID_VOXY = '14'
BEHAVIOR_ID_HOME_WORK = '15'
BEHAVIOR_ID_NATIVE_TALK = '16'
BEHAVIOR_ID_NCSBASIC = '17'

DURATION_VOXY_SUCCESS = 0
DURATION_HOME_WORK_SUCCESS = 0
DURATION_NATIVE_TALK_SUCCESS = 0
DURATION_NCSBASIC_SUCCESS = 0


PACKAGE_ID_TAAM_TT = 1L
PACKAGE_ID_TENUP = 2L
PACKAGE_ID_TAAM_TC = 3L

STUDENT_LEVEL_ID_STARTER = 1L
STUDENT_LEVEL_ID_SBASIC = 2L
STUDENT_LEVEL_ID_BASIC = 3L
STUDENT_LEVEL_ID_PREINTER = 4L
STUDENT_LEVEL_ID_INTER = 5L



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


def is_ls_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_LS \
            and duration >= DURATION_LS_SC_SUCCESS:
        return 1L
    return 0L


def is_sc_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_SC \
            and duration >= DURATION_LS_SC_SUCCESS:
        return 1L
    return 0L


def is_lt_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_LT \
            and duration >= DURATION_LT_SUCCESS:
        return 1L
    return 0L


def is_voxy_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_VOXY \
            and duration >= DURATION_VOXY_SUCCESS:
        return 1L
    return 0L


def is_home_work_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_HOME_WORK \
            and duration >= DURATION_HOME_WORK_SUCCESS:
        return 1L
    return 0L


def is_native_talk_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_NATIVE_TALK \
            and duration >= DURATION_NATIVE_TALK_SUCCESS:
        return 1L
    return 0L


def is_ncsbasic_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_NCSBASIC \
            and duration >= DURATION_NCSBASIC_SUCCESS:
        return 1L
    return 0L


def is_le2_success(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_NCSBASIC and duration >= DURATION_NCSBASIC_SUCCESS:
        return 1L
    if behavior_id == BEHAVIOR_ID_HOME_WORK and duration >= DURATION_HOME_WORK_SUCCESS:
        return 1L
    if behavior_id == BEHAVIOR_ID_NATIVE_TALK and duration >= DURATION_NATIVE_TALK_SUCCESS:
        return 1L
    if behavior_id == BEHAVIOR_ID_VOXY and duration >= DURATION_VOXY_SUCCESS:
        return 1L
    return 0L


def is_duration_voxy(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_VOXY:
        return duration
    return 0L


def is_duration_native_talk(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_NATIVE_TALK:
        return duration
    return 0L


def is_duration_home_work(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_HOME_WORK:
        return duration
    return 0L


def is_duration_ncshasic(behavior_id, duration):
    if behavior_id == BEHAVIOR_ID_NCSBASIC:
        return duration
    return 0L


def caculate_student_bahavior(behavior_id, duration):
    result = []
    result.append(is_ls_sc_lt_success(behavior_id, duration))
    result.append(is_ls_success(behavior_id, duration))
    result.append(is_sc_success(behavior_id, duration))
    result.append(is_lt_success(behavior_id, duration))

    result.append(is_le2_success(behavior_id, duration))
    result.append(is_voxy_success(behavior_id, duration))
    result.append(is_home_work_success(behavior_id, duration))
    result.append(is_native_talk_success(behavior_id, duration))
    result.append(is_ncsbasic_success(behavior_id, duration))

    result.append(is_voxy_success(behavior_id, duration))
    result.append(is_duration_home_work(behavior_id, duration))
    result.append(is_duration_native_talk(behavior_id, duration))
    result.append(is_ncsbasic_success(behavior_id, duration))

    # duration_ls_sc_lt, duration_le2
    duration_ls_sc_lt = 0
    duration_le2 = 0

    if behavior_id in [BEHAVIOR_ID_LS, BEHAVIOR_ID_SC, BEHAVIOR_ID_LT]:
        duration_ls_sc_lt = duration

    if behavior_id in [BEHAVIOR_ID_VOXY, BEHAVIOR_ID_NATIVE_TALK, BEHAVIOR_ID_HOME_WORK, BEHAVIOR_ID_NCSBASIC]:
        duration_le2 = duration

    result.append(duration_ls_sc_lt)
    result.append(duration_le2)

    # total_student_ls_sc_lt_le2
    is_student_ls_sc_lt_le2_success = 0
    if is_ls_sc_lt_success(behavior_id, duration) or is_le2_success(behavior_id, duration):
        is_student_ls_sc_lt_le2_success = 1

    result.append(is_student_ls_sc_lt_le2_success)

    # is_ls_sc_lt, is_le2
    is_ls_sc_lt = 0
    is_le2 = 0

    if behavior_id in [BEHAVIOR_ID_LS, BEHAVIOR_ID_SC, BEHAVIOR_ID_LT]:
        is_ls_sc_lt = 1

    if behavior_id in [BEHAVIOR_ID_VOXY, BEHAVIOR_ID_NATIVE_TALK, BEHAVIOR_ID_HOME_WORK, BEHAVIOR_ID_NCSBASIC]:
        is_le2 = 1

    result.append(is_ls_sc_lt)
    result.append(is_le2)

    return result


udf_caculate_student_bahavior = f.udf(caculate_student_bahavior, ArrayType(LongType()))


def find(lists, key):
    for items in lists:
        if key.startswith(items[0]):
            return items[1]
    return 0L


def get_student_level_id(student_level):
    return find(MAPPING_LEVEL_TOA_LEVEL, '$' + student_level.lower() + '$')


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


udf_get_week_id = f.udf(get_week_id, LongType())


def get_weeks_level(a, b):
    if a is None or b is None:
        return [week_fake]

    weeks = []
    a_t = datetime.fromtimestamp(float(a), ho_chi_minh_timezone)

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

    if b == 99999999999L:
        b_t = current_week
    else:
        b_t = datetime.fromtimestamp(b, ho_chi_minh_timezone)
    b_t_week_id = long(b_t.strftime("%Y%W"))

    date_item = a_t
    while long(date_item.strftime("%Y%W")) < b_t_week_id:
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


def display(data_frame, message):
    if is_dev:
        print "log_data_frame:", message, data_frame.count()
        data_frame.printSchema()
        data_frame.show(10)


def retrieve_data_frame_from_redshift(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_DATABASE + database,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": table_name,
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/" + database}
    )

    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)

    data_frame = dynamic_frame.toDF()
    display(data_frame, table_name)
    return data_frame


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

    # display(df_student_package_week, 'df_student_package_week____1')

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

    df_student_package_week = df_student_package_week \
        .filter(((df_student_package_week.package_id == PACKAGE_ID_TAAM_TT)
                 | (df_student_package_week.package_id == PACKAGE_ID_TENUP)
                 | (df_student_package_week.package_id == PACKAGE_ID_TAAM_TC))
                & ((df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_ACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_SUSPENDED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_DEACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_EXPIRED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_CANCELLED)
                   )
                & (df_student_package_week.week_id >= start_year_week_id)
                & (df_student_package_week.week_id <= end_year_week_id)
                )

    # display(df_student_package_week, 'df_student_package_week__________________________')

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

    # display(df_student_package_status_and_prev_status, 'df_student_package_status_and_prev_status____________')

    df_student_package_status_and_prev_status = df_student_package_status_and_prev_status \
        .filter((df_student_package_status_and_prev_status.package_status_id != PACKAGE_STATUS_DATA_EXPIRED)
                | (df_student_package_status_and_prev_status.package_prev_status_id != PACKAGE_STATUS_DATA_EXPIRED))

    df_student_package_status_and_prev_status = df_student_package_status_and_prev_status\
        .filter((df_student_package_status_and_prev_status.package_status_id != PACKAGE_STATUS_DATA_CANCELLED)
                | (df_student_package_status_and_prev_status.package_prev_status_id != PACKAGE_STATUS_DATA_CANCELLED))

    display(df_student_package_status_and_prev_status, "df_student_package_week split")

    return df_student_package_status_and_prev_status


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
        .filter((df_student_levelweek.student_level_id == STUDENT_LEVEL_ID_STARTER)
                | (df_student_levelweek.student_level_id == STUDENT_LEVEL_ID_SBASIC)
                | (df_student_levelweek.student_level_id == STUDENT_LEVEL_ID_BASIC)
                | (df_student_levelweek.student_level_id == STUDENT_LEVEL_ID_PREINTER)
                | (df_student_levelweek.student_level_id == STUDENT_LEVEL_ID_INTER))

    display(df_student_levelweek, "df_student_levelweek")

    return df_student_levelweek


def retrieve_data_frame(glue_context, database, table_name, push_down_predicate, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database,
                                                                   table_name=table_name,
                                                                   push_down_predicate=push_down_predicate)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)

    data_frame = dynamic_frame.toDF()
    display(data_frame, table_name)
    return data_frame


def get_student_lerning_and_duration(glue_context, start_year_month_id, end_year_month_id):
    push_down_predicate = "((behavior_id == '" + BEHAVIOR_ID_LS + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_SC + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_LT + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_NCSBASIC + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_NATIVE_TALK + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_HOME_WORK + "' " \
                          + " or behavior_id == '" + BEHAVIOR_ID_VOXY + "') " \
                          + " and  year_month_id >= '" + str(start_year_month_id) + "' " \
                          + " and  year_month_id <= '" + str(end_year_month_id) + "') "

    df_student_behavior = retrieve_data_frame(
        glue_context,
        database='olap_student_behavior',
        table_name='sb_student_behavior',
        push_down_predicate=push_down_predicate,
        fields=['student_behavior_id', 'student_behavior_date', 'contact_id', 'behavior_id']
    )

    df_student_learning = retrieve_data_frame(
        glue_context,
        database='olap_student_behavior',
        table_name='sb_student_learning',
        push_down_predicate=push_down_predicate,
        fields=['student_behavior_id', 'duration']
    )

    df_result = df_student_behavior.join(df_student_learning, on=['student_behavior_id'], how='left')

    df_result = df_result.dropDuplicates(['student_behavior_id'])

    df_result = df_result.select(
        'behavior_id',
        'contact_id',
        udf_get_week_id('student_behavior_date').alias('week_id'),
        'duration'
    )

    return df_result


def get_total_student_lerning_and_duration(glueContext, start_year_month_id, end_year_month_id):
    df_result = get_student_lerning_and_duration(glueContext,
                                                 start_year_month_id,
                                                 end_year_month_id)

    df_result = df_result.withColumn('results', udf_caculate_student_bahavior('behavior_id', 'duration'))

    df_result = df_result \
        .withColumn('ls_sc_lt_success', df_result['results'][0]) \
        .withColumn('ls_success', df_result['results'][1]) \
        .withColumn('sc_success', df_result['results'][2]) \
        .withColumn('lt_success', df_result['results'][3]) \
        .withColumn('le2_success', df_result['results'][4]) \
        .withColumn('voxy_success', df_result['results'][5]) \
        .withColumn('home_work_success', df_result['results'][6]) \
        .withColumn('native_talk_success', df_result['results'][7]) \
        .withColumn('ncsbasic_success', df_result['results'][8]) \
        .withColumn('duration_voxy', df_result['results'][9]) \
        .withColumn('duration_home_work', df_result['results'][10]) \
        .withColumn('duration_native_talk', df_result['results'][11]) \
        .withColumn('duration_ncsbasic', df_result['results'][12]) \
        .withColumn('duration_ls_sc_lt', df_result['results'][13]) \
        .withColumn('duration_le2', df_result['results'][14]) \
        .withColumn('ls_sc_lt_le2_success', df_result['results'][15]) \
        .withColumn('ls_sc_lt', df_result['results'][16]) \
        .withColumn('le2', df_result['results'][17])

    df_result = df_result \
        .groupBy('contact_id', 'week_id') \
        .agg(f.count('duration').alias('total_learning_ls_sc_lt_le2_week'),
             f.sum('ls_sc_lt_le2_success').alias('total_learning_ls_sc_lt_le2_success_week'),

             f.sum('ls_sc_lt').alias('total_learning_ls_sc_lt_week'),
             f.sum('ls_sc_lt_success').alias('total_learning_ls_sc_lt_success_week'),

             f.sum('ls_success').alias('total_learning_ls_success_week'),
             f.sum('sc_success').alias('total_learning_sc_success_week'),
             f.sum('lt_success').alias('total_learning_lt_success_week'),

             f.sum('duration_ls_sc_lt').alias('total_duration_ls_sc_lt_week'),

             f.sum('le2').alias('total_learning_le2_week'),
             f.sum('le2_success').alias('total_learning_le2_success_week'),

             f.sum('voxy_success').alias('total_learning_voxy_success_week'),
             f.sum('native_talk_success').alias('total_learning_native_talk_success_week'),
             f.sum('home_work_success').alias('total_learning_home_work_success_week'),
             f.sum('ncsbasic_success').alias('total_learning_ncsbasic_success_week'),

             f.sum('duration_le2').alias('total_duration_le2_week'),
             f.sum('duration_voxy').alias('total_duration_voxy_week'),
             f.sum('duration_native_talk').alias('total_duration_native_talk_week'),
             f.sum('duration_home_work').alias('total_duration_home_work_week'),
             f.sum('duration_ncsbasic').alias('total_duration_ncsbasic_week')
             )

    return df_result


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


def get_weekend_timestamp(week_id):
    r = datetime.strptime(str(week_id) + '-0', "%Y%W-%w")
    weekend_timestamp = time.mktime(r.timetuple())
    return long(weekend_timestamp)


udf_get_weekend_timestamp = f.udf(get_weekend_timestamp, LongType())


def save_data_to_redshift(glue_context, dynamic_frame, database, table, redshift_tmp_dir, transformation_ctx):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir=redshift_tmp_dir,
                                                    transformation_ctx=transformation_ctx)


def get_flag(spark, data_frame):
    flag = data_frame.agg({"week_id": "max"}).collect()[0][0]
    flag_data = [flag]
    return spark.createDataFrame(flag_data, "string").toDF('flag')


def save_flag(data_frame, flag_path):
    data_frame.write.parquet(flag_path, mode="overwrite")


TOPICA_EMAIL_END = '%@topica.edu.vn'

def get_df_student_package():
    df_student_package = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_package',
        ['contact_id', 'student_id', 'package_code', 'package_status_code', 'package_start_time', 'package_end_time']
    )

    dyf_student_contact = glueContext.create_dynamic_frame\
        .from_catalog(database='tig_advisor',
                    table_name='student_contact')

    dyf_student_contact = dyf_student_contact.select_fields(
        ['student_id', 'user_name'])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                      f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                  and x["user_name"] is not None and x["user_name"] != 0)

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])

    df_student_package = df_student_package\
        .join(df_student_contact,
              ['student_id'],
              'inner'
              )

    # df_student_package = df_student_package\
    #     .filter(df_student_package.user_name.endswith(TOPICA_EMAIL_END))

    df_student_package = df_student_package\
        .where('user_name NOT LIKE \'' + TOPICA_EMAIL_END + "\'")

    return df_student_package


def main():
    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    start_date_id = 20200101
    end_date_id = 20200304
    print('start_date_id: ', start_date_id)
    print('end_date_id: ', end_date_id)
    #
    start_year_month_id, end_year_month_id = get_year_month_id_from_date(start_date_id, end_date_id)
    start_year_week_id, end_year_week_id = get_year_week_id_from_date(start_date_id, end_date_id)
    #
    print('start_year_month_id: ', start_year_month_id)
    print('end_year_month_id: ', end_year_month_id)

    print('start_year_week_id: ', start_year_week_id)
    print('end_year_week_id: ', end_year_week_id)

    print('start_year_week_id: ', start_year_week_id)
    print('end_year_week_id: ', end_year_week_id)

    # ------------------------------------------------------------------------------------------------------------------#
    df_student_package = get_df_student_package()
    df_student_package.cache()

    if df_student_package.count() < 1:
        return

    df_student_level = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_level',
        ['contact_id', 'level_code', 'start_date', 'end_date']
    )

    if df_student_level.count() < 1:
        return

    df_student_advisor = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_advisor',
        ['contact_id', 'advisor_id', 'start_date', 'end_date']
    )

    if df_student_advisor.count() < 1:
        return

    # -----------------------------------------------------------------------------------------------------------------#
    df_student_package_week = split_student_package_week(df_student_package, start_year_week_id, end_year_week_id)

    df_student_package_week = df_student_package_week \
        .withColumn('weekend_timestamp', udf_get_weekend_timestamp('week_id'))

    # display(df_student_package_week, "df_student_package_status_by_date get_weekend_timestamp")

    df_student_level_week = df_student_level \
        .withColumnRenamed('contact_id', 'contact_id_level') \
        .withColumn('student_level_id', get_student_level_id('level_code')) \
        .withColumnRenamed('start_date', 'start_date_level') \
        .withColumnRenamed('end_date', 'end_date_level')

    df_student_advisor_week = df_student_advisor \
        .withColumnRenamed('contact_id', 'contact_id_advisor') \
        .withColumnRenamed('start_date', 'start_date_advisor') \
        .withColumnRenamed('end_date', 'end_date_advisor')

    df_student_package_status_by_date = df_student_package_week \
        .join(df_student_level_week,
              on=(df_student_package_week.contact_id == df_student_level_week.contact_id_level)
                 & (df_student_package_week['weekend_timestamp'] >= df_student_level_week['start_date_level'])
                 & (df_student_package_week['weekend_timestamp'] < df_student_level_week['end_date_level']),
              how='left') \
        .join(df_student_advisor_week,
              on=(df_student_package_week.contact_id == df_student_advisor_week.contact_id_advisor)
                 & (df_student_package_week['weekend_timestamp'] >= df_student_advisor_week['start_date_advisor'])
                 & (df_student_package_week['weekend_timestamp'] < df_student_advisor_week['end_date_advisor']),
              how='left')

    df_student_package_status_by_date.cache()

    display(df_student_package_status_by_date, 'df_student_package_status_by_date')

    # -----------------------------------------------------------------------------------------------------------------#
    df_student_learning_and_duration = get_total_student_lerning_and_duration(glueContext,
                                                                              start_year_month_id,
                                                                              end_year_month_id)

    display(df_student_learning_and_duration, 'df_student_learning_and_duration')
    # -----------------------------------------------------------------------------------------------------------------#

    df_student_package_status_by_date_learning = df_student_package_status_by_date \
        .join(df_student_learning_and_duration,
              on=['contact_id', 'week_id'],
              how='left') \
        .na.fill({'advisor_id': -1L,

                  'total_learning_ls_sc_lt_le2_week': 0L,
                  'total_learning_ls_sc_lt_le2_success_week': 0L,

                  'total_learning_ls_sc_lt_week': 0L,
                  'total_learning_ls_sc_lt_success_week': 0L,
                  'total_learning_ls_success_week': 0L,
                  'total_learning_sc_success_week': 0L,
                  'total_learning_lt_success_week': 0L,

                  'total_duration_ls_sc_lt_week': 0L,

                  'total_learning_le2_week': 0L,
                  'total_learning_le2_success_week': 0L,
                  'total_learning_voxy_success_week': 0L,
                  'total_learning_native_talk_success_week': 0L,
                  'total_learning_home_work_success_week': 0L,
                  'total_learning_ncsbasic_success_week': 0L,

                  'total_duration_le2_week': 0L,
                  'total_duration_voxy_week': 0L,
                  'total_duration_native_talk_week': 0L,
                  'total_duration_home_work_week': 0L,
                  'total_duration_ncsbasic_week': 0L,

                  })

    display(df_student_package_status_by_date_learning, 'df_student_package_status_by_date_learning')

    # -----------------------------------------------------------------------------------------------------------------#
    df_student_package_status_by_date_learning = df_student_package_status_by_date_learning \
        .select('week_id', 'package_id', 'student_level_id', 'contact_id', 'advisor_id', 'weekend_timestamp',
                is_active('package_status_id').alias('is_active'),

                f.when(df_student_package_status_by_date_learning['total_learning_ls_sc_lt_le2_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ls_sc_lt_le2'),
                f.when(df_student_package_status_by_date_learning['total_learning_ls_sc_lt_le2_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ls_sc_lt_le2_success'),

                f.when(df_student_package_status_by_date_learning['total_learning_ls_sc_lt_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ls_sc_lt'),
                f.when(df_student_package_status_by_date_learning['total_learning_ls_sc_lt_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ls_sc_lt_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_ls_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ls_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_sc_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_sc_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_lt_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_lt_success'),

                f.when(df_student_package_status_by_date_learning['total_learning_le2_week'] > 0L, 1L)
                .otherwise(0L).alias('is_le2'),
                f.when(df_student_package_status_by_date_learning['total_learning_le2_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_le2_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_voxy_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_voxy_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_native_talk_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_native_talk_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_home_work_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_home_work_success'),
                f.when(df_student_package_status_by_date_learning['total_learning_ncsbasic_success_week'] > 0L, 1L)
                .otherwise(0L).alias('is_ncsbasic_success'),

                'total_learning_ls_sc_lt_le2_week',
                'total_learning_ls_sc_lt_le2_success_week',

                'total_learning_ls_sc_lt_week',
                'total_learning_ls_sc_lt_success_week',
                'total_learning_ls_success_week',
                'total_learning_sc_success_week',
                'total_learning_lt_success_week',

                'total_duration_ls_sc_lt_week',

                'total_learning_le2_week',
                'total_learning_le2_success_week',
                'total_learning_voxy_success_week',
                'total_learning_native_talk_success_week',
                'total_learning_home_work_success_week',
                'total_learning_ncsbasic_success_week',

                'total_duration_le2_week',
                'total_duration_voxy_week',
                'total_duration_native_talk_week',
                'total_duration_home_work_week',
                'total_duration_ncsbasic_week'
                )

    dyf_test = DynamicFrame.fromDF(df_student_package_status_by_date_learning,
                                  glueContext,
                                  'dyf_test')

    table_backup_checking = 'bc200.bc200_fact_by_date_running_date_' + str(current_date_id)

    preactions = 'DROP table  IF EXISTS ' + table_backup_checking
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_test,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       # "preactions": preactions,
                                                       "dbtable": table_backup_checking,
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/bc200_fact_v2_pre" + table_backup_checking,
                                                   transformation_ctx="datasink4")

    # ----------

    df_student_package_status_group_week = df_student_package_status_by_date_learning \
        .groupBy('week_id', 'package_id', 'student_level_id', 'advisor_id') \
        .agg(f.count('contact_id').alias('total_student'),
             f.sum('is_active').alias('total_student_active'),

             f.sum('is_ls_sc_lt_le2').alias('total_student_ls_sc_lt_le2'),
             f.sum('is_ls_sc_lt_le2_success').alias('total_student_ls_sc_lt_le2_success'),

             f.sum('total_learning_ls_sc_lt_le2_week').alias('total_learning_ls_sc_lt_le2'),
             f.sum('total_learning_ls_sc_lt_le2_success_week').alias('total_learning_ls_sc_lt_le2_success'),

             f.sum('is_ls_sc_lt').alias('total_student_ls_sc_lt'),
             f.sum('is_ls_sc_lt_success').alias('total_student_ls_sc_lt_success'),
             f.sum('is_ls_success').alias('total_student_ls_success'),
             f.sum('is_sc_success').alias('total_student_sc_success'),
             f.sum('is_lt_success').alias('total_student_lt_success'),

             f.sum('total_learning_ls_sc_lt_week').alias('total_learning_ls_sc_lt'),
             f.sum('total_learning_ls_sc_lt_success_week').alias('total_learning_ls_sc_lt_success'),
             f.sum('total_learning_ls_success_week').alias('total_learning_ls_success'),
             f.sum('total_learning_sc_success_week').alias('total_learning_sc_success'),
             f.sum('total_learning_lt_success_week').alias('total_learning_lt_success'),

             f.sum('total_duration_ls_sc_lt_week').alias('total_duration_ls_sc_lt'),

             f.sum('is_le2').alias('total_student_le2'),
             f.sum('is_le2_success').alias('total_student_le2_success'),
             f.sum('is_voxy_success').alias('total_student_voxy_success'),
             f.sum('is_native_talk_success').alias('total_student_native_talk_success'),
             f.sum('is_home_work_success').alias('total_student_home_work_success'),
             f.sum('is_ncsbasic_success').alias('total_student_ncsbasic_success'),

             f.sum('total_learning_le2_week').alias('total_learning_le2'),
             f.sum('total_learning_le2_success_week').alias('total_learning_le2_success'),
             f.sum('total_learning_voxy_success_week').alias('total_learning_voxy__success'),
             f.sum('total_learning_native_talk_success_week').alias('total_learning_native_talk_success'),
             f.sum('total_learning_home_work_success_week').alias('total_learning_home_work_success'),
             f.sum('total_learning_ncsbasic_success_week').alias('total_learning_ncsbasic_success'),

             f.sum('total_duration_le2_week').alias('total_duration_le2'),
             f.sum('total_duration_voxy_week').alias('total_duration_voxy'),
             f.sum('total_duration_native_talk_week').alias('total_duration_native_talk'),
             f.sum('total_duration_home_work_week').alias('total_duration_home_work'),
             f.sum('total_duration_ncsbasic_week').alias('total_duration_ncsbasic')
             ) \
        .withColumn('period_id', f.lit(WEEK_PERIOD_ID)) \
        .withColumn('report_role_id', f.lit(REPORT_ROLE_MANAGER_ID))

    # display(df_student_package_status_group_week, "df_student_package_status_group_week")

    dyf_student_package_status_group_week = DynamicFrame.fromDF(df_student_package_status_group_week,
                                                                glueContext,
                                                                'dyf_student_package_status_group_week')

    apply_ouput = ApplyMapping \
        .apply(frame=dyf_student_package_status_group_week,
               mappings=[("report_role_id", "long", "report_role_id", "long"),
                         ("period_id", "long", "period_id", "long"),
                         ("week_id", "long", "time_id", "long"),

                         ("package_id", "long", "package_id", "long"),
                         ("student_level_id", "long", "student_level_id", "long"),
                         ("advisor_id", "long", "advisor_id", "long"),

                         ("total_student", "long", "total_student", "long"),
                         ("total_student_active", "long", "total_student_active", "long"),

                         ("total_student_ls_sc_lt_le2", "long", "total_student_ls_sc_lt_le2", "long"),
                         ("total_student_ls_sc_lt_le2_success", "long", "total_student_ls_sc_lt_le2_success", "long"),
                         ("total_learning_ls_sc_lt_le2", "long", "total_learning_ls_sc_lt_le2", "long"),
                         ("total_learning_ls_sc_lt_le2_success", "long", "total_learning_ls_sc_lt_le2_success", "long"),

                         ("total_student_ls_sc_lt", "long", "total_student_ls_sc_lt", "long"),
                         ("total_student_ls_sc_lt_success", "long", "total_student_ls_sc_lt_success", "long"),
                         ("total_student_ls_success", "long", "total_student_ls_success", "long"),
                         ("total_student_sc_success", "long", "total_student_sc_success", "long"),
                         ("total_student_lt_success", "long", "total_student_lt_success", "long"),

                         ("total_learning_ls_sc_lt", "long", "total_learning_ls_sc_lt", "long"),
                         ("total_learning_ls_sc_lt_success", "long", "total_learning_ls_sc_lt_success", "long"),
                         ("total_learning_ls_success", "long", "total_learning_ls_success", "long"),
                         ("total_learning_sc_success", "long", "total_learning_sc_success", "long"),
                         ("total_learning_lt_success", "long", "total_learning_lt_success", "long"),

                         ("total_duration_ls_sc_lt", "long", "total_duration_ls_sc_lt", "long"),

                         ("total_student_le2", "long", "total_student_le2", "long"),
                         ("total_student_le2_success", "long", "total_student_le2_success", "long"),
                         ("total_student_voxy_success", "long", "total_student_voxy_success", "long"),
                         ("total_student_native_talk_success", "long", "total_student_native_talk_success", "long"),
                         ("total_student_home_work_success", "long", "total_student_home_work_success", "long"),
                         ("total_student_ncsbasic_success", "long", "total_student_ncsbasic_success", "long"),

                         ("total_learning_le2", "long", "total_learning_le2", "long"),
                         ("total_learning_le2_success", "long", "total_learning_le2_success", "long"),
                         ("total_learning_voxy__success", "long", "total_learning_voxy__success", "long"),
                         ("total_learning_native_talk_success", "long", "total_learning_native_talk_success", "long"),
                         ("total_learning_home_work_success", "long", "total_learning_home_work_success", "long"),
                         ("total_learning_ncsbasic_success", "long", "total_learning_ncsbasic_success", "long"),

                         ("total_duration_le2", "long", "total_duration_le2", "long"),
                         ("total_duration_voxy", "long", "total_duration_voxy", "long"),
                         ("total_duration_native_talk", "long", "total_duration_native_talk", "long"),
                         ("total_duration_home_work", "long", "total_duration_home_work", "long"),
                         ("total_duration_ncsbasic", "long", "total_duration_ncsbasic", "long")
                         ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    display(dfy_output, "dfy_output")

    # save_data_to_redshift(
    #     glueContext,
    #     dfy_output,
    #     'student_native_report',
    #     'bc200.bc200_fact_v2_1',
    #     "s3n://dts-odin/temp/bc200/bc200_fact_v2_1",
    #     "datasink4")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       # "preactions": """DELETE from bc200.bc200_fact_v2_1 WHERE time_id >= 202000 """,
                                                       "dbtable": "bc200.bc200_fact_v2_1",
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/bc200_fact_v2",
                                                   transformation_ctx="datasink4")

    df_flag = get_flag(spark=spark, data_frame=df_student_package_status_group_week)
    display(df_flag, "df_flag")

    if df_flag.collect()[0]['flag'] is not None:
        print 'save_flag done'
        save_flag(df_flag, FLAG_BC200_FILE)

    df_student_package.unpersist()

if __name__ == "__main__":
    main()
