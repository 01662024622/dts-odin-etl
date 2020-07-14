import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

import time
from datetime import datetime, timedelta

import pyspark.sql.functions as f
import pytz
from pyspark.context import SparkContext
from pyspark.sql.types import ArrayType, LongType, StructType, StructField, IntegerType, DateType
from pyspark.sql.types import Row


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = False
is_saved_temporary = False

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/"

FLAG_BC200_FILE = 's3://toxd-olap/olap/flag/flag_bc200.parquet'

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_date = datetime.now(ho_chi_minh_timezone)
print('current_week: ', current_date)
current_week_id = long(current_date.strftime("%Y%W"))
current_date_id = long(current_date.strftime("%Y%m%d"))
d4 = current_date.strftime("%Y-%m-%d").replace("-", "")

print('current_date_id: ', current_date_id)

is_limit = False

date_id_fake = 99999999999L
INFINITE_DATE_ID = 99999999999L
WEEK_PERIOD_ID = 1L
DAILY_PERIOD_ID = 2L
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
PACKAGE_ID_VIP3_TT = 4L

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

# ----------------------util---------------------------------------------------------------
HOUR_DAILY_CUT_OFF = 17  # equal 24h local


def get_timestamp_from_date(date_id):
    date_time = datetime.strptime(str(date_id), "%Y%m%d")
    date_time = date_time.replace(hour=HOUR_DAILY_CUT_OFF, minute=0, second=0, microsecond=0,
                                  tzinfo=ho_chi_minh_timezone)
    time_stamp = long(time.mktime(date_time.timetuple()))
    return time_stamp


udf_get_timestamp_from_date = f.udf(get_timestamp_from_date, LongType())

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

def get_time_id(period_id, timestampe):
    if timestampe is None:
        return None

    date = datetime.fromtimestamp(float(timestampe), ho_chi_minh_timezone)
    if period_id == DAILY_PERIOD_ID:
        return long(date.strftime("%Y%m%d"))
    if period_id == WEEK_PERIOD_ID:
        return long(date.strftime("%Y%W"))
    return None

udf_get_time_id = f.udf(get_time_id, LongType())

def get_date_ids_between_start_end(package_status_id, start, end):
    if start is None or end is None:
        return [date_id_fake]

    start_ts = datetime.fromtimestamp(float(start), ho_chi_minh_timezone)

    if end == INFINITE_DATE_ID:
        end_ts = current_date
    else:
        end_ts = datetime.fromtimestamp(end, ho_chi_minh_timezone)
    end_id = long(end_ts.strftime("%Y%m%d"))

    if long(start_ts.strftime("%Y%m%d")) >= end_id:
        return [date_id_fake]

    # with EXPIRED and CANCELLED just get modified date
    if package_status_id in [PACKAGE_STATUS_DATA_EXPIRED, PACKAGE_STATUS_DATA_CANCELLED]:
        return [long(start_ts.strftime("%Y%m%d"))]

    date_item = start_ts

    date_ids = []
    while long(date_item.strftime("%Y%m%d")) < end_id:
        date_id = long(date_item.strftime("%Y%m%d"))
        date_ids.append(date_id)
        date_item += timedelta(1)

    if len(date_ids) == 0:
        date_ids = [date_id_fake]

    return date_ids


udf_get_date_ids_between_start_end = f.udf(get_date_ids_between_start_end, ArrayType(LongType()))


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


# .withColumnRenamed('contact_id', 'contact_id_level') \
#     #     .withColumn('student_level_id', get_student_level_id('level_code')) \
#     #     .withColumnRenamed('start_date', 'start_date_level') \
#     #     .withColumnRenamed('end_date', 'end_date_level')

# 'contact_id', 'advisor_id', 'start_date', 'end_date'

def get_df_student_advisor_by_date(df_student_advisor):
    df_student_advisor = df_student_advisor \
        .select(
        'advisor_id',
        df_student_advisor.contact_id.alias('contact_id_advisor'),
        df_student_advisor.start_date.alias('start_date_advisor'),
        df_student_advisor.end_date.alias('end_date_advisor')
    )
    return df_student_advisor


def get_df_student_level_by_date(df_student_level):
    if is_dev:
        print ('df_student_level')
        df_student_level.printSchema()
        df_student_level.show(3)
    # ['contact_id', 'level_code', 'start_date', 'end_date']

    df_student_level = df_student_level \
        .select(
        get_student_level_id('level_code').alias('student_level_id'),
        df_student_level.contact_id.alias('contact_id_level'),
        df_student_level.start_date.alias('start_date_level'),
        df_student_level.end_date.alias('end_date_level')
    )
    return df_student_level


def get_df_student_package_by_date(df_student_package, start_date_id_focus, end_date_id_focus):
    if is_dev:
        print('split_student_package_week')
        print('start_date_id_focus: ', start_date_id_focus)
        print('end_date_id_focus: ', end_date_id_focus)

    df_student_package_week = df_student_package \
        .select(
        'student_id',
        'contact_id',
        get_package_id('package_code').alias('package_id'),
        get_package_status_id('package_status_code').alias('package_status_id'),
        'package_start_time',
        'package_end_time'
    )

    df_student_package_week = df_student_package_week \
        .select(
        'student_id',
        'contact_id',
        'package_id',
        'package_status_id',
        udf_get_date_ids_between_start_end('package_status_id', 'package_start_time', 'package_end_time').alias(
            'date_ids')
    )

    # if is_dev:
    #     print ('df_student_package_week__date_ids')
    #     df_student_package_week.printSchema()
    #     df_student_package_week.show(3)

    df_student_package_week = df_student_package_week \
        .select(
        'student_id',
        'contact_id',
        'package_id',
        'package_status_id',
        f.explode('date_ids').alias('date_id')
    )

    if is_dev:
        print ('df_student_package_week_step_1')
        df_student_package_week.printSchema()
        df_student_package_week.show(10)

    df_student_package_week = df_student_package_week \
        .filter((df_student_package_week.date_id != date_id_fake)
                & (df_student_package_week.date_id.isNotNull()))

    if is_dev:
        print ('df_student_package_week_step_2')
        df_student_package_week.printSchema()
        df_student_package_week.show(10)

    df_student_package_date_focus = df_student_package_week \
        .filter(((df_student_package_week.package_id == PACKAGE_ID_TAAM_TT)
                 | (df_student_package_week.package_id == PACKAGE_ID_TENUP)
                 | (df_student_package_week.package_id == PACKAGE_ID_TAAM_TC)
                 | (df_student_package_week.package_id == PACKAGE_ID_VIP3_TT))
                & ((df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_ACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_SUSPENDED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_DEACTIVED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_EXPIRED)
                   | (df_student_package_week.package_status_id == PACKAGE_STATUS_DATA_CANCELLED)
                   )
                & (df_student_package_week.date_id >= start_date_id_focus)
                & (df_student_package_week.date_id < end_date_id_focus)
                )

    df_student_package_date_focus = df_student_package_date_focus \
        .withColumn('time_cut_off', udf_get_timestamp_from_date('date_id'))

    if is_dev:
        print ('df_student_package_week_step_3')
        df_student_package_week.printSchema()
        df_student_package_week.show(10)

    # display(df_student_package_week, 'df_student_package_week__________________________')
    if is_dev:
        print ('df_student_package_date_focus')
        df_student_package_date_focus.printSchema()
        df_student_package_date_focus.show(3)

    return df_student_package_date_focus


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

    dyf_student_contact = glueContext.create_dynamic_frame \
        .from_catalog(database='tig_advisor',
                      table_name='student_contact')

    dyf_student_contact = dyf_student_contact.select_fields(
        ['student_id', 'user_name'])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                   and x["user_name"] is not None and x["user_name"] != 0)

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])

    df_student_package = df_student_package \
        .join(df_student_contact,
              ['student_id'],
              'inner'
              )

    # df_student_package = df_student_package\
    #     .filter(df_student_package.user_name.endswith(TOPICA_EMAIL_END))

    df_student_package = df_student_package \
        .where('user_name NOT LIKE \'' + TOPICA_EMAIL_END + "\'")

    return df_student_package


def is_actived(package_status_id):
    if package_status_id in [PACKAGE_STATUS_DATA_ACTIVED,
                             PACKAGE_STATUS_DATA_SUSPENDED,
                             PACKAGE_STATUS_DATA_EXPIRED,
                             PACKAGE_STATUS_DATA_CANCELLED
                             ]:
        return 1L
    return 0L


udf_is_actived = f.udf(is_actived, LongType())

def get_student_package_adivsor_level(start_date_id_focus, end_date_id_focus):
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
    df_student_package_by_date = get_df_student_package_by_date(df_student_package, start_date_id_focus,
                                                                end_date_id_focus)

    df_student_package_by_date.cache()
    df_student_level_by_date = get_df_student_level_by_date(df_student_level)
    df_student_level_by_date.cache()
    df_student_advisor_by_date = get_df_student_advisor_by_date(df_student_advisor)
    df_student_advisor_by_date.cache()
    df_student_package_level_advisor = df_student_package_by_date \
        .join(df_student_level_by_date,
              on=(df_student_package_by_date.contact_id == df_student_level_by_date.contact_id_level)
                 & (df_student_package_by_date.time_cut_off >= df_student_level_by_date.start_date_level)
                 & (df_student_package_by_date.time_cut_off < df_student_level_by_date.end_date_level),
              how='left') \
        .join(df_student_advisor_by_date,
              on=(df_student_package_by_date.contact_id == df_student_advisor_by_date.contact_id_advisor)
                 &(df_student_package_by_date.time_cut_off >= df_student_advisor_by_date.start_date_advisor)
                 & (df_student_package_by_date.time_cut_off < df_student_advisor_by_date.end_date_advisor),
              how='left')
    #

    df_student_level.start_date.alias('start_date_level'),
    df_student_level.end_date.alias('end_date_level')
    if is_dev:
        print ('df_student_package_level_advisor')
        df_student_package_level_advisor.printSchema()
        df_student_package_level_advisor.show()

    # get_package_id('package_code').alias('package_id'),
    # get_package_status_id('package_status_code').alias('package_status_id'),
    df_student_package_level_advisor = df_student_package_level_advisor \
        .select(
        'date_id',
        'student_id',
        'contact_id',
        'package_id',
        'package_status_id',
        'student_level_id',
        'advisor_id',
        udf_is_actived('package_status_id').alias('is_activated')
    )

    if is_saved_temporary:
        dyf_student_package_level_advisor = DynamicFrame \
            .fromDF(df_student_package_level_advisor, glueContext, 'dyf_student_package_level_advisor')
        atasink4 = glueContext.write_dynamic_frame \
            .from_jdbc_conf(frame=dyf_student_package_level_advisor,
                            catalog_connection="glue_redshift",
                            connection_options={
                                "dbtable": "dev.df_student_package_level_advisor",
                                "database": "student_native_report"
                            },
                            redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student/df_student_package_level_advisor",
                            transformation_ctx="datasink4")

    return df_student_package_level_advisor


#-----------------------------------------------------------------------------------------------------------------------
#-----------------end package status level advisor ----------------------------



#----------------start learning ----------------------------------------------------------------------------------------#

#-------------------------------------
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame


def checkDumplicate(df, duplicates):
    if len(duplicates) == 0:
        return df
    else:
        return df.dropDuplicates(duplicates)

def connectGlue(database="", table_name="", select_fields=[], fillter=[], cache=False, duplicates=[],
                push_down_predicate=""):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name,
                                                        push_down_predicate=push_down_predicate)

    if is_dev:
        print("full schema of table: ", database, "and table: ", table_name)
        dyf.printSchema()
    #     select felds
    if len(select_fields) != 0:
        dyf = dyf.select_fields(select_fields)

    if len(fillter) != 0:
        dyf = fillterOutNull(dyf, fillter)

    if is_limit and cache:
        df = dyf.toDF()
        df = df.limit(20000)
        df = checkDumplicate(df, duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit == False and cache:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit and cache == False:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        df = df.limit(20000)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    else:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)

    if is_dev:
        print("full schema of table: ", database, " and table: ", table_name, " after select")
        dyf.printSchema()
    return dyf

def get_df_student_contact():
    dyf_student_contact = connectGlue(database="tig_advisor", table_name="student_contact",
                                          select_fields=["contact_id", "student_id"],
                                          fillter=["contact_id", "student_id"],
                                          duplicates=["student_id"]
                                          )

    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('student_id', 'cast:long')])

    df_student_contact = dyf_student_contact.toDF()
    return df_student_contact








#-----------------------------------
LIST_TOTAL = StructType([
    StructField("total_ls_normal", IntegerType(), False),
    StructField("total_sc_normal", IntegerType(), False),
    StructField("total_ls_audit", IntegerType(), False),
    StructField("total_sc_audit", IntegerType(), False),

    StructField("total_lt", IntegerType(), False),
    StructField("total_voxy", IntegerType(), False),
    StructField("total_hw", IntegerType(), False),
    StructField("total_nt", IntegerType(), False),
    StructField("total_ncsb", IntegerType(), False),

])

def get_final_total(structs):
    ls = sc = lt = voxy = hw = nt = ncsb = audit = 0
    ls_normal = 0
    sc_normal = 0
    ls_audit = 0
    sc_audit = 0
    if structs is not None:
        length_l_p = len(structs)
        for i in range(0, length_l_p):
            ele = structs[i]
            if ele[0] == 11:
                if ele[2] == "AUDIT":
                    ls_audit = ele[1]
                else:
                    ls_normal = ele[1]
            elif ele[0] == 12:
                if ele[2] == "AUDIT":
                    sc_audit = ele[1]
                else:
                    sc_normal = ele[1]
            elif ele[0] == 13:
                lt = ele[1]
            elif ele[0] == 14:
                voxy = ele[1]
            elif ele[0] == 15:
                hw = ele[1]
            elif ele[0] == 16:
                nt = ele[1]
            elif ele[0] == 17:
                ncsb = ele[1]

    final_total = Row("total_ls_normal", "total_sc_normal",
                      'total_ls_audit', 'total_sc_audit',
                      "total_lt",
                      "total_voxy", "total_hw", "total_nt",
                      "total_ncsb") \
        (ls_normal, sc_normal,
         ls_audit, sc_audit,
         lt, voxy, hw, nt, ncsb)

    return final_total


get_final_total = f.udf(get_final_total, LIST_TOTAL)
#--------------------------------
#------------------------
#-----------

TIME_SUCCESS_LSSC = 2100
TIME_SUCCESS_LT = 1500

ROLE_AUDIT = 'AUDIT'

def etl_collect_lssclt(start_date_id_focus, end_date_id_focus, df_student_contact):
    start_year_month_id_focus, end_year_month_id_focus = get_year_month_id_from_date(start_date_id_focus,
                                                                                     end_date_id_focus)
    if is_dev:
        print ('start_date_id_focus: ', start_date_id_focus)
        print ('end_date_id_focus: ', end_date_id_focus)

        print ('start_year_month_id_focus: ', start_year_month_id_focus)
        print ('end_year_month_id_focus: ', end_year_month_id_focus)

    push_down_predicate_v = "((behavior_id == \"" + BEHAVIOR_ID_LS + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_SC + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_LT + "\") " \
                            + " and  year_month_id >= \"" + str(start_year_month_id_focus) + "\" " \
                            + " and  year_month_id <= \"" + str(end_year_month_id_focus) + "\")"

    dyf_sb_student_behavior = connectGlue(database="olap_student_behavior", table_name="sb_student_behavior",
                                          select_fields=["student_behavior_id", "contact_id", "student_behavior_date"],
                                          push_down_predicate=push_down_predicate_v
                                          )

    df_sb_student_behavior = dyf_sb_student_behavior.toDF()
    df_sb_student_behavior = df_sb_student_behavior.drop_duplicates(["student_behavior_id"])
    df_sb_student_behavior = df_sb_student_behavior.select("student_behavior_id", "contact_id",
                                                           f.from_unixtime("student_behavior_date",
                                                                           format="yyyyMMdd").cast("int").alias(
                                                               "date_id"))

    dyf_sb_student_learning = connectGlue(database="olap_student_behavior", table_name="sb_student_learning",
                                          select_fields=["student_behavior_id", "behavior_id", "duration",
                                                         "role_in_class"],
                                          push_down_predicate=push_down_predicate_v
                                          )

    dyf_sb_student_learning = dyf_sb_student_learning.resolveChoice(specs=[("behavior_id", "cast:int")])

    #
    # dyf_sb_student_learning = Filter.apply(frame=dyf_sb_student_learning,
    #                                        f=lambda x: (x["behavior_id"] in [int(BEHAVIOR_ID_LS), int(BEHAVIOR_ID_SC)] and x["duration"] >= 2100)
    #                                                    or (x["behavior_id"] in [int(BEHAVIOR_ID_LT)] and x["duration"] >= 1500)
    #                                                    )

    df_sb_student_learning = dyf_sb_student_learning.toDF()

    if is_dev:
        print ('df_sb_student_behavior')
        df_sb_student_behavior.printSchema()
        df_sb_student_behavior.show(3)

        print ('df_sb_student_learning')
        df_sb_student_learning.printSchema()
        df_sb_student_learning.show(3)

    df_student_behavior_lssclt = df_sb_student_behavior.join(df_sb_student_learning,
                                       on=['student_behavior_id'],
                                       how='inner')

    df_student_behavior_lssclt = df_student_behavior_lssclt.drop("student_behavior_id")

    df_student_behavior_lssclt = df_student_behavior_lssclt\
        .filter((df_student_behavior_lssclt.date_id >= start_date_id_focus)
                 & (df_student_behavior_lssclt.date_id < end_date_id_focus))

    df_student_behavior_lssclt = df_student_behavior_lssclt\
        .select(
            "contact_id",
            "date_id",
            "behavior_id",
            "role_in_class",
            "duration",
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') != ROLE_AUDIT), 1).otherwise(0)
                .alias('ls_normal'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') != ROLE_AUDIT), f.col('duration')).otherwise(0)
                .cast('int').alias('ls_normal_duration'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') != ROLE_AUDIT) & (f.col('duration') >= TIME_SUCCESS_LSSC), 1).otherwise(0)
                .alias('ls_normal_success'),

            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') == ROLE_AUDIT), 1).otherwise(0)
                .alias('ls_audit'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') == ROLE_AUDIT), f.col('duration')).otherwise(0)
                .cast('int').alias('ls_audit_duration'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LS) & (f.col('role_in_class') == ROLE_AUDIT) & (f.col('duration') >= TIME_SUCCESS_LSSC), 1).otherwise(0)
                .alias('ls_audit_success'),


            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') != ROLE_AUDIT), 1).otherwise(0)
                .alias('sc_normal'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') != ROLE_AUDIT),
                   f.col('duration')).otherwise(0)
                .cast('int').alias('sc_normal_duration'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') != ROLE_AUDIT) & (f.col(
                'duration') >= TIME_SUCCESS_LSSC), 1).otherwise(0)
                .alias('sc_normal_success'),

            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') == ROLE_AUDIT), 1).otherwise(0)
                .alias('sc_audit'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') == ROLE_AUDIT),
                   f.col('duration')).otherwise(0)
                .cast('int').alias('sc_audit_duration'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_SC) & (f.col('role_in_class') == ROLE_AUDIT) & (f.col('duration') >= TIME_SUCCESS_LSSC), 1).otherwise(0)
                .alias('sc_audit_success'),


            f.when(f.col('behavior_id') == BEHAVIOR_ID_LT, 1L).otherwise(0L)
                .alias('lt'),
            f.when(f.col('behavior_id') == BEHAVIOR_ID_LT, f.col('duration')).otherwise(0L)
                .alias('lt_duration'),
            f.when((f.col('behavior_id') == BEHAVIOR_ID_LT) & (f.col('duration') >= TIME_SUCCESS_LT), 1).otherwise(0L)
                .alias('lt_success')
        )

    # if is_dev:
    #     print('df_student_behavior_lssclt')
    #     df_student_behavior_lssclt.printSchema()
    #     df_student_behavior_lssclt.show(10)

    df_student_behavior_lssclt_agg = df_student_behavior_lssclt.groupBy('contact_id', 'date_id')\
        .agg(
        f.sum('ls_normal').cast('int').alias('ls_normal'),
        f.sum('ls_normal_duration').cast('int').alias('ls_normal_duration'),
        f.sum('ls_normal_success').cast('int').alias('ls_normal_success'),

        f.sum('ls_audit').cast('int').alias('ls_audit'),
        f.sum('ls_audit_duration').cast('int').alias('ls_audit_duration'),
        f.sum('ls_audit_success').cast('int').alias('ls_audit_success'),


        f.sum('sc_normal').cast('int').alias('sc_normal'),
        f.sum('sc_normal_duration').cast('int').alias('sc_normal_duration'),
        f.sum('sc_normal_success').cast('int').alias('sc_normal_success'),

        f.sum('sc_audit').cast('int').alias('sc_audit'),
        f.sum('sc_audit_duration').cast('int').alias('sc_audit_duration'),
        f.sum('sc_audit_success').cast('int').alias('sc_audit_success'),


        f.sum('lt').cast('int').alias('lt'),
        f.sum('lt_duration').cast('int').alias('lt_duration'),
        f.sum('lt_success').cast('int').alias('lt_success')

    )


    if is_dev:
        print('df_student_behavior_lssclt_agg')
        df_student_behavior_lssclt_agg.printSchema()
        df_student_behavior_lssclt_agg.show(10)
    # "contact_id", "date_id", "behavior_id", "role_in_class", duration

    # join = join.groupby("contact_id", "date_id", "behavior_id", "role_in_class").agg(f.count("duration").alias("total"))
    # join = join.select(
    #     "contact_id", "date_id", f.struct("behavior_id", "total", "role_in_class").alias("type_role_and_total")
    # )
    #
    # # ----get date --focus
    # join = join.filter((join.date_id >= start_date_id_focus)
    #                    & (join.date_id < end_date_id_focus))
    #
    # df_group_by = join.groupBy("contact_id", "date_id") \
    #     .agg(f.collect_list("type_role_and_total").alias("l_type_role_and_total"))
    # join_total = df_group_by.select(
    #     "contact_id", "date_id",
    #     get_final_total("l_type_role_and_total").alias("list_total")
    # )
    #
    #
    #
    # # "total_ls_normal", "total_sc_normal",
    # # 'total_ls_audit', 'total_sc_audit'
    # # "total_lt",
    # # "total_voxy", "total_hw", "total_nt",
    # # "total_ncsb", "total_audit") \
    #
    # df_lss_sc_lt = join_total.select(
    #     "contact_id", "date_id",
    #     f.col("list_total").getItem("total_ls_normal").alias("total_ls_normal"),
    #     f.col("list_total").getItem("total_sc_normal").alias("total_sc_normal"),
    #
    #     f.col("list_total").getItem("total_ls_audit").alias("total_ls_audit"),
    #     f.col("list_total").getItem("total_sc_audit").alias("total_sc_audit"),
    #
    #     f.col("list_total").getItem("total_lt").alias("total_lt")
    # )

    return df_student_behavior_lssclt_agg

UNIT_TYPE_LESSONS = 'lessons'
UNIT_TYPE_WORDS = 'words'
UNIT_TYPE_SECONDS = 'seconds'

def get_ai_learning_number_ait(ai_type, unit_type, count):
    if ai_type == 'ait' and unit_type == UNIT_TYPE_SECONDS:
        return int(round(count * 1.0 / 600))
    return 0


udf_get_ai_learning_number_ait = f.udf(get_ai_learning_number_ait, IntegerType())


def get_ai_learning_number_aip(ai_type, unit_type, count):
    if ai_type == 'aip' and unit_type == UNIT_TYPE_LESSONS:
        return count
    return 0


udf_get_ai_learning_number_aip = f.udf(get_ai_learning_number_aip, IntegerType())


def get_ai_learning_number_micro(ai_type, unit_type, count):
    if ai_type == 'micro' and unit_type == UNIT_TYPE_WORDS:
        return count
    return 0


udf_get_ai_learning_number_micro = f.udf(get_ai_learning_number_micro, IntegerType())

def etl_ai_stater(start_date, end_date, df_student_contact):
    dyf_view_ai_success_by_day = glueContext.create_dynamic_frame \
        .from_catalog(database='moodlestarter',
                      table_name='view_ai_success_by_day')

    if is_dev:
        print('dyf_view_ai_success_by_day')
        dyf_view_ai_success_by_day.printSchema()
        dyf_view_ai_success_by_day.show(30)

    dyf_view_ai_success_by_day = dyf_view_ai_success_by_day.select_fields(
        ['user_id', 'date', 'ait', 'count', 'type']) \
        .rename_field('user_id', 'user_id_ai') \
        .rename_field('date', 'date_learning') \
        .rename_field('ait', 'ai_type') \
        .rename_field('type', 'unit_type')

    dyf_view_ai_success_by_day = dyf_view_ai_success_by_day \
        .resolveChoice(specs=[('user_id_ai', 'cast:long'), ('count', 'cast:int')])

    dyf_view_ai_success_by_day = Filter \
        .apply(frame=dyf_view_ai_success_by_day,
               f=lambda x: x["user_id_ai"] is not None
                           and x["date_learning"] is not None
                           and x["ai_type"] is not None
                           and x["unit_type"] is not None)

    df_view_ai_success_by_day = dyf_view_ai_success_by_day.toDF()
    df_view_ai_success_by_day = df_view_ai_success_by_day \
        .dropDuplicates(['user_id_ai', 'date_learning', 'ai_type'])

    df_view_ai_success_by_day = df_view_ai_success_by_day.withColumn('date_id',
                                                                     f.from_unixtime(
                                                                         timestamp=f.unix_timestamp('date_learning',
                                                                                                    format='yyyy-MM-dd'),
                                                                         format='yyyyMMdd').cast('int'))

    df_view_ai_success_by_day = df_view_ai_success_by_day \
        .filter((df_view_ai_success_by_day.date_id >= start_date)
                & (df_view_ai_success_by_day.date_id < end_date))

    if is_dev:
        print('df_view_ai_success_by_day')
        df_view_ai_success_by_day.printSchema()
        df_view_ai_success_by_day.show(3)

    # root
    # | -- user_id_ai: long(nullable=true)
    # | -- count: string(nullable=true)
    # | -- unit_type: string(nullable=true)
    # | -- date_learning: string(nullable=true)
    # | -- ai_type: string(nullable=true)
    # | -- date_id: long(nullable=true)

    df_view_ai_success_by_day = df_view_ai_success_by_day \
        .select(
        'user_id_ai',
        'date_id',
        'ai_type',
        udf_get_ai_learning_number_ait('ai_type', 'unit_type', 'count').alias('total_starter_ait'),
        udf_get_ai_learning_number_aip('ai_type', 'unit_type', 'count').alias('total_starter_aip'),
        udf_get_ai_learning_number_micro('ai_type', 'unit_type', 'count').alias('total_micro')
    )



    if is_dev:
        print('df_view_ai_success_by_day')
        df_view_ai_success_by_day.printSchema()
        df_view_ai_success_by_day.show(30)

    df_ai_learning_collection = df_view_ai_success_by_day \
        .groupBy('user_id_ai', 'date_id')\
        .agg(f.sum('total_starter_ait').cast('int').alias('total_starter_ait'),
             f.sum('total_starter_aip').cast('int').alias('total_starter_aip'),
             f.sum('total_micro').cast('int').alias('total_micro')
             )

    df_ai_learning_collection = df_ai_learning_collection.withColumnRenamed('user_id_ai', 'student_id')

    df_ai_learning_collection = df_ai_learning_collection\
        .join(df_student_contact,
              on=['student_id'],
              how='inner'
              )

    df_ai_learning_collection = df_ai_learning_collection.drop('student_id')

    if is_dev:
        print('df_ai_learning_collection')
        df_ai_learning_collection.printSchema()
        df_ai_learning_collection.show(30)

    return df_ai_learning_collection

CLASS_TYPE_NATIVE_TALK = 'NATIVE_TALK'
CLASS_TYPE_NCSBASIC = 'NCSBASIC'
CLASS_TYPE_VOXY = 'VOXY'
CLASS_TYPE_HOME_WORK = 'HOME_WORK'

def etl_collet_le2(start_date_id_focus, end_date_id_focus):
    start_year_month_id_focus, end_year_month_id_focus = get_year_month_id_from_date(start_date_id_focus,
                                                                                     end_date_id_focus)

    if is_dev:
        print ('etl_le2::start_date_id_focus: ', start_date_id_focus)
        print ('etl_le2::end_date_id_focus: ', end_date_id_focus)

        print ('etl_le2::start_year_month_id_focus: ', start_year_month_id_focus)
        print ('etl_le2::end_year_month_id_focus: ', end_year_month_id_focus)

    push_down_predicate_v = "((class_type == \"" + CLASS_TYPE_NATIVE_TALK + "\" " \
                            + " or class_type == \"" + CLASS_TYPE_NCSBASIC + "\" " \
                            + " or class_type == \"" + CLASS_TYPE_VOXY + "\" " \
                            + " or class_type == \"" + CLASS_TYPE_HOME_WORK + "\") " \
                            + " and  year_month_id >= \"" + str(start_year_month_id_focus) + "\" " \
                            + " and  year_month_id <= \"" + str(end_year_month_id_focus) + "\")"

    dyf_le2_history = connectGlue(database="olap_student_behavior",
                                          table_name="le2_history",
                                          select_fields=['class_type', 'contact_id', 'learning_date', 'total_learing', 'total_duration'],
                                          push_down_predicate=push_down_predicate_v
                                          )

    if is_dev:
        print('dyf_le2_history')
        dyf_le2_history.printSchema()
        dyf_le2_history.show(3)

    df_le2_history = dyf_le2_history.toDF()

    # "total_voxy", "total_hw", "total_nt", "total_ncsb",

    df_le2_history = df_le2_history\
        .select(
        'contact_id',
        f.from_unixtime(f.unix_timestamp('learning_date', format='yyyy-MM-dd'), format='yyyyMMdd').cast('int').alias('date_id'),

        f.when(f.col('class_type') == CLASS_TYPE_VOXY, f.col('total_duration')).otherwise(f.lit(0)).cast('int')
            .alias('voxy_duration'),
        f.when(f.col('class_type') == CLASS_TYPE_VOXY, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('voxy_success'),

        f.when(f.col('class_type') == CLASS_TYPE_HOME_WORK, f.col('total_duration')).otherwise(f.lit(0)).cast('int')
            .alias('hw_duration'),
        f.when(f.col('class_type') == CLASS_TYPE_HOME_WORK, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('hw_success'),

        f.when(f.col('class_type') == CLASS_TYPE_NATIVE_TALK, f.col('total_duration')).otherwise(f.lit(0)).cast('int')
            .alias('native_talk_duration'),
        f.when(f.col('class_type') == CLASS_TYPE_NATIVE_TALK, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('native_talk_succcess'),

        f.when(f.col('class_type') == CLASS_TYPE_NCSBASIC, f.col('total_duration')).otherwise(f.lit(0)).cast('int')
            .alias('ncsb_duration'),
        f.when(f.col('class_type') == CLASS_TYPE_NCSBASIC, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('ncsb_success')
    )

    df_le2_history = df_le2_history.groupBy('contact_id', 'date_id')\
        .agg(
            f.sum('voxy_duration').cast('int').alias('voxy_duration'),
            f.sum('voxy_success').cast('int').alias('voxy_success'),

            f.sum('hw_duration').cast('int').alias('hw_duration'),
            f.sum('hw_success').cast('int').alias('hw_success'),

            f.sum('native_talk_duration').cast('int').alias('native_talk_duration'),
            f.sum('native_talk_succcess').cast('int').alias('native_talk_succcess'),

            f.sum('ncsb_duration').cast('int').alias('ncsb_duration'),
            f.sum('ncsb_success').cast('int').alias('ncsb_success')
        )

    return df_le2_history

def get_df_behavior(start_date_focus, end_date_focus):
    df_student_contact = get_df_student_contact();

    if is_dev:
        df_student_contact.cache()
        print ('df_student_contact')
        df_student_contact.printSchema()
        df_student_contact.show(3)

    df_collect_lssclt = etl_collect_lssclt(start_date_focus, end_date_focus, df_student_contact)
    if is_dev:
        df_collect_lssclt.cache()
        print('df_collect_lssclt')
        df_collect_lssclt.printSchema()
        df_collect_lssclt.show(3)

    df_collect_ai_stater = etl_ai_stater(start_date_focus, end_date_focus, df_student_contact)

    if is_dev:
        df_collect_ai_stater.cache()
        print('df_collect_ai_stater')
        df_collect_ai_stater.printSchema()
        df_collect_ai_stater.show(3)

    df_le2_history = etl_collet_le2(start_date_focus, end_date_focus)

    if is_dev:
        print('df_le2_history')
        df_le2_history.printSchema()
        df_le2_history.show(3)

    df_le_total = df_collect_lssclt \
        .join(other=df_collect_ai_stater,
              on=['contact_id', 'date_id'],
              how='outer') \
        .join(other=df_le2_history,
              on=['contact_id', 'date_id'],
              how='outer')

    # df_le_total = df_le_total.withColumn("transformed_at", f.lit(d4))
    df_le_total = df_le_total.fillna(0)

    if is_dev:
        print('df_le_total')
        df_le_total.printSchema()
        df_le_total.show(3)

    dyf = DynamicFrame.fromDF(df_le_total, glueContext, "dyf")

    if is_saved_temporary:
        glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf,
                                                       catalog_connection="glue_redshift",
                                                       connection_options={
                                                           "dbtable": "dev.student_behavior_aggregation_day_v02",
                                                           "database": "student_native_report"
                                                       },
                                                       redshift_tmp_dir="s3n://dts-odin/temp/bc200/student_detail/student_behavior_aggregation_day_v02",
                                                       transformation_ctx="datasink4")


    if is_dev:
        df_student_contact.unpersist()
        df_collect_lssclt.unpersist()
        df_collect_ai_stater.unpersist()
        df_le2_history.unpersist()

    return df_le_total
#----------------end learning ------------------------------------------------------------------------------------------#


###-------------------------###-----------------------###------------------------------------------###
def insert_time_dim(start_date_id, end_date_id):
    time_begin = datetime.date(2019, 9, 9)

    # tao timeend bang ngay hien tai + 1 thang - 1 ngay
    # time_end = time_begin + relativedelta(months=1) - datetime.timedelta(days=1)

    time_end = datetime.date(2019, 10, 1)

    # tao dataframe tu time_begin va time_end
    data = [(time_begin, time_end)]
    df = spark.createDataFrame(data, ["minDate", "maxDate"])
    # convert kieu dl va ten field
    df = df.select(df.minDate.cast(DateType()).alias("minDate"), df.maxDate.cast(DateType()).alias("maxDate"))

    # chay vong lap lay tat ca cac ngay giua mindate va maxdate
    df = df.withColumn("daysDiff", f.datediff("maxDate", "minDate")) \
        .withColumn("repeat", f.expr("split(repeat(',', daysDiff), ',')")) \
        .select("*", f.posexplode("repeat").alias("date", "val")) \
        .withColumn("date", f.expr("to_date(date_add(minDate, date))")) \
        .select('date')

    # convert date thanh cac option ngay_thang_nam
    df = df.withColumn('id', f.date_format(df.date, "yyyyMMdd")) \
        .withColumn('ngay_trong_thang', f.dayofmonth(df.date)) \
        .withColumn('ngay_trong_tuan', f.from_unixtime(f.unix_timestamp(df.date, "yyyy-MM-dd"), "EEEEE")) \
        .withColumn('tuan_trong_nam', f.weekofyear(df.date)) \
        .withColumn('thang', f.month(df.date)) \
        .withColumn('quy', f.quarter(df.date)) \
        .withColumn('nam', f.year(df.date))
    df = df.withColumn('tuan_trong_thang', (df.ngay_trong_thang - 1) / 7 + 1)

    data_time = DynamicFrame.fromDF(df, glueContext, 'data_time')

    # convert data
    data_time = data_time.resolveChoice(specs=[('tuan_trong_thang', 'cast:int')])

    # chon cac truong va kieu du lieu day vao db
    applymapping1 = ApplyMapping.apply(frame=data_time,
                                       mappings=[("id", "string", "id", "bigint"),
                                                 ("ngay_trong_thang", 'int', 'ngay_trong_thang', 'int'),
                                                 ("ngay_trong_tuan", "string", "ngay_trong_tuan", "string"),
                                                 ("tuan_trong_thang", "int", "tuan_trong_thang", "int"),
                                                 ("tuan_trong_nam", "int", "tuan_trong_nam", "int"),
                                                 ("thang", "int", "thang", "int"),
                                                 ("quy", "int", "quy", "int"),
                                                 ("nam", "int", "nam", "int"),
                                                 ("date", "date", "ngay", "timestamp")])

    resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols", transformation_ctx="resolvechoice2")
    dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

    # ghi dl vao db
    preactions = 'delete student.time_dim where id >= ' + str(start_date_id)
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": preactions,
                                                                   "dbtable": "student.time_dim",
                                                                   "database": "student_native_report"},
                                                               redshift_tmp_dir="s3n://dts-odin/temp/tu-student_native_report/student/time_dim",
                                                               transformation_ctx="datasink4")


def main():
    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    date_id = long(today.strftime("%Y%m%d"))
    print('today_id: ', date_id)

    #start_date = today - timedelta(15)
    # start_date_id = long(start_date.strftime("%Y%m%d"))
    start_date_id = 20190901
    end_date_id = date_id
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
    df_student_package_status_by_date = get_student_package_adivsor_level(start_date_id, end_date_id)
    df_student_package_status_by_date.cache()

    df_behavior = get_df_behavior(start_date_id, end_date_id)

    df_student_fact = df_student_package_status_by_date\
        .join(df_behavior,
              on=['contact_id', 'date_id'],
              how='left')

    df_student_fact = df_student_fact.na.fill(0)

    dyf_student_fact = DynamicFrame.fromDF(df_student_fact, glueContext, "dyf_student_fact")

    preactions = "delete student.student_fact_v1_0 where date_id >= " + str(start_date_id)
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_fact,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "preactions": preactions,
                                                       "dbtable": "student.student_fact_v1_0",
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/student/student_fact_v1_0",
                                                   transformation_ctx="datasink4")


if __name__ == "__main__":
    main()
