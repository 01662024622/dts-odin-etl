import time
from datetime import datetime, timedelta

import pyspark.sql.functions as f
import pytz
from pyspark.context import SparkContext
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField, StringType
from pyspark.sql.types import Row

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = False
is_limit = False

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/"

FLAG_BC200_FILE = 's3://toxd-olap/olap/flag/flag_bc200.parquet'

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
current_week = datetime.now(ho_chi_minh_timezone)
print('current_week: ', current_week)
current_week_id = long(current_week.strftime("%Y%W"))



#------------------------------------------------------------common function ------------------------------------------#

def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame

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

    # if is_dev:
    #     print("full schema of table: ", database, " and table: ", table_name, " after select")
    #     dyf.printSchema()
    return dyf

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

#-----------------------------------------------------------------------------------------------------------------------#
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
    # display(data_frame, table_name)
    return data_frame


# -----------------------------------------------------------------------------------------------------------------------#
DATE_FAKE_ID = 99999999999L

PACKAGE_STATUS_EXPIRE = 'EXPIRED'
MAX_ADVISING_EXPIRE = 30


def get_list_date_by_start_and_end(start_time, end_time, package_status):
    if start_time is None or end_time is None:
        return [DATE_FAKE_ID]
    date_ids = []

    start_time_t = datetime.fromtimestamp(float(start_time), ho_chi_minh_timezone)
    # a_t_week_id = long(a_t.strftime("%Y%W"))

    if end_time == 99999999999L:
        end_time_t = current_week
    else:
        end_time_t = datetime.fromtimestamp(end_time, ho_chi_minh_timezone)
    end_time_t_id = long(end_time_t.strftime("%Y%m%d"))

    date_item = start_time_t

    order = 0
    while long(date_item.strftime("%Y%m%d")) < end_time_t_id:
        if package_status == PACKAGE_STATUS_EXPIRE and order > MAX_ADVISING_EXPIRE:
            break
        date_ids.append(long(date_item.strftime("%Y%m%d")))
        date_item += timedelta(1)
        order += 1

    if len(date_ids) == 0:
        date_ids = [DATE_FAKE_ID]

    return date_ids


udf_get_list_date_by_start_and_end = f.udf(get_list_date_by_start_and_end, ArrayType(LongType()))

HOUR_DAILY_CUT_OFF = 5 # equal 15h local

def get_timestamp_from_date(date_id):
    date_time = datetime.strptime(str(date_id), "%Y%m%d")
    date_time = date_time.replace(hour=HOUR_DAILY_CUT_OFF, minute=0, second=0, microsecond=0, tzinfo=ho_chi_minh_timezone)
    time_stamp = long(time.mktime(date_time.timetuple()))
    return time_stamp

udf_get_timestamp_from_date = f.udf(get_timestamp_from_date, LongType())

#-------------------

def find(lists, key):
    for items in lists:
        if key.startswith(items[0]):
            return items[1]
    return 0L

def get_package_id(package_code):
    return find(PACKAGE_DATA, package_code)
get_package_id = f.udf(get_package_id, LongType())

def get_package_status_id(package_status_code):
    return find(PACKAGE_STATUS_DATA, package_status_code.upper())
get_package_status_id = f.udf(get_package_status_id, LongType())

def get_student_level_id(student_level):
    return find(MAPPING_LEVEL_TOA_LEVEL, '$' + student_level.lower() + '$')
get_student_level_id = f.udf(get_student_level_id, LongType())

def get_df_student_package_by_date(df_student_package, focus_start_date_id, focus_end_date_id):
    'contact_id', 'student_id', 'package_code', 'package_status_code', 'package_start_time', 'package_end_time'

    df_student_package_date_ids = df_student_package.select(
        'contact_id', 'student_id', 'package_code', 'package_status_code',
        udf_get_list_date_by_start_and_end('package_start_time', 'package_end_time', 'package_status_code').alias('date_ids')
    )

    df_student_package_date_ids = df_student_package_date_ids.select(
        'contact_id', 'student_id',
        get_package_id('package_code').alias('package_id'),
        get_package_status_id('package_status_code').alias('package_status_id'),
        f.explode('date_ids').alias('date_id')
    )

    print('df_student_package_date_ids')
    df_student_package_date_ids.printSchema()
    df_student_package_date_ids.show(2)

    df_student_package_date_ids = df_student_package_date_ids \
        .filter((df_student_package_date_ids.date_id >= focus_start_date_id)
                & (df_student_package_date_ids.date_id < focus_end_date_id)
                & (df_student_package_date_ids.date_id.isNotNull())
                & (df_student_package_date_ids.date_id != DATE_FAKE_ID)
                )

    df_student_package_date_ids = df_student_package_date_ids \
        .filter(((df_student_package_date_ids.package_id == 1L)
                 | (df_student_package_date_ids.package_id == 2L)
                 | (df_student_package_date_ids.package_id == 3L))
                & ((df_student_package_date_ids.package_status_id == PACKAGE_STATUS_DATA_ACTIVED)
                   | (df_student_package_date_ids.package_status_id == PACKAGE_STATUS_DATA_SUSPENDED)
                   | (df_student_package_date_ids.package_status_id == PACKAGE_STATUS_DATA_DEACTIVED)
                   | (df_student_package_date_ids.package_status_id == PACKAGE_STATUS_DATA_EXPIRED))
                )

    df_student_package_date_ids = df_student_package_date_ids.select(
        'contact_id', 'student_id',
        'package_id',
        'package_status_id',
        'date_id',
        udf_get_timestamp_from_date('date_id').alias('date_cut_off')

    )



    return df_student_package_date_ids


BEHAVIOR_ID_LS = "11"
BEHAVIOR_LO_1 = "starter_ait"
BEHAVIOR_LO_2 = "starter_aip"
BEHAVIOR_LO_3 = "starter_micro"
BEHAVIOR_ID_SC = "12"
BEHAVIOR_ID_LT = "13"
BEHAVIOR_ID_VOXY = "14"
BEHAVIOR_ID_HW = "15"
BEHAVIOR_ID_NT = "16"
BEHAVIOR_ID_NCSB = "17"

#---------------------------------------------------------------------
LIST_TOTAL = StructType([
    StructField("total_ls", IntegerType(), False),
    StructField("total_sc", IntegerType(), False),
    StructField("total_lt", IntegerType(), False),
    StructField("total_voxy", IntegerType(), False),
    StructField("total_hw", IntegerType(), False),
    StructField("total_nt", IntegerType(), False),
    StructField("total_ncsb", IntegerType(), False),
    StructField("total_audit", IntegerType(), False)
])

def get_final_total(structs):
    ls = sc = lt = voxy = hw = nt = ncsb = audit = 0
    if structs is not None:
        length_l_p = len(structs)
        for i in range(0, length_l_p):
            ele = structs[i]
            if ele[0] == 11:
                if ele[2] == "AUDIT":
                    audit += ele[1]
                else:
                    ls = ele[1]
            elif ele[0] == 12:
                if ele[2] == "AUDIT":
                    audit += ele[1]
                else:
                    sc = ele[1]
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

        final_total = Row("total_ls", "total_sc", "total_lt",
                          "total_voxy", "total_hw", "total_nt",
                          "total_ncsb", "total_audit")(ls, sc, lt, voxy, hw, nt, ncsb, audit)

    else:
        final_total = Row("total_ls", "total_sc", "total_lt",
                          "total_voxy", "total_hw", "total_nt",
                          "total_ncsb", "total_audit")(ls, sc, lt, voxy, hw, nt, ncsb, audit)
    return final_total


get_final_total = f.udf(get_final_total, LIST_TOTAL)

LIST_TOTAL_LO = StructType([
    StructField("total_starter_ait", IntegerType(), False),
    StructField("total_starter_aip", IntegerType(), False),
    StructField("total_micro", IntegerType(), False)
])

def get_final_total_lo(structs):
    ait = aip = micro = 0
    if structs is not None:
        length_l_p = len(structs)
        for i in range(0, length_l_p):
            ele = structs[i]
            if ele[0] == "starter_ait":
                ait = ele[1]
            elif ele[0] == "starter_aip":
                aip = ele[1]
            elif ele[0] == "starter_micro":
                micro = ele[1]
        return Row("total_starter_ait", "total_starter_aip", "total_micro")(ait, aip, micro)
    else:
        return Row("total_starter_ait", "total_starter_aip", "total_micro")(ait, aip, micro)


get_final_total_lo = f.udf(get_final_total_lo, LIST_TOTAL_LO)


def check_value(val_1, val_2):
    if val_1 is not None:
        return val_1
    return val_2


check_value = f.udf(check_value, StringType())


def check_date(val_1, val_2):
    if val_1 > 201701:
        return long(val_1)
    return long(val_2)


check_date = f.udf(check_date, LongType())

def get_lo(start_date, end_date):
    push_down_predicate_lo = "(partition_0 == \"" + BEHAVIOR_LO_1 + "\" " \
                             + " or partition_0 == \"" + BEHAVIOR_LO_2 + "\" " \
                             + " or partition_0 == \"" + BEHAVIOR_LO_3 + "\") "
    dyf_nvn_knowledge = connectGlue(database="nvn_knowledge", table_name="mapping_lo_student_history",
                                    select_fields=["student_id", "learning_object_id", "created_date_id",
                                                   "source_system"],
                                    push_down_predicate=push_down_predicate_lo
                                    )

    dyf_nvn_knowledge = Filter.apply(frame=dyf_nvn_knowledge,
                                     f=lambda x: start_date <= x["created_date_id"] < end_date)

    dyf_nvn_knowledge = dyf_nvn_knowledge.resolveChoice(specs=[("created_date_id", "cast:long")])
    df_nvn_knowledge = dyf_nvn_knowledge.toDF()

    df_nvn_knowledge = df_nvn_knowledge.groupBy("student_id", "created_date_id", "source_system") \
        .agg(f.count("learning_object_id").alias("total"))
    df_nvn_knowledge = df_nvn_knowledge.select(
        "student_id", "created_date_id", f.struct("source_system", "total").alias("type_and_total")
    )
    df_group_by = df_nvn_knowledge.groupBy("student_id", "created_date_id") \
        .agg(f.collect_list("type_and_total").alias("l_type_and_total"))
    join_total = df_group_by.select(
        "student_id", "created_date_id",
        get_final_total_lo("l_type_and_total").alias("list_total")
    )

    df_latest = join_total.select(
        "student_id", "created_date_id",
        f.col("list_total").getItem("total_starter_ait").alias("total_starter_ait"),
        f.col("list_total").getItem("total_starter_aip").alias("total_starter_aip"),
        f.col("list_total").getItem("total_micro").alias("total_micro")
    )

    dyf_student_contact = connectGlue(database="tig_advisor", table_name="student_contact",
                                      select_fields=["contact_id", "student_id"],
                                      fillter=["contact_id", "student_id"],
                                      duplicates=["contact_id", "student_id"]
                                      ).rename_field("student_id", "student_id_contact") \
        .rename_field("contact_id", "contact_id_lo")
    df_student_contact = dyf_student_contact.toDF()
    df_latest = df_latest.join(df_student_contact, df_student_contact["student_id_contact"] == df_latest["student_id"])
    df_latest = df_latest.drop("student_id_contact", "student_id")
    return df_latest

def get_learnig_info(start_year_month_id, end_year_month_id, start_date, end_date):
    push_down_predicate_v = "((behavior_id == \"" + BEHAVIOR_ID_LS + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_SC + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_LT + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_VOXY + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_HW + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_NCSB + "\" " \
                            + " or behavior_id == \"" + BEHAVIOR_ID_NT + "\") " \
                            + " and  year_month_id >= \"" + str(start_year_month_id) + "\" " \
                            + " and  year_month_id <= \"" + str(end_year_month_id) + "\")"
    dyf_sb_student_behavior = connectGlue(database="olap_student_behavior", table_name="sb_student_behavior",
                                          select_fields=["student_behavior_id", "contact_id", "student_behavior_date"],
                                          push_down_predicate=push_down_predicate_v
                                          )

    df_sb_student_behavior = dyf_sb_student_behavior.toDF()
    df_sb_student_behavior = df_sb_student_behavior.drop_duplicates(["student_behavior_id"])
    df_sb_student_behavior = df_sb_student_behavior.select("student_behavior_id", "contact_id",
                                                           f.from_unixtime("student_behavior_date",
                                                                           format="yyyyMMdd").cast("long").alias(
                                                               "date_id"))

    dyf_sb_student_learning = connectGlue(database="olap_student_behavior", table_name="sb_student_learning",
                                          select_fields=["student_behavior_id", "behavior_id", "duration",
                                                         "role_in_class"],
                                          push_down_predicate=push_down_predicate_v
                                          ).rename_field("student_behavior_id", "student_behavior_id_learning")

    dyf_sb_student_learning = dyf_sb_student_learning.resolveChoice(specs=[("behavior_id", "cast:int")])

    dyf_sb_student_learning = Filter.apply(frame=dyf_sb_student_learning,
                                           f=lambda x: (x["behavior_id"] > 12 and x["duration"] > 59)
                                                       or (x["behavior_id"] < 13 and x["duration"] >= 2100))

    df_sb_student_learning = dyf_sb_student_learning.toDF()

    join = df_sb_student_behavior.join(df_sb_student_learning,
                                       df_sb_student_behavior["student_behavior_id"] == df_sb_student_learning[
                                           "student_behavior_id_learning"])

    join = join.drop("student_behavior_id", "student_behavior_id_learning")

    join = join.groupby("contact_id", "date_id", "behavior_id", "role_in_class").agg(f.count("duration").alias("total"))
    join = join.select(
        "contact_id", "date_id", f.struct("behavior_id", "total", "role_in_class").alias("type_role_and_total")
    )

    df_group_by = join.groupBy("contact_id", "date_id") \
        .agg(f.collect_list("type_role_and_total").alias("l_type_role_and_total"))
    join_total = df_group_by.select(
        "contact_id", "date_id",
        get_final_total("l_type_role_and_total").alias("list_total")
    )

    df_latest = join_total.select(
        "contact_id", "date_id",
        f.col("list_total").getItem("total_ls").alias("total_ls"),
        f.col("list_total").getItem("total_sc").alias("total_sc"),
        f.col("list_total").getItem("total_voxy").alias("total_voxy"),
        f.col("list_total").getItem("total_hw").alias("total_hw"),
        f.col("list_total").getItem("total_nt").alias("total_nt"),
        f.col("list_total").getItem("total_ncsb").alias("total_ncsb"),
        f.col("list_total").getItem("total_audit").alias("total_audit"),
        f.col("list_total").getItem("total_lt").alias("total_lt")
    )

    df_lo = get_lo(start_date, end_date)
    df_latest = df_latest.join(df_lo, (df_lo["contact_id_lo"] == df_latest["contact_id"])
                               & (df_lo["created_date_id"] == df_latest["date_id"]), "outer")

    df_latest = df_latest.fillna(0)

    df_latest = df_latest.select("total_lt", "total_voxy", "total_hw", "total_nt", "total_ncsb", "total_audit",
                                 "total_ls", "total_sc", "total_starter_ait", "total_starter_aip", "total_micro",
                                 check_value(df_latest.contact_id, df_latest.contact_id_lo).alias("contact_id"),
                                 check_date(df_latest.created_date_id, df_latest.date_id).alias("date_id"))

    return df_latest



def get_df_student_package():
    df_student_package = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_package',
        ['contact_id', 'student_id', 'package_code', 'package_status_code', 'package_start_time', 'package_end_time']
    )


def main():

    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)
    #
    focus_start_date_id = 20200101
    focus_end_date_id = 20200206

    start_year_month_id = 202001
    end_year_month_id = 202002
    #
    print('start_year_month_id: ', start_year_month_id)
    print('end_year_month_id: ', end_year_month_id)

    print('start_year_week_id: ', start_year_month_id)
    print('end_year_week_id: ', end_year_month_id)

    # ------------------------------------------------------------------------------------------------------------------#
    df_student_package = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_package',
        ['contact_id', 'student_id', 'package_code', 'package_status_code', 'package_start_time', 'package_end_time']
    )

    if df_student_package.count() < 1:
        return

    #----------------------------------------------------------------

    df_student_level = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_level',
        ['contact_id', 'level_code', 'start_date', 'end_date']
    )

    df_student_level = df_student_level.select(
        df_student_level.contact_id.alias('contact_id_level'),
        get_student_level_id('level_code').alias('student_level_id'),
        df_student_level.start_date.alias('start_date_level'),
        df_student_level.end_date.alias('end_date_level')

    )

    if df_student_level.count() < 1:
        return
    # ----------------------------------------------------------------

    df_student_advisor = retrieve_data_frame_from_redshift(
        glueContext,
        'transaction_log',
        'ad_student_advisor',
        ['contact_id', 'advisor_id', 'start_date', 'end_date']
    )

    df_student_advisor = df_student_advisor.select(
        df_student_advisor.contact_id.alias('contact_id_advisor'),
        'advisor_id',
        df_student_advisor.start_date.alias('start_date_advisor'),
        df_student_advisor.end_date.alias('end_date_advisor')

    )

    # ----------------------------------------------------------------

    print('df_student_package')
    df_student_package.printSchema()

    print('df_student_level')
    df_student_level.printSchema()

    print('df_student_advisor')
    df_student_advisor.printSchema()

    # -----------------------------------------------------------------------------------------------------------------#

    df_student_package_by_date = get_df_student_package_by_date(df_student_package, focus_start_date_id, focus_end_date_id)
    print('df_student_package_by_date')
    df_student_package_by_date.printSchema()
    df_student_package_by_date.show(3)

    # root
    # | -- contact_id: string(nullable=true)
    # | -- student_id: long(nullable=true)
    # | -- package_id: long(nullable=true)
    # | -- package_status_id: long(nullable=true)
    # | -- date_id: long(nullable=true)
    # | -- date_cut_off: long(nullable=true)

    # test something

    dyf_student_package_by_date = DynamicFrame\
        .fromDF(df_student_package_by_date, glueContext, 'dyf_student_package_by_date')

    atasink4 = glueContext.write_dynamic_frame\
        .from_jdbc_conf(frame=dyf_student_package_by_date,
                      catalog_connection="glue_redshift",
                      connection_options={
                          "dbtable": "student.dyf_student_package_by_date",
                          "database": "student_native_report"
                      },
                      redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student/dyf_student_package_by_date",
                      transformation_ctx="datasink4")

    df_student_package_level = df_student_package_by_date \
        .join(df_student_level,
              on=(df_student_package_by_date.contact_id == df_student_level.contact_id_level)
                 & (df_student_package_by_date['date_cut_off'] >= df_student_level['start_date_level'])
                 & (df_student_package_by_date['date_cut_off'] < df_student_level['end_date_level']),
              how='left')

    dyf_student_package_level = DynamicFrame \
        .fromDF(df_student_package_level, glueContext, 'dyf_student_package_level')

    atasink4 = glueContext.write_dynamic_frame \
        .from_jdbc_conf(frame=dyf_student_package_level,
                        catalog_connection="glue_redshift",
                        connection_options={
                            "dbtable": "student.dyf_student_package_level",
                            "database": "student_native_report"
                        },
                        redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student/dyf_student_package_level",
                        transformation_ctx="datasink4")


    df_student_package_level_advisor = df_student_package_level.join(df_student_advisor,
              on=(df_student_package_by_date.contact_id == df_student_advisor.contact_id_advisor)
                 & (df_student_package_by_date['date_cut_off'] >= df_student_advisor['start_date_advisor'])
                 & (df_student_package_by_date['date_cut_off'] < df_student_advisor['end_date_advisor']),
              how='left')

    df_student_package_level_advisor = df_student_package_level_advisor\
        .select(
            'contact_id',
            'student_id',
            'package_id',
            'package_status_id',
            'student_level_id',
            'advisor_id',
            'date_id'
    )

    dyf_student_package_level_advisor = DynamicFrame \
        .fromDF(df_student_package_level_advisor, glueContext, 'dyf_student_package_level_advisor')

    atasink4 = glueContext.write_dynamic_frame \
        .from_jdbc_conf(frame=dyf_student_package_level_advisor,
                        catalog_connection="glue_redshift",
                        connection_options={
                            "dbtable": "student.dyf_student_package_level_advisor",
                            "database": "student_native_report"
                        },
                        redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student/dyf_student_package_level_advisor",
                        transformation_ctx="datasink4")

    return

    df_student_package_level_advisor_by_date.cache()

    print('df_student_package_level_advisor_by_date')
    df_student_package_level_advisor_by_date.printSchema()
    df_student_package_level_advisor_by_date.show(3)


    df_learning_info = get_learnig_info(start_year_month_id, end_year_month_id, focus_start_date_id, focus_end_date_id)
    df_learning_info = df_learning_info.cache()
    print('df_learning_info')
    df_learning_info.printSchema()
    df_learning_info.show(3)

    # | -- total_lt: integer(nullable=true)
    # | -- total_voxy: integer(nullable=true)
    # | -- total_hw: integer(nullable=true)
    # | -- total_nt: integer(nullable=true)
    # | -- total_ncsb: integer(nullable=true)
    # | -- total_audit: integer(nullable=true)
    # | -- total_ls: integer(nullable=true)
    # | -- total_sc: integer(nullable=true)
    # | -- total_starter_ait: integer(nullable=true)
    # | -- total_starter_aip: integer(nullable=true)
    # | -- total_micro: integer(nullable=true)
    # | -- contact_id: string(nullable=true)
    # | -- date_id: integer(nullable=true)

    # df_student_package_level_advisor_by_date_learning = df_student_package_level_advisor_by_date\
    #     .join(df_learning_info,
    #           on=['contact_id', 'date_id'],
    #           how='left'
    #           )
    #
    # print('df_student_package_level_advisor_by_date_learning')
    # df_student_package_level_advisor_by_date_learning.printSchema()
    # df_student_package_level_advisor_by_date_learning.show(3)
    #
    # dyf_student_package_level_advisor_by_date_learning = DynamicFrame\
    #     .fromDF(df_student_package_level_advisor_by_date_learning, glueContext, 'dyf_student_package_level_advisor_by_date_learning')
    #
    # atasink4 = glueContext.write_dynamic_frame\
    #     .from_jdbc_conf(frame=dyf_student_package_level_advisor_by_date_learning,
    #                   catalog_connection="glue_redshift",
    #                   connection_options={
    #                       "dbtable": "student.student_fact_v01",
    #                       "database": "student_native_report"
    #                   },
    #                   redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student/student_fact_v01",
    #                   transformation_ctx="datasink4")
    #
    df_student_package_level_advisor_by_date.unpersist()
    df_learning_info.unpersist()


if __name__ == "__main__":
    main()
