import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

import pyspark.sql.functions as f
from datetime import datetime, date, timedelta
import pytz
from pyspark import SparkContext, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType, DateType


# sparkcontext

sc = SparkContext()
# glue with sparkcontext env
glueContext = GlueContext(sc)
# create new spark session working
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

is_dev = False
is_limit = False
is_checking = True
checking_contact_id = '20190810221735'
checking_date_id = 20200320
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")

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


#------------------common ---------------------------------------------------------------------------------------------#
def get_year_month_id_from_date(start_date_id, end_date_id):
    start_date_time_timestamp = datetime.strptime(str(start_date_id), "%Y%m%d")
    end_date_time_timestamp = datetime.strptime(str(end_date_id), "%Y%m%d")

    start_year_month_id = long(start_date_time_timestamp.strftime("%Y%m"))
    end_year_month_id = long(end_date_time_timestamp.strftime("%Y%m"))

    return start_year_month_id, end_year_month_id

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
LIST_TOTAL_LO = StructType([
    StructField("total_starter_ait", IntegerType(), False),
    StructField("total_starter_aip", IntegerType(), False),
    StructField("total_micro", IntegerType(), False)
])


def check_value(val_1, val_2):
    if val_1 is not None:
        return val_1
    return val_2


check_value = f.udf(check_value, StringType())


def subtraction(val_1, val_2):
    return (val_1 - val_2)


subtraction = f.udf(subtraction, IntegerType())


def check_date(val_1, val_2):
    if val_1 > 201701:
        return val_1
    return val_2


check_date = f.udf(check_date, IntegerType())


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

# df_latest = df_latest.select("total_lt", "total_voxy", "total_hw", "total_nt", "total_ncsb",
#                              "total_ls_normal", "total_sc_normal",
#                              'total_ls_audit', 'total_sc_audit',
#
#                              "total_starter_ait", "total_starter_aip", "total_micro",
#                              check_value(df_latest.contact_id, df_latest.contact_id_lo).alias("contact_id"),
#                              check_date(df_latest.created_date_id, df_latest.date_id).alias("year_month_id"))
#
# prinDev(df_latest, "after add transformed")
# df_latest = df_latest.withColumn("transformed_at", f.lit(d4))

MAPPING = [
    ("contact_id", "string", "contact_id", "string"),
    ("year_month_id", "int", "year_month_day", "string"),

    ("total_ls_normal", "int", "total_ls_normal", "int"),
    ("total_sc_normal", "int", "total_sc_normal", "int"),

    ("total_ls_audit", "int", "total_ls_audit", "int"),
    ("total_sc_audit", "int", "total_sc_audit", "int"),

    ("total_lt", "int", "total_lt", "int"),
    ("total_voxy", "int", "total_voxy", "int"),
    ("total_hw", "int", "total_hw", "int"),
    ("total_nt", "int", "total_nt", "int"),
    ("total_ncsb", "int", "total_ncsb", "int"),
    ("total_micro", "int", "total_micro", "int"),
    ("total_starter_aip", "int", "total_starter_aip", "int"),
    ("total_starter_ait", "int", "total_starter_ait", "int"),

    ("transformed_at", "string", "transformed_at", "long")]

VALUES = {'total_ls_normal': 0,
          'total_ls_normal': 0,

          'total_ls_audit': 0,
          'total_sc_audit': 0,

          # 'total_audit': 0,
          'total_lt': 0,
          'total_voxy': 0,
          'total_hw': 0,
          'total_nt': 0,
          'total_ncsb': 0,
          'total_micro': 0,
          'total_starter_aip': 0,
          'total_starter_ait': 0}

REDSHIFT_USERNAME = "dtsodin"
REDSHIFT_PASSWORD = "DWHDtsodin@123"


# ------------------------------------------------------Function for Tranform-------------------------------------------#

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


# ----------------------------------------------------------------------------------------------------------------------#

def checkDumplicate(df, duplicates):
    if len(duplicates) == 0:
        return df
    else:
        return df.dropDuplicates(duplicates)

# ------------------------ount() > 0---------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame


# -----------------------------------------------------------------------------------------------------------------------#

def mappingForAll(dynamicFrame, mapping):
    applymapping2 = ApplyMapping.apply(frame=dynamicFrame,
                                       mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_communication_after_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_communication_after_mapping


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

    # push_down_predicate_v = "((class_type == \"" + CLASS_TYPE_NATIVE_TALK + "\" " \
    #                         + " or class_type == \"" + CLASS_TYPE_NCSBASIC + "\" " \
    #                         + " or class_type == \"" + CLASS_TYPE_VOXY + "\" " \
    #                         + " or class_type == \"" + CLASS_TYPE_HOME_WORK + "\") " \
    #                         + " and  year_month_id >= \"" + str(start_year_month_id_focus) + "\" " \
    #                         + " and  year_month_id <= \"" + str(end_year_month_id_focus) + "\")"

    dyf_le2_history = connectGlue(database="olap_student_behavior",
                                          table_name="le2_history",
                                          select_fields=['class_type', 'contact_id', 'learning_date', 'total_learing']
                                  # ,
                                  #         push_down_predicate=push_down_predicate_v
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
        f.when(df_le2_history.class_type == CLASS_TYPE_VOXY, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('total_voxy'),
        f.when(df_le2_history.class_type == CLASS_TYPE_HOME_WORK, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('total_hw'),
        f.when(df_le2_history.class_type == CLASS_TYPE_NATIVE_TALK, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('total_nt'),
        f.when(df_le2_history.class_type == CLASS_TYPE_NCSBASIC, f.col('total_learing')).otherwise(f.lit(0)).cast('int')
            .alias('total_ncsb')
    )

    df_le2_history = df_le2_history.groupBy('contact_id', 'date_id')\
        .agg(
        f.sum('total_voxy').cast('int').alias('total_voxy'),
        f.sum('total_hw').cast('int').alias('total_hw'),
        f.sum('total_nt').cast('int').alias('total_nt'),
        f.sum('total_ncsb').cast('int').alias('total_ncsb')
    )

    return df_le2_history


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

    # dyf_sb_student_learning = Filter.apply(frame=dyf_sb_student_learning,
    #                                        f=lambda x: (x["behavior_id"] > 12 and x["duration"] > 59)
    #                                                    or (x["behavior_id"] < 13 and x["duration"] >= 2100))

    dyf_sb_student_learning = Filter.apply(frame=dyf_sb_student_learning,
                                           f=lambda x: (x["behavior_id"] in [int(BEHAVIOR_ID_LS), int(BEHAVIOR_ID_SC)] and x["duration"] >= 2100)
                                                       or (x["behavior_id"] in [int(BEHAVIOR_ID_LT)] and x["duration"] >= 1500)
                                                       )

    df_sb_student_learning = dyf_sb_student_learning.toDF()

    if is_dev:
        print ('df_sb_student_behavior')
        df_sb_student_behavior.printSchema()
        df_sb_student_behavior.show(3)

        print ('df_sb_student_learning')
        df_sb_student_learning.printSchema()
        df_sb_student_learning.show(3)

    join = df_sb_student_behavior.join(df_sb_student_learning,
                                       on=['student_behavior_id'],
                                       how='inner')

    join = join.drop("student_behavior_id")

    join = join.groupby("contact_id", "date_id", "behavior_id", "role_in_class").agg(f.count("duration").alias("total"))
    join = join.select(
        "contact_id", "date_id", f.struct("behavior_id", "total", "role_in_class").alias("type_role_and_total")
    )

    # ----get date --focus
    join = join.filter((join.date_id >= start_date_id_focus)
                       & (join.date_id < end_date_id_focus))

    df_group_by = join.groupBy("contact_id", "date_id") \
        .agg(f.collect_list("type_role_and_total").alias("l_type_role_and_total"))
    join_total = df_group_by.select(
        "contact_id", "date_id",
        get_final_total("l_type_role_and_total").alias("list_total")
    )



    # "total_ls_normal", "total_sc_normal",
    # 'total_ls_audit', 'total_sc_audit'
    # "total_lt",
    # "total_voxy", "total_hw", "total_nt",
    # "total_ncsb", "total_audit") \

    df_lss_sc_lt = join_total.select(
        "contact_id", "date_id",
        f.col("list_total").getItem("total_ls_normal").alias("total_ls_normal"),
        f.col("list_total").getItem("total_sc_normal").alias("total_sc_normal"),

        f.col("list_total").getItem("total_ls_audit").alias("total_ls_audit"),
        f.col("list_total").getItem("total_sc_audit").alias("total_sc_audit"),

        f.col("list_total").getItem("total_lt").alias("total_lt")
    )

    return df_lss_sc_lt


def get_df_student_contact():
    dyf_student_contact = connectGlue(database="tig_advisor", table_name="student_contact",
                                          select_fields=["contact_id", "student_id"],
                                          fillter=["contact_id", "student_id"],
                                          duplicates=["student_id"]
                                          )

    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('student_id', 'cast:long')])

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])

    return df_student_contact



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


def get_ai_learning_number(unit_type, count):
    if unit_type == UNIT_TYPE_SECONDS:
        return int(round(count * 1.0 / 600))
    if unit_type == UNIT_TYPE_LESSONS:
        return count
    if unit_type == UNIT_TYPE_WORDS:
        return count


udf_get_ai_learning_number = f.udf(get_ai_learning_number, IntegerType())


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
        .agg(f.sum('total_starter_ait').alias('total_starter_ait'),
             f.sum('total_starter_aip').alias('total_starter_aip'),
             f.sum('total_micro').alias('total_micro')
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


def prinDev(df, df_name="print full information"):
    if is_dev:
        print df_name
        df.printSchema()
        # df.show(3)

###------------------------------------####----------------------------------------------##-----------------------------

###------------------------------------####----------------------------------------------##-----------------------------

def main():
    # ----------test------------------

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_id = long(today.strftime("%Y%m%d"))
    print('today_id: ', today_id)

    start_date = today - timedelta(5)
    start_date_focus = long(start_date.strftime("%Y%m%d"))
    end_date_focus = today_id

    df_student_contact = get_df_student_contact();
    df_student_contact.cache()
    if is_dev:
        print ('df_student_contact')
        df_student_contact.printSchema()
        df_student_contact.show(3)

    if is_checking:
        df_student_contact_test = df_student_contact.filter(f.col('contact_id') == checking_contact_id)
        print('is_checking::df_student_contact_test')
        df_student_contact_test.show(30)

    df_collect_lssclt = etl_collect_lssclt(start_date_focus, end_date_focus, df_student_contact)
    if is_dev:
        df_collect_lssclt.cache()
        print('df_collect_lssclt')
        df_collect_lssclt.printSchema()
        df_collect_lssclt.show(3)

    if is_checking:
        df_collect_lssclt_test = df_collect_lssclt.filter((f.col('contact_id') == checking_contact_id)
                                                          & (f.col('date_id') == checking_date_id))
        print('is_checking::df_collect_lssclt_test')
        df_collect_lssclt_test.show(30)


    df_collect_ai_stater = etl_ai_stater(start_date_focus, end_date_focus, df_student_contact)

    if is_dev:
        df_collect_ai_stater.cache()
        print('df_collect_ai_stater')
        df_collect_ai_stater.printSchema()
        df_collect_ai_stater.show(3)

    if is_checking:
        df_collect_ai_stater_test = df_collect_ai_stater.filter((f.col('contact_id') == checking_contact_id)
                                                          & (f.col('date_id') == checking_date_id))
        print('is_checking::df_collect_ai_stater_test')
        df_collect_ai_stater_test.show(30)



    df_le2_history = etl_collet_le2(start_date_focus, end_date_focus)


    if is_checking:
        df_le2_history_test = df_le2_history.filter((f.col('contact_id') == checking_contact_id)
                                                          & (f.col('date_id') == checking_date_id))
        print('is_checking::df_le2_history_test')
        df_le2_history_test.show(30)

    if is_dev:
        print('df_le2_history')
        df_le2_history.printSchema()
        df_le2_history.show(3)

    df_le_total = df_collect_lssclt\
        .join(other=df_collect_ai_stater,
              on=['contact_id', 'date_id'],
              how='outer')\
        .join(other=df_le2_history,
              on=['contact_id', 'date_id'],
              how='outer')\
        .join(df_student_contact,
              on=['contact_id'],
              how='inner')

    df_le_total = df_le_total.withColumn("transformed_at", f.lit(d4))
    df_le_total = df_le_total.fillna(0)

    if is_checking:
        df_le_total_test = df_le_total.filter((f.col('contact_id') == checking_contact_id)
                                                          & (f.col('date_id') == checking_date_id))
        print('is_checking::df_le_total_test')
        df_le_total_test.show(30)

    if is_dev:
        print('df_le_total')
        df_le_total.printSchema()
        df_le_total.show(3)

    dyf = DynamicFrame.fromDF(df_le_total, glueContext, "dyf")

    preactions = "DELETE student_detail.student_behavior_aggregation_day_v1 where date_id >= " + str(start_date_focus);

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "preactions": preactions,
                                                       "dbtable": "student_detail.student_behavior_aggregation_day_v1",
                                                       "database": "student_native_report"
                                                   },
                                                   redshift_tmp_dir="s3n://dts-odin/temp/bc200/student_detail/student_behavior_aggregation_day_v1",
                                                   transformation_ctx="datasink4")
    df_student_contact.unpersist()
    if is_dev:
        df_collect_lssclt.unpersist()
        df_collect_ai_stater.unpersist()
        df_le2_history.unpersist()

if __name__ == "__main__":
    main()
