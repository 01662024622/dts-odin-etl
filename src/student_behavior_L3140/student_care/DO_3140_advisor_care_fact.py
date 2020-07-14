import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice, DropNullFields, RenameField, Join
from datetime import date

# --------------------------------------------------Spark and Glue--------------------------------------------------------------------#
# sparkcontext
sc = SparkContext()
# glue with sparkcontext env
glueContext = GlueContext(sc)
# create new spark session working
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

# --------------------------------------------------Data for Comunication-------------------------------------------------------------------#


MAPPING = [("period_id", "int", "period_id", "int"),
           ("ip_phone", "string", "ip_phone", "string"),

           ("m_total_student_managed", "long", "m_total_student_managed", "long"),
           ("m_total_call", "long", "m_total_call", "long"),
           ("m_total_call_anwser", "long", "m_total_call_anwser", "long"),
           ("m_total_call_answer_m30", "long", "m_total_call_answer_m30", "long"),
           ("m_total_call_press_8", "long", "m_total_call_press_8", "long"),
           ("m_total_call_answer_m30_and_press_8", "long", "m_total_call_answer_m30_and_press_8", "long"),
           ("m_total_call_answer_m30_ratting", "long", "m_total_call_answer_m30_ratting", "long"),
           ("m_total_student_called", "long", "m_total_student_called", "long"),
           ("m_total_student_answer", "long", "m_total_student_answer", "long"),
           ("m_total_student_answer_m30", "long", "m_total_student_answer_m30", "long"),
           ("m_total_student_press_8", "long", "m_total_student_press_8", "long"),
           ("m_total_student_answer_m30_and_press_8", "long", "m_total_student_answer_m30_and_press_8", "long"),
           ("m_total_student_answer_m30_ratting", "long", "m_total_student_answer_m30_ratting", "long"),

           ("transformed_at", "string", "transformed_at", "long"),
           ("year_month_id", "string", "year_month_id", "long")]



is_dev = True
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")



REDSHIFT_USERNAME = "dtsodin"
REDSHIFT_PASSWORD = "DWHDtsodin@123"





# ------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="", table_name="", select_fields=[], fillter=[], cache=False, duplicates=[]):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name)

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
        df = df.limit(5000)
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
        df = df.limit(5000)
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

def print_is_dev(string):
    if is_dev:
        print (string)


# ----------------------------------------------------------------------------------------------------------------------#

def checkDumplicate(df, duplicates):
    if len(duplicates) == 0:
        return df.dropDuplicates()
    else:
        return df.dropDuplicates(duplicates)


# ----------------------------------------------------------------------------------------------------------------------#

def parquetToS3(dyf, path="default", format="parquet", default=["year_month_id"]):
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={
                                                     "path": path,
                                                     "partitionKeys": default
                                                 },
                                                 format=format)


# ----------------------------------------------------------------------------------------------------------------------#
def save_communication_redshift(dyf_communication):
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_communication,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "up_user_communication",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_profile/up_user_communication",
                                                   transformation_ctx="datasink4")


# -----------------------------------------------------------------------------------------------------------------------#
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


# -------------------------------------------------------Main-----------------------------------------------------------#

def main():

    dyf_student_care_advisor = connectGlue(
        database="callcenter",
        table_name="student_care_advisor",
        select_fields=["transformed_at", "idcall", "student_behavior_date", "student_id", "answer_duration", "total_duration","requested_rating","value_rating","ip_phone","call_status"],
        duplicates=["student_behavior_date", "student_id", "answer_duration", "total_duration","requested_rating","value_rating"],
        fillter=["student_id","ip_phone"]
    ).rename_field("transformed_at","_key")

    dyf_student_care_advisor = dyf_student_care_advisor.resolveChoice(specs=[("_key", "cast:long")])

    try:
        df_flag_phone_rating = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_student_care_advisor_fact.parquet")
        max_key = df_flag_phone_rating.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_student_care_advisor = Filter.apply(frame=dyf_student_care_advisor, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    count =dyf_student_care_advisor.count()
    print (count)
    if count>0:
        df_student_care_advisor = dyf_student_care_advisor.toDF()



        df_student_care_advisor = df_student_care_advisor.withColumn("transformed_at", f.lit(d4))
        prinDev(df_student_care_advisor)

        flag = df_student_care_advisor.agg({"_key": "max"}).collect()[0][0]

        convertAndSaveS3(df_student_care_advisor)
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_student_care_advisor_fact.parquet", mode="overwrite")

def convertAndSaveS3(df):
    df = df.withColumn("year_month_id", f.from_unixtime('student_behavior_date', format="yyyyMM"))

    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    if dyf.count() > 0:
        if is_dev:
            print('dyf____________________________')
            dyf.printSchema()
            dyf.show(10)

        behavior_mapping = mappingForAll(dyf, MAPPING)
        parquetToS3(dyf=behavior_mapping, path="s3://toxd-olap/transaction_log/student_care_advisor")


def prinDev(df, df_name="print full information"):
    if is_dev:
        df.printSchema()
        df.show(3)


if __name__ == "__main__":
    main()

