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


MAPPING = [("idcall", "string", "idcall", "string"),
           ("student_behavior_date", "int", "student_behavior_date", "long"),

           ("student_phone", "string", "student_phone", "string"),
           ("student_id", "string", "student_id", "string"),
           ("contact_id", "string", "contact_id", "string"),

           ("answertime", "int", "answer_duration", "int"),
           ("totaltime", "int", "total_duration", "int"),
           ("hanguphv", "int", "requested_rating", "int"),
           ("value_rating", "int", "value_rating", "int"),
           ("device", "string", "device", "string"),
           ("ip_phone", "string", "ip_phone", "string"),
           ("status", "string", "call_status", "string"),

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

    dyf_advisorcall_key = connectGlue(
        database="callcenter",
        table_name="advisorcall",
        select_fields=["_key", "calldate", "idcall", "rating", "device", "hanguphv","totaltime","answertime"],
        duplicates=["calldate", "idcall", "rating", "device", "hanguphv","totaltime","answertime"],
        fillter=["idcall"]
    ).rename_field("rating", "value_rating")

    dyf_advisorcall_key = dyf_advisorcall_key.resolveChoice(specs=[("_key", "cast:long")])

    try:
        df_flag_phone_rating = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_phone_care_answertime.parquet")
        max_key = df_flag_phone_rating.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_advisorcall_key = Filter.apply(frame=dyf_advisorcall_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    count =dyf_advisorcall_key.count()
    print (count)
    if count>0:
        df_advisorcall_key = dyf_advisorcall_key.toDF()
        df_advisorcall = df_advisorcall_key \
            .withColumn("student_behavior_date", f.unix_timestamp(df_advisorcall_key.calldate, "yyyy-MM-dd HH:mm:ss"))
        dyf_advisorcall = DynamicFrame.fromDF(df_advisorcall, glueContext, "dyf_advisorcall")

        dyf_advisorcall = dyf_advisorcall.resolveChoice(specs=[("student_behavior_date", "cast:int")])

        df_advisorcall = dyf_advisorcall.toDF()

        dyf_cdr = connectGlue(
            database="callcenter",
            table_name="cdr",
            select_fields=["ip_phone", "call_id", "student_phone", "status"],
            fillter = ["call_id"]
        )
        df_cdr = dyf_cdr.toDF()
        df_advisorcall = df_advisorcall.join(df_cdr,
                                             (df_advisorcall["idcall"] == df_cdr["call_id"]),"right")


        dyf_student_contact_phone = connectGlue(
            database="tig_advisor",
            table_name="student_contact_phone",
            select_fields=["phone", "contact_id"],
            fillter=["phone", "contact_id"],
            duplicates=["phone", "contact_id"]
        )
        df_student_contact_phone = dyf_student_contact_phone.toDF()

        df_advisorcall = df_advisorcall.join(df_student_contact_phone,
                                             (df_advisorcall["student_phone"] == df_student_contact_phone["phone"]))

        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["student_id", "contact_id"],
            fillter=["student_id", "contact_id"],
            duplicates=["student_id", "contact_id"]
        ).rename_field("contact_id","contact_id_contact")
        df_student_contact = dyf_student_contact.toDF()

        df_advisorcall = df_advisorcall.join(df_student_contact,
                                             (df_advisorcall["contact_id"] == df_student_contact["contact_id_contact"]))

        df_advisorcall = df_advisorcall.drop("contact_id_contact", "phone", "call_id")

        df_rating_phone = df_advisorcall.withColumn("transformed_at", f.lit(d4))

        flag = df_rating_phone.agg({"_key": "max"}).collect()[0][0]
        prinDev(df_rating_phone)


        convertAndSaveS3(df_rating_phone)
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_phone_care_answertime.parquet", mode="overwrite")

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

