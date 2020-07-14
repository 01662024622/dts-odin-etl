import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice, DropNullFields, RenameField, Join
from datetime import date


# sparkcontext
sc = SparkContext()
# glue with sparkcontext env
glueContext = GlueContext(sc)
# create new spark session working
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = False
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")
PACKAGE_STATUS_CODE = "ACTIVED"

ADD_COLLUM={
    "behavior_id": f.lit(34),
    "transformed_at": f.lit(d4),
    "package_status_code": f.lit(PACKAGE_STATUS_CODE)
}


def concaText(student_behavior_date, behavior_id, student_id, contact_id,
              package_code,student_level_code, package_status_code, transformed_at):
    text_concat = ""
    if student_behavior_date is not None:
        text_concat += str(student_behavior_date)
    if behavior_id is not None:
        text_concat += str(behavior_id)
    if student_id is not None:
        text_concat += str(student_id)
    if contact_id is not None:
        text_concat += str(contact_id.decode("ascii", "ignore"))
    if package_code is not None:
        text_concat += str(package_code)
    if student_level_code is not None:
        text_concat += str(student_level_code)
    if package_status_code is not None:
        text_concat += str(package_status_code)
    if transformed_at is not None:
        text_concat += str(transformed_at)
    return text_concat


concaText = f.udf(concaText, StringType())

MAPPING = [("student_behavior_id", "string", "student_behavior_id", "string"),
           ("student_behavior_date", "long", "student_behavior_date", "long"),

            ("behavior_id", "int", "behavior_id", "int"),
            ("student_id", "string", "student_id", "long"),
            ("contact_id", "string", "contact_id", "string"),

            ("package_code", "string", "package_code", "string"),
            ("student_level_code", "string", "student_level_code", "string"),
            ("package_status_code", "string", "package_status_code", "string"),
           ("advisor_id", "long", "advisor_id", "long"),

           ("transformed_at", "string", "transformed_at", "long"),
           ("year_month_id", "string", "year_month_id", "long")]


REDSHIFT_USERNAME = "dtsodin"
REDSHIFT_PASSWORD = "DWHDtsodin@123"


def get_behavior_date(par_1, par_2, par_3, par_4, par_5, par_6):
    result=1999999999
    if par_1<result and par_1 > 999999:
        result = par_1
    if par_2<result and par_2 > 999999:
        result = par_2
    if par_3<result and par_3 > 999999:
        result = par_3
    if par_4<result and par_4 > 999999:
        result = par_4
    if par_5<result and par_5 > 999999:
        result = par_5
    if par_6<result and par_6 > 999999:
        result = par_6
    return result


get_behavior_date = udf(get_behavior_date, IntegerType())

# ------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="", table_name="", select_fields=[], fillter=[], cache=False, duplicates=[]):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database,table_name=table_name)

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
        dyf.printSchema()
    return dyf

# ----------------------------------------------------------------------------------------------------------------------#

def checkDumplicate(df, duplicates):
    if len(duplicates) == 0:
        return df.dropDuplicates()
    else:
        return df.dropDuplicates(duplicates)


# ----------------------------------------------------------------------------------------------------------------------#
def parquetToS3(dyf, path="default", format="parquet"):
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={
                                                     "path": path,
                                                     "partitionKeys": ["behavior_id", "year_month_id"]
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


# ------------------------ount() > 0---------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame
# -----------------------------------------------------------------------------------------------------------------------#


def mappingForAll(dynamicFrame, mapping):

    # for k, v in add_collum.items():
    #     df_communication = df_communication.withColumn(k, v)

    applymapping2 = ApplyMapping.apply(frame=dynamicFrame,
                                       mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_communication_after_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_communication_after_mapping


def etl_hoc_vien_chuyen_sang_giai_doan_hoc_nghiem_tuc():
    dyf_advisor_call_key = connectGlue(database="callcenter", table_name="advisorcall",
                                      select_fields=["_key","calldate", "device", "phonenumber","status"],
                                      fillter=["calldate", "device", "status", "phonenumber"],
                                      duplicates=["calldate", "device", "status", "phonenumber"]
                                      )
    dyf_advisor_call_key = Filter.apply(frame=dyf_advisor_call_key, f=lambda x: x["device"] == "3CX"
                                                                                and x["status"] == "ANSWER"
                                        )

    try:
        dyf_advisor_call_key = dyf_advisor_call_key.resolveChoice(specs=[("_key", "cast:long")])
        df_flag_advisor_call = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_advisorcall_hoc_theo_dinh_huong.parquet")
        max_key = df_flag_advisor_call.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_advisor_call_key = Filter.apply(frame=dyf_advisor_call_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")


    if dyf_advisor_call_key.count()>0:
        dyf_advisor_call_key = dyf_advisor_call_key.resolveChoice(specs=[("phonenumber", "cast:string")])
        df_advisor_call_key = dyf_advisor_call_key.toDF()

        flag = df_advisor_call_key.agg({"_key": "max"}).collect()[0][0]

        df_advisor_call = df_advisor_call_key.select("phonenumber_full",
                                     f.unix_timestamp(df_advisor_call_key.calldate, "yyyy-MM-dd HH:mm:ss").cast("long").alias("calldate_long"))

        df_advisor_call = df_advisor_call.groupby( "phonenumber_full").agg(
            f.min(df_advisor_call.calldate_long).alias("student_behavior_date"))


        dyf_student_contact_phone = connectGlue(database="tig_advisor", table_name="student_contact_phone",
                                          select_fields=["contact_id", "phone"],
                                          fillter=["contact_id", "phone",],
                                          duplicates=["contact_id", "phone"]
                                          )
        df_student_contact_phone = dyf_student_contact_phone.toDF()
        df_join_01 = df_advisor_call.join(df_student_contact_phone,df_student_contact_phone.phone.contains(df_advisor_call.phonenumber_full))

        dyf_student_contact = connectGlue(database="tig_advisor",table_name="student_contact",
                                        select_fields=["contact_id","student_id"],
                                        fillter=["contact_id","student_id"],
                                        duplicates=["contact_id","student_id"]
                                        ).rename_field("contact_id","contact_id_contact")
        df_student_contact = dyf_student_contact.toDF()

        df_join_02 = df_join_01.join(df_student_contact,df_join_01["contact_id"]==df_student_contact["contact_id_contact"])
        df_join_02 = df_join_02.drop("contact_id_contact","phone","phonenumber_full")
        prinDev(df_join_02)

        # -----------------------------------------------------------------------------------------------------------------#
        df_join_level_code = set_package_advisor_level(df_join_02)
        for k, v in ADD_COLLUM.items():
            df_join_level_code = df_join_level_code.withColumn(k, v)
        prinDev(df_join_level_code,"end data")
        convertAndSaveS3(df_join_level_code)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_advisorcall_hoc_theo_dinh_huong.parquet", mode="overwrite")


    # ----------------------------------------------------------------------------------------------------------------




def get_df_student_package():
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_package",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )
    dyf_student_package =Filter.apply(frame=dyf_student_package, f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_package.printSchema()
    dyf_student_package = dyf_student_package.select_fields(
        ["contact_id","package_status_code", "package_start_time","package_end_time","package_code"]).\
        rename_field("contact_id", "contact_id_package")

    df_student_package= dyf_student_package.toDF()


    return df_student_package

def get_df_student_level():
    dyf_student_level = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_level",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )
    dyf_student_level =Filter.apply(frame=dyf_student_level, f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_level.printSchema()

    dyf_student_level = dyf_student_level.select_fields(
        ["contact_id","level_code", "start_date","end_date","student_id"]).\
        rename_field("contact_id", "contact_id_level").\
        rename_field("student_id", "student_id_level")

    df_student_level= dyf_student_level.toDF()

    if is_dev:
        print("dyf_student_level__original")
        df_student_level.show(10)

    return df_student_level

def get_df_student_advisor():
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_advisor",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )
    dyf_student_package =Filter.apply(frame=dyf_student_package, f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_package.printSchema()
        dyf_student_package.show(10)

    dyf_student_package = dyf_student_package.select_fields(
        ["contact_id","advisor_id", "end_date","start_date"]) .\
        rename_field("contact_id", "contact_id_advisor")

    df_student_package= dyf_student_package.toDF()
    prinDev(df_student_package,"dyf_student_package")
    return df_student_package


def set_package_advisor_level(df):

    df_student_level = get_df_student_level()
    df = df.join(df_student_level, (df["student_behavior_date"] >= df_student_level["start_date"])
                 & (df["student_behavior_date"] <= df_student_level["end_date"])
                 & ((df["contact_id"] == df_student_level["contact_id_level"])
                    | (df["student_id"] == df_student_level["student_id_level"])), "left")

    df=df.drop("student_id_level","contact_id_level","end_date","start_date")
    df = df.withColumnRenamed("level_code","student_level_code")
    prinDev(df, "join with level")


    df_student_advisor = get_df_student_advisor()
    df = df.join(df_student_advisor, (df["student_behavior_date"] >= df_student_advisor["start_date"])
                 & (df["student_behavior_date"] <= df_student_advisor["end_date"])
                 & (df["contact_id"] == df_student_advisor["contact_id_advisor"]), "left")

    df = df.drop("contact_id_advisor", "start_date", "end_date")
    prinDev(df, "join with advisor")

    df_student_package = get_df_student_package()
    df = df.join(df_student_package, (df["student_behavior_date"] >= df_student_package["package_start_time"])
                 & (df["student_behavior_date"] <= df_student_package["package_end_time"])
                 & (df["contact_id"] == df_student_package["contact_id_package"]), "left")

    df = df.drop("contact_id_package", "package_start_time", "package_end_time")
    prinDev(df, "join with package")

    return df



def convertAndSaveS3(df):
    df = df.dropDuplicates()
    prinDev(df, "befor convert")
    df = df.withColumn("year_month_id",f.from_unixtime(df.student_behavior_date, format="yyyyMM"))
    df = df.withColumn("student_behavior_id",
                                             f.md5(concaText(
                                                 df.student_behavior_date,
                                                 df.behavior_id,
                                                 df.student_id,
                                                 df.contact_id,
                                                 df.package_code,
                                                 df.package_status_code,
                                                 df.student_level_code,
                                                 df.transformed_at)))
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    prinDev(df,"befor mapping")
    behavior_mapping = mappingForAll(dyf, MAPPING)


    if behavior_mapping.count() > 0:
        parquetToS3(dyf=behavior_mapping, path="s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior")




def prinDev(df,df_name="print full information"):
    if is_dev:
        df.printSchema()
        df.show(3)


def main():
    etl_hoc_vien_chuyen_sang_giai_doan_hoc_nghiem_tuc()
if __name__ == "__main__":
    main()