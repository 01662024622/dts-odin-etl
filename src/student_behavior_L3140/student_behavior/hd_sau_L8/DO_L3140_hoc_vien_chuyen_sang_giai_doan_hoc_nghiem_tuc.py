import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType, IntegerType, LongType

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

is_dev = True
is_limit = False
number_limit=50
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")
PACKAGE_STATUS_CODE = "ACTIVED"

ADD_COLLUM={
    "behavior_id": f.lit(32),
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
            ("student_id", "int", "student_id", "long"),
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
    if par_1<result and par_1 > 99999999:
        result = par_1
    if par_2<result and par_2 > 99999999:
        result = par_2
    if par_3<result and par_3 > 99999999:
        result = par_3
    if par_4<result and par_4 > 99999999:
        result = par_4
    if par_5<result and par_5 > 99999999:
        result = par_5
    if par_6<result and par_6 > 99999999:
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
        df = df.limit(number_limit)
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
        df = df.limit(number_limit)
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


def etl_hoc_vien_trai_nghiem_mot_trong_cac_thanh_to_hoc():

    dyf_student_contact = connectGlue(database="tig_advisor",table_name="student_contact",
                                    select_fields=["_key","contact_id","student_id","user_name"],
                                    fillter=["contact_id","student_id","user_name"],
                                    duplicates=["contact_id","user_name"]
                                    ).rename_field("user_name","email")

    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[("_key", "cast:long")])
    try:
        flag_smile_care = spark.read.parquet(
            "s3://toxd-olap/transaction_log/flag/flag_behavior_trai_nghiem_1_trong_7_thanh_to.parquet")
        max_key = flag_smile_care.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    if dyf_student_contact>0:
        df_student_contact = dyf_student_contact.toDF()
        flag = df_student_contact.agg({"_key": "max"}).collect()[0][0]
        print(flag)
        prinDev(df_student_contact)
        df_ls_sc = get_ls_sc()
        df_lt = get_lt()
        df_hw = get_hw_basic()
        df_nt = get_native_talk()
        df_voxy = get_voxy()
        df_ncsb = get_ncsbasic()
        df_join = df_student_contact.join(df_ls_sc,df_ls_sc["student_id_ls_sc"]==df_student_contact["student_id"],"left").\
            join(df_lt,df_lt["student_id_lt"]==df_student_contact["student_id"],"left").\
            join(df_hw,df_hw["email_hw"]==df_student_contact["email"],"left").\
            join(df_nt,df_nt["contact_id_nt"]==df_student_contact["contact_id"],"left").\
            join(df_voxy,df_voxy["email_voxy"]==df_student_contact["email"],"left").\
            join(df_ncsb,df_ncsb["email_ncbs"]==df_student_contact["email"],"left")
        prinDev(df_join)
        df_fillter = df_join.select("contact_id","student_id",
            get_behavior_date("student_behavior_date_ls_sc",
                              "student_behavior_date_lt",
                              "student_behavior_date_voxy",
                              "student_behavior_date_hw",
                              "student_behavior_date_nt",
                              "student_behavior_date_ncsb").cast("long").alias("student_behavior_date"))

        df_fillter = df_fillter.filter(df_fillter.student_behavior_date < 1999999999)
        prinDev(df_fillter)
        # -----------------------------------------------------------------------------------------------------------------#


        df_join_level_code = set_package_advisor_level(df_fillter)
        for k, v in ADD_COLLUM.items():
            df_join_level_code = df_join_level_code.withColumn(k, v)
        prinDev(df_join_level_code,"end data")
        # return
        convertAndSaveS3(df_join_level_code)
        # (student_behavior_date, behavior_id, student_id, user_id, contact_id,
        #  package_code, student_level_code, package_status_code, transformed)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_behavior_trai_nghiem_1_trong_7_thanh_to.parquet",
                         mode="overwrite")
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
        rename_field("student_id", "student_id_level"). \
        rename_field("level_code", "student_level_code")


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


def get_ls_sc():
    mdl_logsservice_in_out = connectGlue(database="topicalms", table_name="mdl_logsservice_in_out_cutoff",
                               select_fields=["userid", "time_in", "time_out"],
                               fillter=["userid"],
                               duplicates=["userid","time_in"]
                               ).rename_field("userid","student_id_ls_sc")
    mdl_logsservice_in_out = Filter.apply(frame=mdl_logsservice_in_out,
                                          f=lambda x: x["time_in"] >= 1483203600
                                                      and x["time_out"] >1483203600
                                                      and (x["time_out"] - x["time_in"])>2100
                                          )
    mdl_logsservice_in_out = mdl_logsservice_in_out.toDF()
    mdl_logsservice_in_out = mdl_logsservice_in_out.drop("time_out")
    mdl_logsservice_in_out = mdl_logsservice_in_out.groupby("student_id_ls_sc").agg(
        f.min(mdl_logsservice_in_out.time_in).cast("long").alias("student_behavior_date_ls_sc"))
    return mdl_logsservice_in_out

def get_lt():
    dyf_native_livestream = connectGlue(database="native_livestream", table_name="log_in_out",
                               select_fields=["student_id", "time_in", "thoigianhoc"],
                               fillter=["student_id","thoigianhoc","time_in"],
                               duplicates=["student_id","time_in"]
                               ).rename_field("student_id","student_id_lt")

    dyf_native_livestream = dyf_native_livestream.resolveChoice(specs=[("time_in", "cast:long")])
    dyf_native_livestream = dyf_native_livestream.resolveChoice(specs=[("thoigianhoc", "cast:int")])


    dyf_native_livestream = Filter.apply(frame=dyf_native_livestream,f=lambda x: x["time_in"] >1483203600 and x["thoigianhoc"] >59)
    df_native_livestream = dyf_native_livestream.toDF()
    df_native_livestream = df_native_livestream.drop("thoigianhoc")
    df_native_livestream = df_native_livestream.groupby("student_id_lt").agg(
        f.min(expr("time_in div 1000")).cast("long").alias("student_behavior_date_lt"))
    return df_native_livestream


def get_voxy():
    dyf_voxy = connectGlue(database="voxy", table_name="voxy_api",
                               select_fields=["email", "time_created", "total_hours_studied"],
                               fillter=["email"],
                               duplicates=["email","time_created"]
                               ).rename_field("email","email_voxy")
    dyf_voxy = Filter.apply(frame=dyf_voxy,
                                          f=lambda x:  x["total_hours_studied"] >0
                                          )
    df_voxy = dyf_voxy.toDF()

    df_voxy = df_voxy.withColumn("time_created_new",
                                 f.unix_timestamp(df_voxy.time_created, "yyyy-MM-dd"))
    dyf_voxy = DynamicFrame.fromDF(df_voxy, glueContext, "dyf_voxy")

    dyf_voxy = dyf_voxy.resolveChoice(specs=[("time_created_new", "cast:long")])
    df_voxy = dyf_voxy.toDF()
    df_voxy.drop("time_created")
    df_voxy = df_voxy.groupby("email_voxy").agg(
        f.min(df_voxy.time_created_new).alias("student_behavior_date_voxy"))


    return df_voxy


def get_hw_basic():
    dyf_mdl_le_exam_attemp = connectGlue(database="home_work_basic_production", table_name="mdl_le_exam_attemp",
                               select_fields=["user_id", "created_at"],
                               fillter=["user_id"],
                               duplicates=["user_id","created_at"]
                               )

    df_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.toDF()

    df_mdl_le_exam_attemp = df_mdl_le_exam_attemp.withColumn("created_at_min",
                                 f.unix_timestamp(df_mdl_le_exam_attemp.created_at, "yyyy-MM-dd HH:mm:ss"))
    dyf_mdl_le_exam_attemp = DynamicFrame.fromDF(df_mdl_le_exam_attemp, glueContext, "dyf_mdl_le_exam_attemp")

    dyf_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.resolveChoice(specs=[("created_at_min", "cast:long")])
    df_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.toDF()

    df_mdl_le_exam_attemp = df_mdl_le_exam_attemp.groupby("user_id").agg(
        f.min(df_mdl_le_exam_attemp.created_at_min).alias("student_behavior_date_hw"))

    dyf_mdl_user = connectGlue(database="home_work_basic_production", table_name="mdl_user",
                                         select_fields=["id", "username"],
                                         fillter=["id"],
                                         duplicates=["id", "username"]
                                         ).rename_field("username","email_hw")
    df_mdl_user = dyf_mdl_user.toDF()
    join = df_mdl_user.join(df_mdl_le_exam_attemp,df_mdl_user.id==df_mdl_le_exam_attemp.user_id)
    join= join.drop("id","user_id")
    return join

def get_native_talk():
    dyf_native_talk_history_log = connectGlue(database="native_talk", table_name="native_talk_history_log_api",
                                         select_fields=["learning_date", "username","speaking_dialog_score"],
                                         fillter=["username"],
                                         duplicates=["username", "learning_date"]
                                         )
    dyf_native_talk_history_log = Filter.apply(frame=dyf_native_talk_history_log,
                            f=lambda x: x["speaking_dialog_score"] > 0
                            )

    df_native_talk_history_log = dyf_native_talk_history_log.toDF()
    df_native_talk_history_log = df_native_talk_history_log.drop("speaking_dialog_score")

    df_native_talk_history_log = df_native_talk_history_log.withColumn("learning_date_int",
                                                             f.unix_timestamp(df_native_talk_history_log.learning_date,
                                                                              "yyyy-MM-dd"))
    dyf_native_talk_history_log = DynamicFrame.fromDF(df_native_talk_history_log, glueContext, "dyf_native_talk_history_log")

    dyf_native_talk_history_log = dyf_native_talk_history_log.resolveChoice(specs=[("learning_date_int", "cast:long")])
    df_native_talk_history_log = dyf_native_talk_history_log.toDF()
    df_native_talk_history_log = df_native_talk_history_log.groupby("username").agg(
        f.min(df_native_talk_history_log.learning_date_int).alias("student_behavior_date_nt"))

    dyf_native_talk_account_mapping = connectGlue(database="native_talk", table_name="native_talk_account_mapping",
                                                  select_fields=["username", "contact_id"],
                                                  fillter=["username", "contact_id"],
                                                  duplicates=["username", "contact_id"]
                                                  ).rename_field("username", "username_mapping").\
                                                    rename_field("contact_id","contact_id_nt")
    df_native_talk_account_mapping = dyf_native_talk_account_mapping.toDF()
    join = df_native_talk_account_mapping.join(df_native_talk_history_log, df_native_talk_account_mapping.username_mapping == df_native_talk_history_log.username)
    join=join.drop("username_mapping","username")
    return join

def get_ncsbasic():
    dyf_results = connectGlue(database="ncsbasic", table_name="results",
                                         select_fields=["user_id", "time_created"],
                                         fillter=["user_id"],
                                         duplicates=["user_id", "time_created"]
                                         )
    dyf_results = dyf_results.resolveChoice(specs=[("time_created", "cast:long")])

    df_results = dyf_results.toDF()

    df_results = df_results.groupby("user_id").agg(
        f.min(df_results.time_created).alias("student_behavior_date_ncsb"))

    dyf_native_talk_account_mapping = connectGlue(database="ncsbasic", table_name="users",
                                                  select_fields=["_id", "email"],
                                                  fillter=["_id", "email"],
                                                  duplicates=["_id", "email"]
                                                  ).rename_field("email", "email_ncbs")
    df_native_talk_account_mapping = dyf_native_talk_account_mapping.toDF()
    join = df_results.join(df_native_talk_account_mapping, df_results.user_id == df_native_talk_account_mapping._id)
    join=join.drop("user_id","_id")
    return join


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
    etl_hoc_vien_trai_nghiem_mot_trong_cac_thanh_to_hoc()
if __name__ == "__main__":
    main()