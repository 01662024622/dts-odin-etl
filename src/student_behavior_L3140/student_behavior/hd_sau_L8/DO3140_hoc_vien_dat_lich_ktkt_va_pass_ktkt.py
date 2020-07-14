import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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

ADD_COLLUM_HEN_KTKT={
    "behavior_id": f.lit(36),
    "transformed_at": f.lit(d4),
    "package_status_code": f.lit(PACKAGE_STATUS_CODE)
}
ADD_COLLUM_KTKT_TC={
    "behavior_id": f.lit(5),
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
    result=99999999
    if par_1<result and par_1 is not None:
        result = par_1
    if par_2<result and par_2 is not None:
        result = par_2
    if par_3<result and par_3 is not None:
        result = par_3
    if par_4<result and par_4 is not None:
        result = par_4
    if par_5<result and par_5 is not None:
        result = par_5
    if par_6<result and par_6 is not None:
        result = par_6
    return result


get_behavior_date = udf(get_behavior_date, StringType())

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


def etl_ktkt():
    dyf_technical_test = connectGlue(database="technical_test", table_name="student_technical_test",
                                     select_fields=["_key","trinhdohocvien", "studentid", "thoigianhenktkt", "ketluan"],
                                     fillter=["studentid", "thoigianhenktkt"],
                                     duplicates=["trinhdohocvien", "studentid", "thoigianhenktkt", "ketluan"]
                                     )

    dyf_technical_test = dyf_technical_test.resolveChoice(specs=[("_key", "cast:long"),("studentid", "cast:string")])
    try:
        flag_smile_care = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_behavior_ktkt.parquet")
        max_key = flag_smile_care.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_technical_test = Filter.apply(frame=dyf_technical_test, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")

    if dyf_technical_test.count()>0:
        df_technical_test = dyf_technical_test.toDF()

        flag = df_technical_test.agg({"_key": "max"}).collect()[0][0]


        dyf_student_contact = connectGlue(database="tig_advisor",table_name="student_contact",
                                        select_fields=["contact_id","student_id","user_name"],
                                        fillter=["contact_id","student_id"],
                                        duplicates=["contact_id","student_id"]
                                        )

        df_student_contact = dyf_student_contact.toDF()

        df_technical_test = df_technical_test.withColumn("date",
                                     f.unix_timestamp(df_technical_test.thoigianhenktkt, "yyyy-MM-dd HH:mm:ss"))
        dyf_technical_test = DynamicFrame.fromDF(df_technical_test, glueContext, "dyf_technical_test")
        dyf_technical_test = dyf_technical_test.resolveChoice(specs=[("date", "cast:long")])

        df_technical_test = dyf_technical_test.toDF()

        df_technical_test_min = df_technical_test.select("trinhdohocvien","studentid","date")
        df_technical_test_min = df_technical_test_min.groupBy("studentid","trinhdohocvien").agg(
            f.min(df_technical_test_min.date).alias("student_behavior_date"))

        df_join_min = df_student_contact.join(df_technical_test_min, df_technical_test_min["studentid"] == df_student_contact["student_id"])
        df_select_min = df_join_min.select("contact_id","student_id","student_behavior_date", df_join_min.trinhdohocvien.alias("student_level_code"))


        # -----------------------------------------------------------------------------------------------------------------#
        df_technical_test_pass =df_technical_test.where(df_technical_test.ketluan == "Pass")

        df_technical_test_pass = df_technical_test_pass.groupBy("studentid","trinhdohocvien").agg(
            f.min(df_technical_test_pass.date).alias("student_behavior_date"))

        df_join_pass = df_student_contact.join(df_technical_test_pass,
                                          df_technical_test_pass["studentid"] == df_student_contact["student_id"])
        df_select_pass = df_join_pass.select("contact_id", "student_id","" "student_behavior_date",df_join_min.trinhdohocvien.alias("student_level_code"))

        prinDev(df=df_select_pass,df_name="pass")
        prinDev(df=df_select_pass,df_name="min")


        df_join_level_code_min = set_package_advisor_level(df_select_min)
        for k, v in ADD_COLLUM_HEN_KTKT.items():
            df_join_level_code_min = df_join_level_code_min.withColumn(k, v)
        prinDev(df_join_level_code_min,"end data min")
        convertAndSaveS3(df_join_level_code_min)

        # ----------------------------------------------------------------------------------------------------------------
        df_join_level_code_pass = set_package_advisor_level(df_select_pass)
        for k, v in ADD_COLLUM_KTKT_TC.items():
            df_join_level_code_pass = df_join_level_code_pass.withColumn(k, v)
        prinDev(df_join_level_code_pass,"end data pass")
        convertAndSaveS3(df_join_level_code_pass)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_behavior_ktkt.parquet", mode="overwrite")


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
    etl_ktkt()
if __name__ == "__main__":
    main()