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

is_dev = True
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")
PACKAGE_STATUS_CODE = "DEACTIVED"

ADD_COLLUM={
    "behavior_id": f.lit(3),
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
        text_concat += str(contact_id.decode('ascii', 'ignore'))
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
           ("student_behavior_date", "int", "student_behavior_date", "long"),

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


def checkData(par_1, par_2):
    if par_1 is not None:
        return par_1
    return par_2


checkData = udf(checkData, StringType())

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
        print("full schema of table: ", database, " and table: ", table_name, " after select")
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


def etl_duoc_mua_khoa_hoc():
    dyf_tig_market=connectGlue(database="tig_market",table_name="tpe_invoice_product",
                                    select_fields=["_key","user_id","timecreated","buyer_id","invoice_code"],
                                    fillter=["user_id"],
                                    duplicates=["user_id","timecreated","buyer_id","invoice_code"]
                                    ).rename_field("user_id","contact_id")\
                                    .rename_field("timecreated","student_behavior_date")\
                                    .rename_field("buyer_id","advisor_id")
    dyf_tig_market_details=connectGlue(database="tig_market",table_name="tpe_invoice_product_details",
                                    select_fields=["_key","cat_code","invoice_code"],
                                    fillter=["invoice_code"],
                                    duplicates=["invoice_code","cat_code"]
                                    ).rename_field("cat_code","package_code")\
                                    .rename_field("invoice_code","invoice_code_detail")


    dyf_student_contact = connectGlue(database="tig_advisor", table_name="student_contact",
                                         select_fields=["student_id", "contact_id"],
                                         duplicates=["student_id", "contact_id"],
                                         fillter=["student_id", "contact_id"]
                                         ).rename_field("contact_id", "contact_id_contact")
    df_student_contact = dyf_student_contact.toDF()

    df_tig_market = dyf_tig_market.toDF()
    df_tig_market_details = dyf_tig_market_details.toDF()
    df_tig_market = df_tig_market.join(df_tig_market_details,df_tig_market["invoice_code"]==df_tig_market_details["invoice_code_detail"],"left")
    df_tig_market= df_tig_market.drop("invoice_code","invoice_code_detail")
    df_tig_market_contact = df_tig_market.join(df_student_contact,
                                           df_tig_market["contact_id"]==df_student_contact["contact_id_contact"],"left")

    # -----------------------------------------------------------------------------------------------------------------#
    dyf_contact_crm_native = connectGlue(database="crm_native", table_name="contacts",
                                         select_fields=["Id", "Code"],
                                         duplicates=["Code", "Id"],
                                         fillter=["Code", "Id"]
                                         ).rename_field("Id", "user_id")

    df_contact_crm_native = dyf_contact_crm_native.toDF()

    df_dang_ki_tai_khoan= df_tig_market_contact.join(df_contact_crm_native,
                                        (df_tig_market_contact["contact_id"] == df_contact_crm_native["Code"]),"left")
    df_dang_ki_tai_khoan=df_dang_ki_tai_khoan.drop("Code","contact_id_contact")


    prinDev(df_dang_ki_tai_khoan,"df_khoa_hoc_contact")
    # -----------------------------------------------------------------------------------------------------------------#





    df_join_level_code = set_package_advisor_level(df_dang_ki_tai_khoan)
    prinDev(df_join_level_code,"end data")
    for k, v in ADD_COLLUM.items():
        df_join_level_code = df_join_level_code.withColumn(k, v)
    convertAndSaveS3(df_join_level_code)
    # (student_behavior_date, behavior_id, student_id, user_id, contact_id,
    #  package_code, student_level_code, package_status_code, transformed)



    # ----------------------------------------------------------------------------------------------------------------



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


def set_package_advisor_level(df):

    df_student_level = get_df_student_level()
    df = df.join(df_student_level, (df["student_behavior_date"] >= df_student_level["start_date"])
                 & (df["student_behavior_date"] <= df_student_level["end_date"])
                 & ((df["contact_id"] == df_student_level["contact_id_level"])
                    | (df["student_id"] == df_student_level["student_id_level"])), "left")

    df=df.drop("student_id_level","contact_id_level","end_date","start_date")
    df = df.withColumnRenamed("level_code","student_level_code")
    prinDev(df, "join with level")


    return df

def convertAndSaveS3(df):
    df = df.dropDuplicates()
    prinDev(df, "befor convert")
    df = df.withColumn("year_month_id",f.from_unixtime('student_behavior_date', format="yyyyMM"))
    df = df.withColumn('student_behavior_id',
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
        print df_name
        df.printSchema()
        df.show(3)
        print (df_name,"count is:", df.count())


def main():
    etl_duoc_mua_khoa_hoc()
if __name__ == "__main__":
    main()