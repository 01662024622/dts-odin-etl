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
PACKAGE_STATUS_CODE = "DEACTIVED"

ADD_COLLUM={
    "behavior_id": f.lit(4),
    "transformed_at": f.lit(d4),
    "package_status_code":f.lit(PACKAGE_STATUS_CODE),
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
        text_concat += str(contact_id)
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
        print string


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


# ---------------------------------------------------------------------------------------------------------------------#
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


def etl_hoc_vien_duoc_chao_mung():
    dyf_av_care_call = connectGlue(database="tig_advisor", table_name="care_call",
                                    select_fields=["_key","phone", "duration", "call_status","time_created"],
                                    fillter=["phone", "duration", "call_status"],
                                    duplicates=["phone", "call_status", "time_created"],
                                    ).rename_field("phone", "phone1")


    dyf_av_care_call = Filter.apply(frame=dyf_av_care_call,
                                    f=lambda x: x["call_status"] in ("success", "call_success") and x["duration"] > 30)

    dyf_av_care_call = dyf_av_care_call.resolveChoice(specs=[("_key", "cast:long")])

    try:
        df_flag_1 = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_behavior_chao_mung_hoc_vien.parquet")
        max_key = df_flag_1.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_av_care_call = Filter.apply(frame=dyf_av_care_call, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    if dyf_av_care_call.count()>0:
        df_av_care_call = dyf_av_care_call.toDF()
        flag = df_av_care_call.agg({"_key": "max"}).collect()[0][0]

        dyf_student_contact_phone = connectGlue(database="tig_advisor", table_name="student_contact_phone",
                                        select_fields=["contact_id", "phone"],
                                        fillter=["contact_id", "phone"],
                                        duplicates=["contact_id", "phone"],
                                        )

        df_av_care_call = df_av_care_call\
        .withColumn("student_behavior_date", f.unix_timestamp(df_av_care_call.time_created, "yyyy-MM-dd HH:mm:ss"))
        dyf_av_care_call = DynamicFrame.fromDF(df_av_care_call, glueContext, "dyf_av_care_call")

        dyf_av_care_call = dyf_av_care_call.resolveChoice(specs=[("student_behavior_date", "cast:int")])
        df_av_care_call = dyf_av_care_call.toDF()

        dyf_student_contact = connectGlue(database="tig_advisor", table_name="student_contact",
                                          select_fields=["student_id", "contact_id"],
                                          duplicates=["student_id", "contact_id"],
                                          fillter=["student_id", "contact_id"]
                                          ).rename_field("contact_id", "contact_id_contact")
        df_student_contact = dyf_student_contact.toDF()



        df_student_contact_phone = dyf_student_contact_phone.toDF()
        df_student_care = df_av_care_call.join(df_student_contact_phone,
                                               (df_av_care_call["phone1"] == df_student_contact_phone["phone"]),"left")
        df_student_care = df_student_care.drop("phone1","phone")
        # -----------------------------------------------------------------------------------------------------------------#

        # -----------------------------------------------------------------------------------------------------------------#
        df_student_care = df_student_care.join(df_student_contact,
                                               (df_student_care["contact_id"] == df_student_contact["contact_id_contact"]), "left")
        df_student_care=df_student_care.drop("contact_id_contact")

        df_package_code = khoa_hoc()
        df_behavior_join_2=df_student_care.join(df_package_code,df_student_care["contact_id"]==df_package_code["crm_id"])
        df_behavior_join_2=df_behavior_join_2.drop("crm_ id")
        df_behavior_join_2 = set_package_advisor_level(df_behavior_join_2)

        for k, v in ADD_COLLUM.items():
            df_behavior_join_2 = df_behavior_join_2.withColumn(k, v)
        prinDev(df_behavior_join_2,"end_code")
        # (student_behavior_date, behavior_id, student_id, user_id, contact_id,
        #  package_code, student_level_code, package_status_code, transformed)

        convertAndSaveS3(df_behavior_join_2)
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_behavior_chao_mung_hoc_vien.parquet", mode="overwrite")
        # ----------------------------------------------------------------------------------------------------------------




def package_code():

    dyf_package_mapping_poss = connectGlue(database="poss", table_name="package_mapping",
                                            select_fields=["poss_package_name", "market_package_name"],
                                            fillter=["poss_package_name", "market_package_name"],
                                            duplicates=["poss_package_name", "market_package_name"]
                                            ).rename_field("market_package_name","package_code")

    dyf_san_pham_poss = connectGlue(database="poss", table_name="goi_sanpham",
                                            select_fields=["id", "ma"],
                                            fillter=["id", "ma"],
                                            duplicates=["id", "ma"]
                                           )
    df_package_mapping_poss=dyf_package_mapping_poss.toDF()
    df_san_pham_poss=dyf_san_pham_poss.toDF()

    df_join=df_package_mapping_poss.join(df_san_pham_poss,
                                         (df_san_pham_poss.ma.contains(df_package_mapping_poss.poss_package_name)))
    df_join=df_join.drop("poss_package_name","ma")
    if is_dev:
        print df_package_mapping_poss.count()
        print df_san_pham_poss.count()
        df_join.printSchema()
        df_join.show(10)
    return df_join

def khoa_hoc():
    dyf_hoc_vien_poss =connectGlue(database="poss", table_name="hoc_vien",
                                    select_fields=["mahv", "crm_id"],
                                    fillter=["mahv","crm_id"],
                                    duplicates=["mahv","crm_id"]
                                    )

    df_hoc_vien_poss=dyf_hoc_vien_poss.toDF()

    # df_ghi_nhan_hoc_phi=df_ghi_nhan_hoc_phi.groupBy("khoa_hoc_makh").agg(
    #         f.max("ngay_tao").alias("ngay_tao")
    #     )

    dyf_khoa_hoc_poss = connectGlue(database="poss", table_name="khoa_hoc",
                                    select_fields=["_key", "mahv", "goi_sanpham_id"],
                                    fillter=["mahv", "goi_sanpham_id"],
                                    duplicates=["mahv", "goi_sanpham_id"]
                                    ).rename_field("mahv","ma_hv")

    dyf_khoa_hoc_poss = dyf_khoa_hoc_poss.resolveChoice(specs=[("_key", "cast:long")])

    df_khoa_hoc_poss = dyf_khoa_hoc_poss.toDF()
    df_khach_hang_crm=df_khoa_hoc_poss.join(df_hoc_vien_poss,(df_khoa_hoc_poss["ma_hv"]==df_hoc_vien_poss["mahv"]))
    df_khach_hang_crm=df_khach_hang_crm.drop("ma_hv","mahv")
    if is_dev:
        df_khach_hang_crm.show(10)
    # try:
    #     df_flag_1 = spark.read.parquet("s3://dtsodin/flag/flag_hoc_vien_rating_hotline.parquet")
    #     max_key = df_flag_1.collect()[0]["flag"]
    #     print("max_key:  ", max_key)
    #     # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    #     dyf_advisorcall = Filter.apply(frame=dyf_advisorcall, f=lambda x: x["_key"] > max_key)
    # except:
    #     print("read flag file error ")
    df_package_code = package_code()
    if is_dev:
        df_package_code.printSchema()
        df_khach_hang_crm.printSchema()
    df_packge_code_contact = df_khach_hang_crm.join(df_package_code,
                                                  (df_khach_hang_crm["goi_sanpham_id"]==df_package_code["id"]))
    df_packge_code_contact = df_packge_code_contact.drop("goi_sanpham_id","id")
    if is_dev:
        df_packge_code_contact.printSchema()
        df_packge_code_contact.show(10)
    return df_packge_code_contact


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
        rename_field("student_id", "student_id_level").\
        rename_field("level_code", "student_level_code")

    df_student_level= dyf_student_level.toDF()

    if is_dev:
        print("dyf_student_level__original")
        df_student_level.show(10)

    return df_student_level

def get_df_student_advisor():
    dyf_student_advisor = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_advisor",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )
    dyf_student_advisor =Filter.apply(frame=dyf_student_advisor, f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_advisor.printSchema()
        dyf_student_advisor.show(10)

    dyf_student_advisor = dyf_student_advisor.select_fields(
        ["contact_id","advisor_id", "end_date","start_date"]) .\
        rename_field("contact_id", "contact_id_advisor")

    df_student_package= dyf_student_advisor.toDF()
    prinDev(df_student_package,"df_student_advisor")
    return df_student_package


def set_package_advisor_level(df):

    df_student_level = get_df_student_level()
    df = df.join(df_student_level, (df["student_behavior_date"] >= df_student_level["start_date"])
                 & (df["student_behavior_date"] <= df_student_level["end_date"])
                 & ((df["contact_id"] == df_student_level["contact_id_level"])
                    | (df["student_id"] == df_student_level["student_id_level"])), "left")

    df = df.drop("student_id_level","contact_id_level","end_date","start_date")
    prinDev(df, "join with level")


    df_student_advisor = get_df_student_advisor()
    df = df.join(df_student_advisor, (df["student_behavior_date"] >= df_student_advisor["start_date"])
                 & (df["student_behavior_date"] <= df_student_advisor["end_date"])
                 & (df["contact_id"] == df_student_advisor["contact_id_advisor"]), "left")

    df = df.drop("contact_id_advisor", "start_date", "end_date")
    prinDev(df, "join with advisor")

    return df


def convertAndSaveS3(df):
    df = df.dropDuplicates()
    prinDev(df, "befor convert")
    df = df.withColumn("year_month_id",f.from_unixtime('student_behavior_date', format="yyyyMM"))
    prinDev(df,"befor mapping")
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


    behavior_mapping = mappingForAll(dyf, MAPPING)

    prinDev(behavior_mapping,"after mapping")
    if behavior_mapping.count() > 0:
        parquetToS3(dyf=behavior_mapping, path="s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior")




def prinDev(df,df_name="print full information"):
    if is_dev:
        print df_name
        df.printSchema()
        df.show(3)
        print (df_name,"count is:", df.count())
def main():
    etl_hoc_vien_duoc_chao_mung()
if __name__ == "__main__":
    main()