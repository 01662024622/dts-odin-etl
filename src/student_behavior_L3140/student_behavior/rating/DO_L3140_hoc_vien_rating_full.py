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

RATE_MAPPING = [
    ("student_behavior_id", "string", "student_behavior_id", "string"),
    ("rating_type", "string", "rating_type", "string"),
    ("comment", "string", "comment", "string"),
    ("rating_about", "int", "rating_about", "long"),
    ("number_rating", "int", "number_rating", "long"),
    ("value_rating", "int", "value_rating", "long"),
    ("behavior_id", "int", "behavior_id", "long"),
    ("year_month_id", "string", "year_month_id", "long")
]

ADD_COLLUM = {
    "behavior_id": f.lit(27),
    "rating_type": f.lit("rating_native_smile_h2472"),
    "comment": f.lit(""),
    "rating_about": f.lit(None),
    "number_rating": f.lit(1),
}

is_dev = False
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")

student_id_unavailable = '0'
package_endtime_unavailable = 99999999999
package_starttime_unavailable = 0
student_level_code_unavailable = 'UNAVAILABLE'
student_status_code_unavailable = 'UNAVAILABLE'

package_endtime = 'package_endtime'
package_starttime = 'package_starttime'
student_level_code = 'student_level_code'
student_status_code = 'student_status_code'

REDSHIFT_USERNAME = "dtsodin"
REDSHIFT_PASSWORD = "DWHDtsodin@123"


def doCheckModified(val1, val2):
    if val1 is not None:
        return val1
    return val2


check_modified_null = udf(doCheckModified, StringType())


def doCheckStudentID(code):
    code = str(code)
    if code is None:
        return student_id_unavailable
    return code


check_student_id = udf(doCheckStudentID, StringType())


def doCheckData(code, key):
    key = str(key)
    if code is None:
        if key == package_endtime:
            return package_endtime_unavailable
        else:
            return package_starttime_unavailable
    return code


check_data = udf(doCheckData, IntegerType())


def doCheckDataNull(code, key):
    code = str(code)
    key = str(key)
    if (code is None) & (key == student_level_code):
        return student_level_code_unavailable

    if (code is None) & (key == student_status_code):
        return student_status_code_unavailable

    return code


check_data_null = udf(doCheckDataNull, StringType())


def concaText(student_behavior_date, behavior_id, student_id, contact_id,
              package_code, package_status_code, student_level_code,
              transformed_at):
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

points = [1, 2, 3, 4, 5]
ratings = [1, 2, 3, 4, 5]
satisfaction = ["1", "2", "3", "4", "5"]
rangeid = [14, 15, 16, 17, 18]




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

def parquetToS3(dyf, path="default", format="parquet", default=["behavior_id", "year_month_id"]):
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
    cara_rating()
    # smile_care_rating()
    # phone_rating()
    # smile_H2472_care_rating()


    # ------------------------------------------------------------------------------------------------------------------#


def smile_care_rating():
    dyf_smile_care_key = connectGlue(database="native_smile",
                                 table_name="ticket_log_5450ed3d8cb5a34974310b6b26e451fa",
                                 select_fields=["_key", "requester_email", "satisfaction", "satisfaction_at","created_at"],
                                 fillter=['requester_email']
                                 ).rename_field("satisfaction", "value_rating")

    dyf_smile_care_key = Filter.apply(frame=dyf_smile_care_key, f=lambda x: x["value_rating"] in satisfaction)
    dyf_smile_care_key = dyf_smile_care_key.resolveChoice(specs=[("_key", "cast:long")])
    try:
        flag_smile_care = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_smile_care_rating.parquet")
        max_key = flag_smile_care.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_smile_care_key = Filter.apply(frame=dyf_smile_care_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")

    if dyf_smile_care_key.count() > 0:
        df_smile_care_key = dyf_smile_care_key.toDF()
        flag = df_smile_care_key.agg({"_key": "max"}).collect()[0][0]
        df_smile_care = df_smile_care_key \
            .withColumn("student_behavior_date", f.unix_timestamp(df_smile_care_key.created_at, "yyyy-MM-dd HH:mm:ss"))
        dyf_smile_care = DynamicFrame.fromDF(df_smile_care, glueContext, "dyf_smile_care")

        dyf_smile_care = dyf_smile_care.resolveChoice(specs=[("student_behavior_date", "cast:int")])
        dyf_smile_care = dyf_smile_care.select_fields(
            ["_key", "requester_email", "value_rating", "satisfaction_at", "student_behavior_date"])
        df_smile_care = dyf_smile_care.toDF()

        dyf_student_contact_email = connectGlue(
            database="tig_advisor",
            table_name="student_contact_email",
            select_fields=["email", "contact_id", "user_id"]
        )
        dyf_student_contact_ = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["student_id", "contact_id"]
        ).rename_field("contact_id", "contact_id_contact")
        df_student_contact = dyf_student_contact_.toDF()
        df_student_contact_email = dyf_student_contact_email.toDF()

        df_smile_care = df_smile_care.join(df_student_contact_email,
                                           (df_smile_care["requester_email"] == df_student_contact_email["email"]))
        df_smile_care = df_smile_care.join(df_student_contact,
                                           (df_smile_care["contact_id"] == df_student_contact["contact_id_contact"]))

        df_smile_care.drop("email", "requester_email", "contact_id_contact")

        df_smile_care = df_smile_care.withColumn("rating_type", f.lit("rating_native_smile_caresoft")) \
            .withColumn("comment", f.lit("")) \
            .withColumn("rating_about", f.lit(None)) \
            .withColumn("number_rating", f.lit(1)) \
            .withColumn("behavior_id", f.lit(26)) \
            .withColumn("transformed_at", f.lit(d4))

        df_smile_care = set_package_advisor_level(df_smile_care)

        convertAndSaveS3(df_smile_care)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_smile_care_rating.parquet", mode="overwrite")


def phone_rating():
    dyf_advisorcall_key = connectGlue(
        database="callcenter",
        table_name="advisorcall",
        select_fields=["_key", "calldate", "phonenumber", "rating", "device", "hanguptvts", "status"]
    )
    dyf_advisorcall_key = Filter.apply(frame=dyf_advisorcall_key, f=lambda x: x["rating"] in ratings
                                                                              and x["device"] == "3CX"
                                                                              and x["hanguptvts"] == 1
                                                                              and x["status"] == "ANSWER")

    dyf_advisorcall_key = dyf_advisorcall_key.resolveChoice(specs=[("_key", "cast:long")])

    try:
        df_flag_phone_rating = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_phone_rating.parquet")
        max_key = df_flag_phone_rating.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_advisorcall_key = Filter.apply(frame=dyf_advisorcall_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")

    if dyf_advisorcall_key.count()>0:

        df_advisorcall_key = dyf_advisorcall_key.toDF()
        flag = df_advisorcall_key.agg({"_key": "max"}).collect()[0][0]
        df_advisorcall = df_advisorcall_key \
            .withColumn("student_behavior_date", f.unix_timestamp(df_advisorcall_key.calldate, "yyyy-MM-dd HH:mm:ss"))
        dyf_advisorcall = DynamicFrame.fromDF(df_advisorcall, glueContext, "dyf_advisorcall")

        dyf_advisorcall = dyf_advisorcall.resolveChoice(specs=[("student_behavior_date", "cast:int")])
        dyf_advisorcall = dyf_advisorcall.select_fields(
            ["_key", "student_behavior_date", "phonenumber", "rating", "device", "hanguptvts", "status"]) \
            .rename_field("rating", "value_rating")

        df_advisorcall = dyf_advisorcall.toDF()


        dyf_student_contact_phone = connectGlue(
            database="tig_advisor",
            table_name="student_contact_phone",
            select_fields=["phone", "contact_id", "user_id"]
        )
        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["student_id", "contact_id"]
        ).rename_field("contact_id", "contact_id_contact")

        df_student_contact = dyf_student_contact.toDF()
        df_student_contact_phone = dyf_student_contact_phone.toDF()
        df_advisorcall = df_advisorcall.join(df_student_contact_phone,
                                             (df_advisorcall["phonenumber"] == df_student_contact_phone["phone"]))
        df_advisorcall = df_advisorcall.join(df_student_contact,
                                             (df_advisorcall["contact_id"] == df_student_contact["contact_id_contact"]))

        df_advisorcall = df_advisorcall.drop("phonenumber", "phone", "contact_id_contact")

        df_advisorcall = df_advisorcall.withColumn("comment", f.lit("")).withColumn("rating_about", f.lit(None)) \
            .withColumn("rating_type", f.lit("rating_hotline")) \
            .withColumn("number_rating", f.lit(1))

        df_rating_phone = df_advisorcall \
            .withColumn("behavior_id", f.lit(25)) \
            .withColumn("transformed_at", f.lit(d4))

        df_rating_phone = set_package_advisor_level(df_rating_phone)

        convertAndSaveS3(df_rating_phone)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_phone_rating.parquet", mode="overwrite")

def smile_H2472_care_rating():
    dyf_tblreply_rating_key = connectGlue(
        database="native_smile",
        table_name="tblreply_rating",
        select_fields=["_key", "userid", "ratingid", "time_rating"],
        fillter=['userid']
    ).rename_field("time_rating", "student_behavior_date").rename_field("userid", "student_id")

    dyf_tblreply_rating_key = Filter.apply(frame=dyf_tblreply_rating_key, f=lambda x: x["ratingid"] > 13)
    dyf_tblreply_rating_key = dyf_tblreply_rating_key.resolveChoice(specs=[("_key", "cast:long")])

    try:
        df_flag_H2472 = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_smile_H2472_care_rating.parquet")
        max_key = df_flag_H2472.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_tblreply_rating_key = Filter.apply(frame=dyf_tblreply_rating_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    if dyf_tblreply_rating_key.count() > 0:
        df_tblreply_rating_key = dyf_tblreply_rating_key.toDF()

        flag = df_tblreply_rating_key.agg({"_key": "max"}).collect()[0][0]

        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["contact_id", "student_id"],
            fillter=["contact_id", "student_id"],
            duplicates=["contact_id", "student_id"]
        ).rename_field("student_id", "student_id_contact")

        dyf_tma_dm_tu_dien = connectGlue(
            database="native_smile",
            table_name="tma_dm_tu_dien",
            select_fields=["id", "ma_tu_dien", "id_dm_loai_tu_dien"],
            fillter=["id", "id_dm_loai_tu_dien"]
        )

        dyf_tma_dm_tu_dien = Filter.apply(frame=dyf_tma_dm_tu_dien,
                                          f=lambda x: x["id_dm_loai_tu_dien"] == 7
                                                      and x["id"] in rangeid)
        # df_mdl_user=dyf_mdl_user.toDF()
        df_tma_dm_tu_dien = dyf_tma_dm_tu_dien.toDF()
        ################
        # df_tblreply_rating = df_tblreply_rating.join(df_mdl_user,(df_tblreply_rating["userid"]== df_mdl_user["id"]),"left")
        # join_rating_user.drop("id","userid")
        join_rating_user01 = df_tblreply_rating_key.join(df_tma_dm_tu_dien,
                                                     (df_tblreply_rating_key["ratingid"] == df_tma_dm_tu_dien["id"]))
        join_rating_user01.drop("id")
        df_student_contact = dyf_student_contact.toDF()
        join_rating_user01 = join_rating_user01.join(df_student_contact,
                                                     (join_rating_user01["student_id"] == df_student_contact[
                                                         "student_id_contact"]))
        join_rating_user01 = join_rating_user01.drop("student_id_contact")
        if is_dev:
            join_rating_user01.printSchema()
        join_rating_user01 = join_rating_user01.dropDuplicates()
        join_rating_user01 = join_rating_user01.withColumn("rating_type", f.lit("rating_native_smile_h2472")) \
            .withColumn("comment", f.lit("")) \
            .withColumn("rating_about", f.lit(None)) \
            .withColumn("number_rating", f.lit(1)) \
            .withColumn("value_rating", (join_rating_user01.ratingid - f.lit(13))) \
            .withColumn("behavior_id", f.lit(27)) \
            .withColumn("transformed_at", f.lit(d4))

        join_rating_user01 = set_package_advisor_level(join_rating_user01)
        convertAndSaveS3(join_rating_user01)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_smile_H2472_care_rating.parquet", mode="overwrite")


def cara_rating():

    dyf_mdl_rating_class_key = connectGlue(database="topicalms", table_name="mdl_rating_class",
                                       select_fields=["_key", "points", "vote", "student_id",
                                                      "room_id", "opinion", "timecreated", "timemodified"],
                                       fillter=["points", "student_id", "opinion", "timecreated"]
                                       ).rename_field("points", "value_rating"). \
        rename_field("opinion", "comment"). \
        rename_field("vote", "rating_about"). \
        rename_field("timecreated", "student_behavior_date")

    dyf_mdl_rating_class_key = dyf_mdl_rating_class_key.resolveChoice(specs=[("_key", "cast:long")])
    try:
        df_flag_1 = spark.read.parquet("s3://toxd-olap/transaction_log/flag/flag_cara_rating.parquet")
        max_key = df_flag_1.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf_mdl_rating_class_key = Filter.apply(frame=dyf_mdl_rating_class_key, f=lambda x: x["_key"] > max_key)
    except:
        print("read flag file error ")
    dyf_mdl_rating_class_key = Filter.apply(frame=dyf_mdl_rating_class_key, f=lambda x: x["value_rating"] in points)
    if dyf_mdl_rating_class_key.count() > 0:

        df_mdl_rating_class_key = dyf_mdl_rating_class_key.toDF()
        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["contact_id", "student_id"],
            fillter=["contact_id", "student_id"],
            duplicates=["contact_id", "student_id"]
        ).rename_field("student_id", "student_id_contact")




        df_student_contact = dyf_student_contact.toDF()
        df_mdl_rating_class = df_mdl_rating_class_key.join(df_student_contact,df_mdl_rating_class_key["student_id"] == df_student_contact["student_id_contact"])

        df_mdl_rating_class = df_mdl_rating_class.drop("student_id_contact")
        df_mdl_rating_class = df_mdl_rating_class.withColumn("number_rating", f.lit(1)) \
            .withColumn("rating_type", f.lit("rating_cara"))
        if is_dev:
            df_mdl_rating_class.show(10)

        #     free run
        df_mdl_rating_class = df_mdl_rating_class \
            .withColumn("behavior_id", f.lit(24)) \
            .withColumn("transformed_at", f.lit(d4))

        df_mdl_rating_class = set_package_advisor_level(df_mdl_rating_class)
        convertAndSaveS3(df_mdl_rating_class)

        flag = df_mdl_rating_class_key.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/flag_cara_rating.parquet", mode="overwrite")


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
    dyf_student_level = Filter.apply(frame=dyf_student_level,
                                     f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_level.printSchema()

    dyf_student_level = dyf_student_level.select_fields(
        ["contact_id", "level_code", "start_date", "end_date", "student_id", "user_id"]). \
        rename_field("level_code", "student_level_code"). \
        rename_field("contact_id", "contact_id_level"). \
        rename_field("student_id", "student_id_level")

    df_student_level = dyf_student_level.toDF()

    if is_dev:
        print("dyf_student_level__original")
        df_student_level.show(10)

    return df_student_level


def get_df_student_package():
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_package",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )
    dyf_student_package = Filter.apply(frame=dyf_student_package,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_package.printSchema()
        dyf_student_package.show(10)

    dyf_student_package = dyf_student_package.select_fields(
        ["contact_id", "package_code", "package_start_time", "student_id", "package_end_time", "package_status_code"]). \
        rename_field("contact_id", "contact_id_package"). \
        rename_field("student_id", "student_id_package")

    if is_dev:
        print("dyf_student_package")
        dyf_student_package.show(10)

    df_student_package = dyf_student_package.toDF()
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
    dyf_student_package = Filter.apply(frame=dyf_student_package,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != "")
    if is_dev:
        dyf_student_package.printSchema()
        dyf_student_package.show(10)

    dyf_student_package = dyf_student_package.select_fields(
        ["contact_id", "advisor_id", "end_date", "start_date"]). \
        rename_field("contact_id", "contact_id_advisor")

    df_student_package = dyf_student_package.toDF()
    return df_student_package


def set_package_advisor_level(df):
    df_student_level = get_df_student_level()
    df = df.join(df_student_level, (df["student_behavior_date"] >= df_student_level["start_date"])
                 & (df["student_behavior_date"] <= df_student_level["end_date"])
                 & ((df["contact_id"] == df_student_level["contact_id_level"])
                    | (df["student_id"] == df_student_level["student_id_level"])), "left")

    df = df.drop("student_id_level", "contact_id_level", "end_date", "start_date")


    df_student_package = get_df_student_package()
    df = df.join(df_student_package, (df["student_behavior_date"] >= df_student_package["package_start_time"])
                 & (df["student_behavior_date"] <= df_student_package["package_end_time"])
                 & ((df["contact_id"] == df_student_package["contact_id_package"])
                    | (df["student_id"] == df_student_package["student_id_package"])), "left")
    df = df.drop("student_id_package", "contact_id_package", "package_end_time", "package_start_time")


    df_student_advisor = get_df_student_advisor()
    df = df.join(df_student_advisor, (df["student_behavior_date"] >= df_student_advisor["start_date"])
                 & (df["student_behavior_date"] <= df_student_advisor["end_date"])
                 & (df["contact_id"] == df_student_advisor["contact_id_advisor"]), "left")

    df = df.drop("contact_id_advisor", "start_date", "end_date")


    return df


def convertAndSaveS3(df):
    df = df.withColumn("year_month_id", f.from_unixtime('student_behavior_date', format="yyyyMM"))
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

    if is_dev:
        print('dyf____________________________')
        dyf.printSchema()
        dyf.show(10)

    behavior_mapping = mappingForAll(dyf, MAPPING)

    if behavior_mapping.count() > 0:
        parquetToS3(dyf=behavior_mapping, path="s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior")
    rate_mapping = mappingForAll(dyf, RATE_MAPPING)
    if rate_mapping.count() > 0:
        parquetToS3(dyf=rate_mapping, path="s3://toxd-olap/transaction_log/student_behavior/sb_student_rating")


def prinDev(df, df_name="print full information"):
    if is_dev:
        df.printSchema()
        df.show(3)


if __name__ == "__main__":
    main()

