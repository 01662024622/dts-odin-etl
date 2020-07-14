import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.functions as f
from pyspark import SparkContext
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice, DropNullFields, RenameField, Join
from datetime import date, datetime
import pytz

# -----------------------------------------------

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

# --------------------------------------------------Data for Comunication-------------------------------------------------------------------#

LE2_S3_SAVE = "s3://toxd-olap/trasaction_le2/le2_history"
FLAG_NATIVE_TALK_FILE = "s3://toxd-olap/trasaction_le2/flag/flag_student_behavior_native_talk.parquet"
FLAG_NATIVE_TALK_SAVE = "s3a://toxd-olap/trasaction_le2/flag/flag_student_behavior_native_talk.parquet"

FLAG_VOXY_FILE = "s3://toxd-olap/trasaction_le2/flag/flag_student_behavior_voxy.parquet"
FLAG_VOXY_SAVE = "s3a://toxd-olap/trasaction_le2/flag/flag_student_behavior_voxy.parquet"

FLAG_NCSB_FILE = "s3://toxd-olap/trasaction_le2/flag/flag_student_behavior_ncsb.parquet"
FLAG_NCSB_SAVE = "s3a://toxd-olap/trasaction_le2/flag/flag_student_behavior_ncsb.parquet"

FLAG_HW_FILE = "s3://toxd-olap/trasaction_le2/flag/flag_student_behavior_hw.parquet"
FLAG_HW_SAVE = "s3a://toxd-olap/trasaction_le2/flag/flag_student_behavior_hw.parquet"

# -----------------------------------------------------------------------------------------------------------------#

MAPPING = [
    ("student_id", "string", "student_id", "long"),
    ("contact_id", "string", "contact_id", "string"),

    ("class_type", "string", "class_type", "string"),
    ("learning_date", "string", "learning_date", "string"),
    ("total", "long", "total_learing", "long"),
    ("total_duration", "long", "total_duration", "long"),

    ("year_month_id", "string", "year_month_id", "string"),
    ("transformed_at", "string", "transformed_at", "long")]

is_dev = False
is_limit = False
is_load_full = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.now(ho_chi_minh_timezone)
print('today: ', today)
today_id = long(today.strftime("%Y%m%d"))

REDSHIFT_USERNAME = "dtsodin"
REDSHIFT_PASSWORD = "DWHDtsodin@123"


# ------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="", table_name="", select_fields=[]):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=table_name)

    if is_dev:
        print("full schema of table: ", database, "and table: ", table_name)
        dyf.printSchema()
    #     select felds
    if len(select_fields) != 0:
        dyf = dyf.select_fields(select_fields)

    if is_dev:
        print("full schema of table: ", database, " and table: ", table_name, " after select")
        dyf.printSchema()
    return dyf


# ----------------------------------------------------------------------------------------------------------------------#


# ----------------------------------------------------------------------------------------------------------------------#

def parquetToS3(dyf, path="default", format="parquet", default=["class_type", "year_month_id"]):
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


# -----------------------------------------------------------------------------------------------------------------------#

def filter_flag(dyf, config_file):
    try:
        flag_smile_care = spark.read.parquet(config_file)
        max_key = flag_smile_care.collect()[0]["flag"]
        print("max_key:  ", max_key)
        dyf = Filter.apply(frame=dyf, f=lambda x: x["_key"] > max_key)
        return dyf
    except:
        print("read flag file error")
        return dyf


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
    # etl_ncsbasic(df_student_level=None, df_student_package=None, df_student_advisor=None)
    # etl_home_work(df_student_level=None, df_student_package=None, df_student_advisor=None)
    etl_native_talk(df_student_level=None, df_student_package=None, df_student_advisor=None)
    # etl_voxy(df_student_level=None, df_student_package=None, df_student_advisor=None)

    # ------------------------------------------------------------------------------------------------------------------#


def etl_native_talk(df_student_level=None, df_student_package=None, df_student_advisor=None):
    dyf_native_talk_history = connectGlue(
        database="native_talk",
        table_name="native_talk_history_log_api_cutoff_2020",
        select_fields=["_key", "username", "learning_date", "speaking_completed_dialog_name",
                       "speaking_dialog_score", "time_of_completingadialog"],
    )

    dyf_native_talk_history = Filter.apply(
        frame=dyf_native_talk_history,
        f=lambda x: x["username"] is not None and x["username"] != ""
                    and x["learning_date"] is not None
                    and x["learning_date"] != ""
                    and x["speaking_completed_dialog_name"] != ""
                    and x["speaking_dialog_score"] > 0)

    dyf_native_talk_history = dyf_native_talk_history.resolveChoice(specs=[("_key", "cast:float")])

    if not is_load_full:
        # dyf_native_talk_history = filter_flag(dyf_native_talk_history, FLAG_NATIVE_TALK_FILE)
        try:
            df_flag = spark.read.parquet(FLAG_NATIVE_TALK_FILE)
            read_from_index = df_flag.collect()[0]['flag']
            print('read from index: ', read_from_index)
            dyf_native_talk_history = Filter.apply(frame=dyf_native_talk_history,
                                                  f=lambda x: x["learning_date"] > read_from_index)
        except:
            print('read flag file error ')

    df_native_talk_history = dyf_native_talk_history.toDF()

    number_native_talk_history = df_native_talk_history.count()

    print("native_talk_history")
    print("number_native_talk_history_number: ", number_native_talk_history)
    prinDev(df_native_talk_history)

    if number_native_talk_history < 1:
        return
    df_native_talk_history = df_native_talk_history \
        .drop_duplicates(["username", "learning_date",
                          "speaking_completed_dialog_name",
                          "speaking_dialog_score", "time_of_completingadialog"])
    # ---------------------
    flag = df_native_talk_history.agg({"learning_date": "max"}).collect()[0][0]
    # -----------------------------------------------------------------------------------------------------

    df_native_talk_history = df_native_talk_history \
        .groupby("username", "learning_date") \
        .agg(f.count("speaking_dialog_score").alias("total"))
    df_native_talk_history = df_native_talk_history.select("username", "learning_date", "total")

    # -------------process duplicate ------------------------

    dyf_native_talk_mapping = connectGlue(
        database="native_talk",
        table_name="native_talk_account_mapping",
        select_fields=["username", "contact_id"],
    ).rename_field("username", "user_name_mapping")

    dyf_native_talk_mapping = Filter.apply(
        frame=dyf_native_talk_mapping,
        f=lambda x: x["user_name_mapping"] is not None and x["user_name_mapping"] != ""
                    and x["contact_id"] is not None
                    and x["contact_id"] != "")

    dyf_student_contact = connectGlue(
        database="tig_advisor",
        table_name="student_contact",
        select_fields=["student_id", "contact_id"],
    ).rename_field("contact_id", "contact_id_contact")
    df_native_talk_mapping = dyf_native_talk_mapping.toDF()
    df_native_talk_mapping = df_native_talk_mapping.drop_duplicates(["user_name_mapping", "contact_id"])

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.drop_duplicates(["student_id", "contact_id_contact"])

    df_result = df_native_talk_history \
        .join(df_native_talk_mapping,
              df_native_talk_history["username"] == df_native_talk_mapping["user_name_mapping"]) \
        .join(df_student_contact, df_native_talk_mapping["contact_id"] == df_student_contact["contact_id_contact"])

    df_result = df_result \
        .withColumn("total_duration", f.col('total') * f.lit(300)) \
        .withColumn("transformed_at", f.lit(d4)) \
        .withColumn("class_type", f.lit("NATIVE_TALK"))

    prinDev(df_result)
    # df_result = set_package_advisor_level(df_result, df_student_level=df_student_level,
    #                                       df_student_package=df_student_package, df_student_advisor=df_student_advisor)

    convertAndSaveS3(df_result)

    flag_data = [flag]
    df = spark.createDataFrame(flag_data, "string").toDF("flag")
    # ghi de _key vao s3
    # df.write.parquet(FLAG_NATIVE_TALK_SAVE, mode="overwrite")
    df.write.parquet(FLAG_NATIVE_TALK_SAVE, mode="overwrite")


def etl_voxy(df_student_level=None, df_student_package=None, df_student_advisor=None):
    dyf_voxy = connectGlue(database="voxy", table_name="voxy_api_cutoff_2020",
                           select_fields=["_key", "email", "last_login", "total_activities_completed",
                                          'total_hours_studied'],
                           )

    dyf_voxy = Filter.apply(
        frame=dyf_voxy,
        f=lambda x:
        # x["total_activities_completed"] > 0
        x["email"] is not None and x["email"] != ""
        and x['last_login'] is not None
    )

    if is_dev:
        print('dyf_voxy')
        dyf_voxy.printSchema()
        dyf_voxy.show(3)

    dyf_voxy = dyf_voxy.resolveChoice(specs=[("_key", "cast:long")])

    print('is_load_full___: ' + str(is_load_full))
    print('dyf_voxy before filter: ' + str(dyf_voxy.count()))
    if not is_load_full:
        print('not load full-----------------')
        dyf_voxy = filter_flag(dyf_voxy, FLAG_VOXY_FILE)

    number_dyf_voxy = dyf_voxy.count()
    print("number_dyf_voxy after filter :", number_dyf_voxy)

    if number_dyf_voxy > 0:
        df_voxy = dyf_voxy.toDF()

        flag = df_voxy.agg({"_key": "max"}).collect()[0][0]

        df_voxy = df_voxy.filter(~df_voxy.email.startswith("vip_"))

        # df_voxy = df_voxy.dropDuplicates(['email', 'last_login'])

        df_voxy = df_voxy.withColumn('learning_date',
                                     f.from_unixtime(timestamp=f.unix_timestamp(f.col('last_login'),
                                                                                format="yyyy-MM-dd'T'HH:mm:ss"),
                                                     format="yyyy-MM-dd")) \
            .withColumn('learning_date_id',
                        f.from_unixtime(timestamp=f.unix_timestamp(f.col('last_login'),
                                                                   format="yyyy-MM-dd'T'HH:mm:ss"),
                                        format="yyyyMMdd").cast('long'))

        df_voxy = df_voxy.filter(f.col('learning_date_id') < today_id)

        if is_dev:
            print('dyf_voxy__2')
            dyf_voxy.printSchema()
            dyf_voxy.show(3)

        df_voxy = df_voxy \
            .groupby("email", "learning_date") \
            .agg(f.sum("total_activities_completed").alias("total"),
                 f.sum("total_hours_studied").cast('double').alias("total_duration")
                 )
        df_voxy = df_voxy.select("email",
                                 "learning_date",
                                 "total",
                                 f.round(f.col('total_duration') * 3600).cast('long').alias('total_duration'))

        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["student_id", "contact_id", "user_name"],
        )
        dyf_student_contact = Filter.apply(
            frame=dyf_student_contact,
            f=lambda x: x["student_id"] is not None and x["student_id"] != ""
                        and x["contact_id"] is not None and x["contact_id"] != ""
                        and x["user_name"] is not None and x["user_name"] != ""
        )

        df_student_contact = dyf_student_contact.toDF()
        df_student_contact = df_student_contact.drop_duplicates(["student_id", "contact_id", "user_name"])
        df_result = df_voxy.join(df_student_contact, (df_voxy["email"]).endswith(df_student_contact["user_name"]))

        df_result = df_result \
            .withColumn("transformed_at", f.lit(d4)) \
            .withColumn("class_type", f.lit("VOXY")) \
 \
                prinDev(df_result)
        df_result = set_package_advisor_level(
            df_result,
            df_student_level=df_student_level,
            df_student_package=df_student_package,
            df_student_advisor=df_student_advisor)

        convertAndSaveS3(df_result)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF("flag")
        print('flag_data : ---')
        df.show()
        # ghi de _key vao s3
        df.write.parquet(FLAG_VOXY_SAVE, mode="overwrite")


def etl_ncsbasic(df_student_level=None, df_student_package=None, df_student_advisor=None):
    dyf_results = connectGlue(database="ncsbasic", table_name="results_cutoff_2020",
                              select_fields=["_key", "user_id", "time_created", "time_end"],
                              )

    dyf_results = Filter.apply(
        frame=dyf_results,
        f=lambda x: x["time_created"] is not None and x["time_created"] != ""
                    and x["time_end"] is not None and x["time_end"] != ""
                    and x["user_id"] is not None and x["user_id"] != "")

    dyf_results = dyf_results.resolveChoice(specs=[("time_created", "cast:long")])
    dyf_results = dyf_results.resolveChoice(specs=[("time_end", "cast:long")])
    dyf_results = dyf_results.resolveChoice(specs=[("_key", "cast:long")])

    if not is_load_full:
        dyf_results = filter_flag(dyf_results, FLAG_NCSB_FILE)

    if dyf_results.count() > 0:
        df_results = dyf_results.toDF()
        flag = df_results.agg({"_key": "max"}).collect()[0][0]
        df_results = df_results.drop_duplicates(["user_id", "time_created"])
        df_results = df_results.withColumn("learning_date",
                                           f.from_unixtime('time_created', format="yyyy-MM-dd").cast("string")) \
            .withColumn('total_duration', f.round(f.col('time_end') - f.col('time_created')).cast('long'))
        df_results = df_results \
            .groupby("user_id", "learning_date") \
            .agg(f.count("time_end").alias("total"),
                 f.sum('total_duration').alias('total_duration')
                 )
        dyf_native_talk_account_mapping = connectGlue(database="ncsbasic", table_name="users",
                                                      select_fields=["_id", "email"],
                                                      )
        dyf_native_talk_account_mapping = Filter.apply(
            frame=dyf_native_talk_account_mapping,
            f=lambda x: x["_id"] is not None and x["_id"] != ""
                        and x["email"] is not None and x["email"] != "")

        df_native_talk_account_mapping = dyf_native_talk_account_mapping.toDF()
        df_native_talk_account_mapping = df_native_talk_account_mapping.drop_duplicates(["_id"])

        df_join = df_results.join(df_native_talk_account_mapping,
                                  df_results.user_id == df_native_talk_account_mapping._id)

        dyf_student_contact = connectGlue(
            database="tig_advisor",
            table_name="student_contact",
            select_fields=["student_id", "contact_id", "user_name"],
        )
        dyf_student_contact = Filter.apply(
            frame=dyf_student_contact,
            f=lambda x: x["student_id"] is not None and x["student_id"] != ""
                        and x["contact_id"] is not None and x["contact_id"] != ""
                        and x["user_name"] is not None and x["user_name"] != "")

        df_student_contact = dyf_student_contact.toDF()
        df_student_contact = df_student_contact.drop_duplicates(["student_id", "contact_id", "user_name"])

        df_result = df_join.join(df_student_contact, df_join["email"] == df_student_contact["user_name"])

        df_result = df_result.filter(df_result.total > 0)

        df_result = df_result \
            .withColumn("transformed_at", f.lit(d4)) \
            .withColumn("class_type", f.lit("NCSBASIC")) \
 \
                prinDev(df_result)
        # df_result = set_package_advisor_level(df_result, df_student_level=df_student_level,
        #                                       df_student_package=df_student_package,
        #                                       df_student_advisor=df_student_advisor)
        convertAndSaveS3(df_result)

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF("flag")
        # ghi de _key vao s3
        df.write.parquet(FLAG_NCSB_SAVE, mode="overwrite")


def etl_home_work(df_student_level=None, df_student_package=None, df_student_advisor=None):
    dyf_mdl_le_exam_attemp = connectGlue(database="home_work_basic_production", table_name="mdl_le_exam_attemp",
                                         select_fields=["_key", "user_id", "created_at", "le_attemp_id", 'le_id',
                                                        'point'],
                                         )

    dyf_mdl_le_exam_attemp = Filter.apply(
        frame=dyf_mdl_le_exam_attemp,
        f=lambda x: x["user_id"] is not None and x["user_id"] != ""
                    and x["le_attemp_id"] is not None and x["le_attemp_id"] != ""
                    and x["created_at"] is not None and x["created_at"] != ""
                    and x["created_at"] is not None
    )

    dyf_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.resolveChoice(specs=[("_key", "cast:long")])

    if is_dev:
        print('dyf_mdl_le_exam_attemp')
        dyf_mdl_le_exam_attemp.printSchema()
        dyf_mdl_le_exam_attemp.show(3)

    if not is_load_full:
        dyf_mdl_le_exam_attemp = filter_flag(dyf_mdl_le_exam_attemp, FLAG_HW_FILE)

    number_dyf_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.count()
    if number_dyf_mdl_le_exam_attemp < 1:
        return

    df_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.toDF()

    flag = df_mdl_le_exam_attemp.agg({"_key": "max"}).collect()[0][0]

    df_mdl_le_exam_attemp = df_mdl_le_exam_attemp.drop_duplicates(["user_id", "created_at"])

    df_mdl_le_exam_attemp = df_mdl_le_exam_attemp \
        .select('user_id',
                'le_attemp_id',
                'le_id',
                'point',
                f.unix_timestamp(df_mdl_le_exam_attemp.created_at, "yyyy-MM-dd HH:mm:ss").cast("long").alias(
                    "created_at")
                )

    if is_dev:
        print('df_mdl_le_exam_attemp')
        df_mdl_le_exam_attemp.printSchema()
        df_mdl_le_exam_attemp.show(3)

    # ------------------------------------------------------------------------------------------------------------------#
    dyf_mdl_le_attemp = connectGlue(database="home_work_basic_production", table_name="mdl_le_attemp",
                                    select_fields=["id", "created_at"],
                                    )
    dyf_mdl_le_attemp = Filter.apply(
        frame=dyf_mdl_le_attemp,
        f=lambda x: x["id"] is not None and x["id"] != ""
                    and x["created_at"] is not None and x["created_at"] != "")

    df_mdl_le_attemp = dyf_mdl_le_attemp.toDF()
    df_mdl_le_attemp = df_mdl_le_attemp.drop_duplicates(["id"])
    df_mdl_le_attemp = df_mdl_le_attemp.select("id", f.unix_timestamp(df_mdl_le_attemp.created_at,
                                                                      "yyyy-MM-dd HH:mm:ss").cast("long")
                                               .alias("created_at_le"))

    # ------------------------------------------------------------------------------------------------------------------#

    df_mdl_le_exam_attemp_detail = df_mdl_le_exam_attemp \
        .join(other=df_mdl_le_attemp,
              on=df_mdl_le_exam_attemp.le_attemp_id == df_mdl_le_attemp.id,
              how='inner'
              )

    df_mdl_le_exam_attemp_detail = df_mdl_le_exam_attemp_detail \
        .select(
        'user_id',
        'le_attemp_id',
        'le_id',
        'created_at',
        'point',
        (df_mdl_le_exam_attemp_detail.created_at - df_mdl_le_exam_attemp_detail.created_at_le).alias(
            'learning_duration'),
        f.from_unixtime(f.col('created_at'), "yyyy-MM-dd").cast("string").alias("learning_date")
    )

    if is_dev:
        print('df_mdl_le_exam_attemp_detail')
        df_mdl_le_exam_attemp_detail.printSchema()
        df_mdl_le_exam_attemp_detail.show(3)

    # ----------------get learning turn by le_id

    # ("student_id", "string", "student_id", "long"),
    # ("contact_id", "string", "contact_id", "string"),
    #
    # ("class_type", "string", "class_type", "string"),
    # ("learning_date", "string", "learning_date", "string"),
    # ("total", "long", "total_learing", "long"),
    # ("total_duration", "long", "total_duration", "long"),
    #
    # ("year_month_id", "string", "year_month_id", "string"),
    # ("transformed_at", "string", "transformed_at", "long")]

    df_mdl_le_exam_attemp_detail = df_mdl_le_exam_attemp_detail \
        .orderBy(f.asc('user_id'), f.asc('le_id'), f.asc('learning_date'), f.asc('created_at'))

    df_mdl_le_exam_attemp_learning_turn = df_mdl_le_exam_attemp_detail \
        .groupBy('user_id', 'le_id', 'learning_date') \
        .agg(
        f.first('created_at').alias('created_at'),
        f.first('learning_duration').alias('learning_duration'),
        f.first('point').alias('point')
    )

    df_mdl_le_exam_attemp_learning_turn = df_mdl_le_exam_attemp_learning_turn \
        .select("user_id",
                "created_at",
                'learning_duration',
                'point',
                'le_id',
                'learning_date',
                f.from_unixtime(f.col('created_at'), "yyyy-MM-dd HH:mm:ss").cast("string").alias("learning_date_time")
                )

    df_mdl_le_exam_attemp_learning_turn_success = df_mdl_le_exam_attemp_learning_turn \
        .filter(f.col('learning_duration') >= 600)

    df_learning_turn_success_total = df_mdl_le_exam_attemp_learning_turn_success \
        .groupby("user_id", "learning_date") \
        .agg(f.count("created_at").cast('long').alias("total"),
             f.sum('learning_duration').cast('long').alias('total_duration')
             )

    if is_dev:
        print('df_learning_turn_success_total')
        df_learning_turn_success_total.printSchema()
        df_learning_turn_success_total.show(3)

    dyf_mdl_user = connectGlue(database="home_work_basic_production", table_name="mdl_user",
                               select_fields=["id", "username"],
                               ).rename_field("username", "email")
    dyf_mdl_user = Filter.apply(
        frame=dyf_mdl_user,
        f=lambda x: x["id"] is not None and x["id"] != ""
                    and x["email"] is not None and x["email"] != "")

    df_mdl_user = dyf_mdl_user.toDF()
    df_mdl_user = df_mdl_user.drop_duplicates(["id"])

    dyf_student_contact = connectGlue(
        database="tig_advisor",
        table_name="student_contact",
        select_fields=["student_id", "contact_id", "user_name"],
    )

    # ------------------------------------------------------------------------------------------------------------------#

    dyf_student_contact = Filter.apply(
        frame=dyf_student_contact,
        f=lambda x: x["student_id"] is not None and x["student_id"] != ""
                    and x["contact_id"] is not None and x["contact_id"] != ""
                    and x["user_name"] is not None and x["user_name"] != "")
    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.drop_duplicates(["student_id", "contact_id", "user_name"])

    # ------------------------------------------------------------------------------------------------------------------#

    df_join = df_learning_turn_success_total \
        .join(other=df_mdl_user,
              on=df_mdl_user.id == df_mdl_le_exam_attemp.user_id,
              how='inner') \
        .join(other=df_student_contact,
              on=df_mdl_user.email == df_student_contact.user_name,
              how='inner')

    df_result = df_join \
        .withColumn("transformed_at", f.lit(d4)) \
        .withColumn("class_type", f.lit("HOME_WORK"))

    convertAndSaveS3(df_result)
    flag_data = [flag]
    df = spark.createDataFrame(flag_data, "long").toDF("flag")
    # ghi de _key vao s3
    df.write.parquet(FLAG_HW_SAVE, mode="overwrite")


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


def set_package_advisor_level(df, df_student_level=None, df_student_package=None, df_student_advisor=None):
    if df_student_package is not None:
        df = df.join(df_student_level, (df["student_behavior_date"] > df_student_level["start_date"])
                     & (df["student_behavior_date"] <= df_student_level["end_date"])
                     & ((df["contact_id"] == df_student_level["contact_id_level"])
                        | (df["student_id"] == df_student_level["student_id_level"])), "left")

        df = df.drop("student_id_level", "contact_id_level", "end_date", "start_date")

    if df_student_package is not None:
        df = df.join(df_student_package, (df["student_behavior_date"] > df_student_package["package_start_time"])
                     & (df["student_behavior_date"] <= df_student_package["package_end_time"])
                     & ((df["contact_id"] == df_student_package["contact_id_package"])
                        | (df["student_id"] == df_student_package["student_id_package"])), "left")
        df = df.drop("student_id_package", "contact_id_package", "package_end_time", "package_start_time")

    if df_student_advisor is not None:
        df = df.join(df_student_advisor, (df["student_behavior_date"] > df_student_advisor["start_date"])
                     & (df["student_behavior_date"] <= df_student_advisor["end_date"])
                     & (df["contact_id"] == df_student_advisor["contact_id_advisor"]), "left")

        df = df.drop("contact_id_advisor", "start_date", "end_date")

    return df


def convertAndSaveS3(df):
    df = df.withColumn("year_month_id",
                       f.from_unixtime(
                           f.unix_timestamp(
                               df.learning_date, "yyyy-MM-dd").cast("long"), "yyyyMM").cast("string"))

    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

    behavior_mapping = mappingForAll(dyf, MAPPING)

    if behavior_mapping.count() > 0:
        parquetToS3(dyf=behavior_mapping, path="s3://toxd-olap/trasaction_le2/le2_history")


def prinDev(df, df_name="print full information"):
    if is_dev:
        df.printSchema()
        df.show(3)


if __name__ == "__main__":
    main()

job.commit()