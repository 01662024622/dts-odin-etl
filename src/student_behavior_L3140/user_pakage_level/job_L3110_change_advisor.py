import sys
import pyspark.sql.functions as f

from pyspark import SparkContext, Row
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, ArrayType

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
is_dev = True
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")

ADVISOR_MAPPING= [("user_id", "int", "advisor_id", "bigint"),\
                ("user_name", "string", "user_name", "string"),\
                ("user_display_name", "string", "name", "string"),\
                ("user_email", "string", "email", "string"),\
                ("level", "int", "level", "int"),\
                ("type_manager", "string", "advisor_type", "string")]

STUDENT_ADVISOR_MAPPING=[("id", "string", "student_advisor_id", "string"),\
                ("contact_id", "string", "contact_id", "string"),\
                ("advisor_id_new", "string", "advisor_id", "string"),\
                ("start", "string", "start_date", "string"),\
                ("end", "string", "end_date", "string"),\
                ("created_at", "string", "created_at", "int"),\
                ("updated_at", "string", "updated_at", "string")]

STUDENT_ADVISOR_ADD_COL={'created_at': f.lit(d4),
                   'updated_at': f.lit('')}

MIN_DATE = "946663200"
MAX_DATE = "99999999999"

start_end_date_struct = StructType([
                    StructField("advisor_id", StringType()),
                    StructField("start", StringType()),
                    StructField("end", StringType())])
# ------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="", table_name="", select_fields=[], fillter=[], cache=False, duplicates=[]):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database,
                                                        table_name=table_name)

    if is_dev:
        print("full schema of table: ", database, "and table: ", table_name)
        dyf.printSchema()
    #     select felds
    if len(select_fields) != 0:
        dyf = dyf.select_fields(select_fields)

    if len(fillter) != 0:
        dyf = fillterOutNull(dyf, fillter)
    else:
        dyf = fillterOutNull(dyf, select_fields)

    if is_limit and cache:
        df = dyf.toDF()
        df = df.limit(500)
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
        df = df.limit(500)
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
                                                     "path": path
                                                 },
                                                 format=format,
                                                 transformation_ctx="datasink6")


# -----------------------------------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame


# -----------------------------------------------------------------------------------------------------------------------#
def myFunc(data_list):
    for val in data_list:
        if val is not None and val != "":
            return val
    return None

# -----------------------------------------------------------------------------------------------------------------------#
UNAVALIABLE_ADVISOR_ID = -1
unavaliable_item = Row("advisor_id_new", "id", "start","end")(UNAVALIABLE_ADVISOR_ID, 1, MIN_DATE, MIN_DATE)
def transpose_start_end_date_function(data_list):
    len_data = len(data_list)
    result = []
    if len_data == 0:
        result.append(unavaliable_item)
        return result

    # first_item = Row("advisor_id", "start", "end")(data_list[0][0], MIN_DATE, data_list[0][1])
    # result.append(first_item)

    for i in range(0 ,(len_data-1)):
        start_data = data_list[i]
        end_data = data_list[i+1]
        start_end_row = Row("advisor_id", "start", "end")(start_data[0], start_data[1], end_data[1])
        result.append(start_end_row)
    final= data_list[len_data-1]
    final_fix_end=Row("advisor_id", "start", "end")(
        final[0],
        final[1],
        MAX_DATE
    )
    result.append(final_fix_end)
    return result

transpose_start_end_udf = f.udf(transpose_start_end_date_function, ArrayType(start_end_date_struct))

# -----------------------------------------------------------------------------------------------------------------------#
def addCollum(df,add_col):
    if len(add_col)!=0:
        for k, v in add_col.items():
            df = df.withColumn(k, v)
    return df
# -----------------------------------------------------------------------------------------------------------------------#
def mappingForAll(dynamicFrame, mapping):
    df = dynamicFrame.toDF()
    df = df.dropDuplicates()
    dyf= DynamicFrame.fromDF(df, glueContext, "dyf")
    print("-------------------------------------------------")
    dyf.printSchema()
    print(mapping)

    applymapping2 = ApplyMapping.apply(frame=dyf, mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_mapping


# -------------------------------------------------------Main-----------------------------------------------------------#




def etl_advisor():
    dyf_advisor_account = connectGlue(database="tig_advisor", table_name="advisor_account",
                                      select_fields=["user_id", "user_name", "user_display_name", "user_email", "level",
                                                     "type_manager"],
                                      fillter=["user_id", "type_manager"])

    dyf_advisor_account.show(10)
    # ------------------------------------------------------------------------------------------------------------------#
    number_advisor_account = dyf_advisor_account.count()

    if is_dev:
        print ("number_advisor_account: ", number_advisor_account)
    if number_advisor_account < 1:
        return
    print_is_dev(
        "etl_advisor_profile___****************************************************************************************")
    # ETL for profile
    myUdf = f.udf(myFunc, StringType())

    df_advisor_account = dyf_advisor_account.toDF()
    df_advisor_account = df_advisor_account.groupBy("user_id") \
        .agg(f.collect_list("user_name").alias("user_name"),
             f.collect_list("user_email").alias("user_email"),
             f.first("level").alias("level"),
             f.first("type_manager").alias("type_manager"),
             f.collect_list("user_display_name").alias("user_display_name")) \
        .withColumn("user_name", myUdf("user_name")) \
        .withColumn("user_email", myUdf("user_email")) \
        .withColumn("user_display_name", myUdf("user_display_name"))
    dyf_advisor_account = DynamicFrame.fromDF(df_advisor_account, glueContext, "dyf_advisor_account")
    dyf_advisor_account_mapping = mappingForAll(dyf_advisor_account, mapping = ADVISOR_MAPPING)

    dyf_advisor_account_mapping.printSchema()
    dyf_advisor_account_mapping.show(10)
    ########  save to s3######
    # parquetToS3(dyf_advisor_account_mapping, path="s3://dtsodin/student_behavior/advisor_thangvm/advisor_history")
    ############################################################
    # ------------------------------------------------------------------------------------------------------------------#

def get_df_change_advisor_new(df_student_advisor):
    df_student_advisor_new = df_student_advisor.select(
        'contact_id',
        df_student_advisor.advisor_id_new.alias('advisor_id'),
        'created_at'
    )
    return df_student_advisor_new


# root
# |-- created_at: string
# |-- updated_at: string
# |-- advisor_id_new: string
# |-- contact_id: string
# |-- id: string




def get_df_change_advisor_init(df_student_advisor):
    df_student_advisor_new = df_student_advisor.select(
        'contact_id',
        df_student_advisor.advisor_id_old.alias('advisor_id')
    )

    df_student_advisor_new = df_student_advisor_new.orderBy(f.asc('contact_id'), f.asc('created_at'))
    df_student_advisor_first = df_student_advisor_new.groupBy('contact_id').agg(
        f.first('advisor_id').alias('advisor_id'),
        f.lit(MIN_DATE).alias('created_at')
    )

    df_student_advisor_first = df_student_advisor_first\
        .filter(df_student_advisor_first.advisor_id.isNotNull())

    return df_student_advisor_first



def get_df_student_contact(glueContext):
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id', 'advisor_id'])

    # dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x['student_id'] is not None
                                                   and x['contact_id'] is not None
                                                   and x['advisor_id'] is not None and x['advisor_id'] != '')

    df_student_contact = dyf_student_contact.toDF()
    return df_student_contact

def get_advisor_from_student_contact(glueContext, df_contact_list):
    df_student_contact = get_df_student_contact(glueContext)

    df_student_contact = df_student_contact.join(df_contact_list, 'contact_id', 'left_anti')
    print('get_current_level_student_contact')
    df_student_contact.printSchema()
    df_student_contact.show(3)

    df_student_contact_level = df_student_contact.select(
        'contact_id',
        'advisor_id',
        f.lit(MIN_DATE).alias('created_at')
    )

    return df_student_contact_level


def get_dyf_student_advisor(glueContext):
    dyf_student_advisor = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_change_assignment_advisor"
    )

    dyf_student_advisor = dyf_student_advisor.select_fields(
        ["id", "contact_id", "advisor_id_old", "advisor_id_new", "created_at", "updated_at"])

    dyf_student_advisor = Filter.apply(frame=dyf_student_advisor,
                                       f=lambda x: x['contact_id'] is not None and x['contact_id'] != ''
                                                   and x['advisor_id_new'] is not None and x['advisor_id_new'] != ''
                                                   and x['created_at'] is not None
                                       )

    df_student_advisor = dyf_student_advisor.toDF()
    return df_student_advisor


def etl_student_advisor():
    print_is_dev(
        "etl_advisor_profile___****************************************************************************************")
    # ETL for profile
    # dyf_student_advisor = connectGlue(database="tig_advisor", table_name="log_change_assignment_advisor",
    #     #                                   select_fields=["id", "contact_id", "advisor_id_old", "advisor_id_new","created_at", "updated_at"],
    #     #                                   fillter=["id"],
    #     #                                   duplicates=["id"])



    # ------------------------------------------------------------------------------------------------------------------#
    df_student_advisor = get_dyf_student_advisor(glueContext)

    number_student_advisor = df_student_advisor.count()
    if is_dev:
        print ("number_advisor_account: ", number_student_advisor)
        df_student_advisor.printSchema()
        df_student_advisor.show(3)

    if number_student_advisor < 1:
        return

    # ------------------------------------------------------------------------------------------------------------------#
    # process contact_id with advisor from log_change_assignment_advisor and student_contact

    df_contact_id_list = df_student_advisor.select(
        'contact_id'
    )

    df_contact_id_list = df_contact_id_list.dropDuplicates(['contact_id'])

    df_change_advisor_new = get_df_change_advisor_new(df_student_advisor)
    #df_change_advisor_init = get_df_change_advisor_init(df_student_advisor)

    #df_advisor_from_student_contact = get_advisor_from_student_contact(glueContext, df_contact_id_list)

    df_change_advisor_total = df_change_advisor_new
        # .union(df_change_advisor_init)
        #.union(df_advisor_from_student_contact)


    if is_dev:
        print ('df_change_advisor_total')
        df_change_advisor_total.printSchema()
        df_change_advisor_total.show(3)
    #-------------------------------------------------------------------------------------------------------------------#


    df_change_advisor_total = df_change_advisor_total\
        .withColumn('created_at',
                    f.unix_timestamp('created_at', format='yyyy-MM-dd HH:mm:ss'))

    df_change_advisor_total = df_change_advisor_total.orderBy("contact_id", "created_at")

    if is_dev:
        print('df_change_advisor_total_after_order')
        df_change_advisor_total.printSchema()
        df_change_advisor_total.show(3)

    df_change_advisor_total = df_change_advisor_total.select("contact_id",
                                                   f.struct('advisor_id',
                                                            'created_at')
                                                            .alias('end_start'))

    print('df_change_advisor_total')
    df_change_advisor_total.printSchema()
    df_change_advisor_total.show(3)

    df_change_advisor_group_contact = df_change_advisor_total.groupBy("contact_id") \
        .agg(f.collect_list("end_start").alias("l_created_at"))

    print('df_change_advisor_group_contact_______________')
    df_change_advisor_group_contact.printSchema()
    df_change_advisor_group_contact.show(3)

    df_change_advisor_group_contact = df_change_advisor_group_contact\
        .withColumn("l_created_at", transpose_start_end_udf("l_created_at"))

    df_change_advisor_group_contact.printSchema()
    df_change_advisor_explore = df_change_advisor_group_contact\
        .select("contact_id",
        f.explode("l_created_at").alias("start_end_transpose")
    )

    df_change_advisor_explore.printSchema()
    df_change_advisor_explore = df_change_advisor_explore.select(
        "contact_id",
        f.col("start_end_transpose").getItem("advisor_id").alias("advisor_id"),
        f.col("start_end_transpose").getItem("start").alias("start"),
        f.col("start_end_transpose").getItem("end").alias("end"))
    df_change_advisor_explore.printSchema()
    df_change_advisor_explore = addCollum(df_change_advisor_explore, STUDENT_ADVISOR_ADD_COL)
    dyf_change_advisor_explore = DynamicFrame.fromDF(df_change_advisor_explore, glueContext, "dyf_change_advisor_explore")
    # dyf_change_advisor_explore = mappingForAll(dyf_change_advisor_explore, mapping = STUDENT_ADVISOR_MAPPING)

    print('dyf_change_advisor_explore')
    dyf_change_advisor_explore.printSchema()
    dyf_change_advisor_explore.show(10)

    ########  save to s3######
    # parquetToS3(dyf_student_advisor_mapping, path="s3://dtsodin/student_behavior/advisor_thangvm/advisor_history")
    ############################################################
    # ------------------------------------------------------------------------------------------------------------------#

    # | -- student_advisor_id: string
    # | -- contact_id: string
    # | -- advisor_id: string
    # | -- start_date: string
    # | -- end_date: string
    # | -- created_at: int
    # | -- updated_at: string



    apply_ouput = ApplyMapping.apply(frame=dyf_change_advisor_explore,
                                     mappings=[("contact_id", "string", "contact_id", "string"),
                                               ("advisor_id", "string", "advisor_id", "long"),
                                               ("start", "string", "start_date", "long"),
                                               ("end", "string", "end_date", "long"),
                                               ("created_at", "int", "created_at", "long")
                                               ])
    #
    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                       catalog_connection="glue_redshift",
                                                       connection_options={
                                                           # "preactions": """TRUNCATE TABLE ad_student_advisor """,
                                                           "dbtable": "ad_student_advisor",
                                                           "database": "transaction_log"
                                                       },
                                                       redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package",
                                                       transformation_ctx="datasink4")


def main():
    # --------------------------------------------------------------------------------------------------------------#
    # etl_advisor()
    etl_student_advisor()

if __name__ == "__main__":
    main()