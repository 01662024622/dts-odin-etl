import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.types import StringType

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
d4 = today.strftime("%Y-%m-%d").replace('-', '')



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
def parquetToS3(dyf, path='default', format="parquet"):
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={
                                                     "path": path
                                                 },
                                                 format=format,
                                                 transformation_ctx="datasink6")


# -----------------------------------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != '')
    return dynamicFrame


# -----------------------------------------------------------------------------------------------------------------------#
def myFunc(data_list):
    for val in data_list:
        if val is not None and val != '':
            return val
    return None


# -----------------------------------------------------------------------------------------------------------------------#


def mappingForAll(dynamicFrame, mapping, add_collum):
    df_communication = dynamicFrame.toDF()
    df_communication = df_communication.dropDuplicates()

    for k, v in add_collum.items():
        df_communication = df_communication.withColumn(k, v)

    dyf_communication = DynamicFrame.fromDF(df_communication, glueContext, 'dyf_communication')
    applymapping2 = ApplyMapping.apply(frame=dyf_communication,
                                       mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_communication_after_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_communication_after_mapping


# -------------------------------------------------------Main-----------------------------------------------------------#

def main():
    # --------------------------------------------------------------------------------------------------------------#
    dyf_crm_contacts = connectGlue(database='crm_native', table_name='contacts',
                                   select_fields=['Id', 'Code', 'Fullname', 'Address', 'Email', 'Email2', 'Job',
                                                  'Birthday', 'Gender'],
                                   fillter=['Id', 'Code'],
                                   cache=True)

    dyf_crm_contacts.show(10)
    # ------------------------------------------------------------------------------------------------------------------#
    number_crm_concat = dyf_crm_contacts.count()

    if is_dev:
        print ('number_crm_concat: ', number_crm_concat)
    if number_crm_concat < 1:
        return

    # dyf_crm_concat = DynamicFrame.fromDF(df_source_cache, glueContext, 'dyf_crm_concats_name')
    # etl_user_profile(dyf_crm_contacts)
    # dyf_crm_concat = DynamicFrame.fromDF(df_source_cache, glueContext, 'dyf_crm_concats_name')
    # etl_user_map(dyf_crm_contacts)


def etl_user_profile(dyf_crm_concat):
    print_is_dev(
        'etl_user_profile___****************************************************************************************')
    # ETL for profile
    myUdf = f.udf(myFunc, StringType())

    df_crm_concat_pf = dyf_crm_concat.toDF()
    df_crm_concat_pf = df_crm_concat_pf.groupBy('Id') \
        .agg(f.collect_list('Gender').alias('Gender'),
             f.collect_list('Birthday').alias('Birthday'),
             f.collect_list('Job').alias('Job')) \
        .withColumn('Gender', myUdf('Gender')) \
        .withColumn('Birthday', myUdf('Birthday')) \
        .withColumn('Job', myUdf('Job'))
    dyf_crm_concat_pf = DynamicFrame.fromDF(df_crm_concat_pf, glueContext, 'dyf_crm_contacts')
    dyf_crm_concat_pf = mappingForAll(dyf_crm_concat_pf, profile, profile_add_collum)

    dyf_crm_concat_pf.printSchema()
    dyf_crm_concat_pf.show(10)
    ########  save to s3######
    parquetToS3(dyf_crm_concat_pf, path="s3://dtsodin/student_behavior/up/user_profile")
    ############################################################
    # ------------------------------------------------------------------------------------------------------------------#


if __name__ == "__main__":
    main()