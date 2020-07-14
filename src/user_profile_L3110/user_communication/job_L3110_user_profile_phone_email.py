import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime,unix_timestamp,date_format
import pyspark.sql.functions as f
from datetime import date

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source
    dyf_crm_contacts = glueContext.create_dynamic_frame.from_catalog(database='crm_native',
                                                                      table_name='contacts')

    print('dyf_crm_contacts::schema')
    dyf_crm_contacts.printSchema()
    dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('_key', 'cast:long')])
    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/user_profile/communication/email.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                        f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')

    dyf_crm_contacts = dyf_crm_contacts.select_fields(['_key', 'Id', 'Email', 'Email2'])
    dy_source_voxy_cache = dyf_crm_contacts.toDF()
    dy_source_voxy_cache = dy_source_voxy_cache.cache()
    dyf_crm_contacts = DynamicFrame.fromDF(dy_source_voxy_cache, glueContext, 'dyf_crm_contacts')

    today = date.today()
    d4 = today.strftime("%Y-%m-%d")
    print("d4 =", d4)

    print('the number of new contacts: ', dyf_crm_contacts.count())

    if (dyf_crm_contacts.count() > 0):

        # print('Chay vao day nhe------------------')
        # print('dyf_crm_contacts::----------------')
        # dyf_crm_contacts.printSchema()
        # try:
        #--------------------------------------------------------------------------------------------------------------#
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                              f=lambda x: x["Id"] is not None and x["Id"] != ''
                                                          and x["Email"] is not None and x["Email"] != '')
        # --------------------------------------------------------------------------------------------------------------#

        # --------------------------------------------------------------------------------------------------------------#

        dy_crm_contacts = dyf_crm_contacts.toDF()
        dy_crm_contacts = dy_crm_contacts.withColumn('communication_type', f.lit(2))
        dy_crm_contacts = dy_crm_contacts.withColumn('is_primary', f.lit(0))
        dy_crm_contacts = dy_crm_contacts.withColumn('is_deleted', f.lit(0))
        dy_crm_contacts = dy_crm_contacts.withColumn('last_update_date', f.lit(d4))
        dyf_crm_contacts = DynamicFrame.fromDF(dy_crm_contacts, glueContext, 'dyf_crm_contacts')

        dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('last_update_date', 'cast:string')])


        applymapping2 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("communication_type", 'int', 'communication_type', 'int'),
                                                     ("is_primary", 'int', 'is_primary', 'int'),
                                                     ("is_deleted", 'int', 'is_deleted', 'int'),
                                                     ("Email", 'string', 'comunication', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])

        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        print('dropnullfields6::schema')
        dropnullfields6.printSchema()
        dropnullfields6.show(5)

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "user_communication",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/communication/fullname/",
                                                                   transformation_ctx="datasink4")

        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                        f=lambda x: x["Email2"] is not None and x["Email2"] != '')

        applymapping2 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("communication_type", 'int', 'communication_type', 'int'),
                                                     ("is_primary", 'int', 'is_primary', 'int'),
                                                     ("is_deleted", 'int', 'is_deleted', 'int'),
                                                     ("Email2", 'string', 'comunication', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])

        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "user_communication",
                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/communication/fullname/",
                                                                   transformation_ctx="datasink4")

        df_datasource = dyf_crm_contacts.toDF()
        flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://dts-odin/flag/student_status/user_profile/communication/email.parquet", mode="overwrite")


if __name__ == "__main__":
    main()