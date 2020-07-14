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

    dyf_crm_contacts = dyf_crm_contacts.select_fields(['_key', 'Id', 'Code', 'Fullname', 'Address'])
    dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('_key', 'cast:long')])

    dy_source_voxy_cache = dyf_crm_contacts.toDF()
    dy_source_voxy_cache = dy_source_voxy_cache.cache()
    dyf_crm_contacts = DynamicFrame.fromDF(dy_source_voxy_cache, glueContext, 'dyf_crm_contacts')

    # try:
    #     df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_user_communication_full_name.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
    #                                     f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')

    print('the number of new contacts: ', dyf_crm_contacts.count())

    if (dyf_crm_contacts.count() > 0):

        # print('Chay vao day nhe------------------')
        # print('dyf_crm_contacts::----------------')
        # dyf_crm_contacts.printSchema()
        # try:
        #--------------------------------------------------------------------------------------------------------------#
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                              f=lambda x: x["Id"] is not None and x["Id"] != ''
                                                          and x["Code"] is not None and x["Code"] != ''
                                                          and x["Fullname"] is not None and x["Fullname"] != '')
        # --------------------------------------------------------------------------------------------------------------#

        # --------------------------------------------------------------------------------------------------------------#
        # today = date.today()
        # today_timestamp = today.timestamp();
        # print("Today's date:", today_timestamp)

        dy_crm_contacts = dyf_crm_contacts.toDF()
        dy_crm_contacts = dy_crm_contacts.dropDuplicates(['Code'])
        dy_crm_contacts = dy_crm_contacts.withColumn('communication_type_full_name', f.lit(4))
        dy_crm_contacts = dy_crm_contacts.withColumn('communication_type_address', f.lit(6))
        dy_crm_contacts = dy_crm_contacts.withColumn('is_primary', f.lit(1))
        dy_crm_contacts = dy_crm_contacts.withColumn('is_deleted', f.lit(0))
        dy_crm_contacts = dy_crm_contacts.withColumn('last_update_date', f.lit('2019-08-28 00:00:00'))
        dyf_crm_contacts = DynamicFrame.fromDF(dy_crm_contacts, glueContext, 'dyf_crm_contacts')

        dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('last_update_date', 'cast:long')])


        applymapping2 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("communication_type_full_name", 'int', 'communication_type', 'int'),
                                                     ("is_primary", 'int', 'is_primary', 'int'),
                                                     ("is_deleted", 'int', 'is_deleted', 'int'),
                                                     ("Fullname", 'string', 'comunication', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])

        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "user_communication",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/communication/fullname/",
                                                                   transformation_ctx="datasink4")

        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                        f=lambda x: x["Address"] is not None and x["Address"] != '')
        #--------------------------------------------------------------------------------------------------------------#
        applymapping3 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("communication_type_address", 'int', 'communication_type', 'int'),
                                                     ("is_primary", 'int', 'is_primary', 'int'),
                                                     ("is_deleted", 'int', 'is_deleted', 'int'),
                                                     ("Address", 'string', 'comunication', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])
        #
        #
        resolvechoice3 = ResolveChoice.apply(frame=applymapping3, choice="make_cols",
                                             transformation_ctx="resolvechoice3")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice3, transformation_ctx="dropnullfields3")

        datasink3 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "user_communication",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/communication/address/",
                                                                   transformation_ctx="datasink3")
        # --------------------------------------------------------------------------------------------------------------#

        #insert into source_id

        # lay max _key tren datasource
        datasource = dyf_crm_contacts.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        # ghi de flag moi vao s3
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        df.write.parquet("s3a://dts-odin/flag/flag_user_communication_full_name.parquet", mode="overwrite")



if __name__ == "__main__":
    main()