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

    # print('dyf_crm_contacts::schema')
    # dyf_crm_contacts.printSchema()
    dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('_key', 'cast:long')])
    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/user_profile/communication/phone_crm_contact.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                        f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')

    dyf_crm_contacts = dyf_crm_contacts.select_fields(['_key', 'Id', 'Code'])
    dy_source_voxy_cache = dyf_crm_contacts.toDF()
    dy_source_voxy_cache = dy_source_voxy_cache.cache()
    dyf_crm_contacts = DynamicFrame.fromDF(dy_source_voxy_cache, glueContext, 'dyf_crm_contacts')

    today = date.today()
    d4 = today.strftime("%Y-%m-%d")
    # print("d4 =", d4)

    print('the number of new contacts: ', dyf_crm_contacts.count())

    if (dyf_crm_contacts.count() > 0):



        # print('Chay vao day nhe------------------')
        # print('dyf_crm_contacts::----------------')
        # dyf_crm_contacts.printSchema()
        # try:
        #--------------------------------------------------------------------------------------------------------------#
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                              f=lambda x: x["Id"] is not None and x["Id"] != ''
                                                          and x["Code"] is not None and x["Code"] != '')
        # --------------------------------------------------------------------------------------------------------------#
        dyf_crm_phones = glueContext.create_dynamic_frame.from_catalog(database='crm_native',
                                                                         table_name='phones')

        # print('phones::schema')
        # dyf_crm_phones.printSchema()
        # dyf_crm_phones.show(3)

        dyf_crm_phones = dyf_crm_phones.select_fields(['ContactId', 'PhoneNumber'])
        dyf_crm_phones = Filter.apply(frame=dyf_crm_phones,
                                        f=lambda x: x["ContactId"] is not None and x["ContactId"] != ''
                                                    and x["PhoneNumber"] is not None and x["PhoneNumber"] != '')

        # --------------------------------------------------------------------------------------------------------------#

        # --------------------------------------------------------------------------------------------------------------#
        dyf_crm_jon_contact_phones = Join.apply(dyf_crm_contacts, dyf_crm_phones, 'Id', 'ContactId')
        # dyf_crm_jon_contact_phones = Filter.apply(frame=dyf_crm_jon_contact_phones,
        #                               f=lambda x: x["Id"] is not None and x["Id"] != ''
        #                                           and x["ContactId"] is not None and x["ContactId"] != '')

        # --------------------------------------------------------------------------------------------------------------#

        dy_crm_jon_contact_phones = dyf_crm_jon_contact_phones.toDF()
        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.dropDuplicates()
        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.withColumn('communication_type', f.lit(1))
        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.withColumn('is_primary', f.lit(0))
        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.withColumn('is_deleted', f.lit(0))
        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.withColumn('last_update_date', f.lit(d4))
        dy_crm_jon_contact_phones = DynamicFrame.fromDF(dy_crm_jon_contact_phones, glueContext, 'dy_crm_jon_contact_phones')

        dy_crm_jon_contact_phones = dy_crm_jon_contact_phones.resolveChoice(specs=[('last_update_date', 'cast:string')])


        applymapping2 = ApplyMapping.apply(frame=dy_crm_jon_contact_phones,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("communication_type", 'int', 'communication_type', 'int'),
                                                     ("is_primary", 'int', 'is_primary', 'int'),
                                                     ("is_deleted", 'int', 'is_deleted', 'int'),
                                                     ("PhoneNumber", 'string', 'comunication', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])

        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        # print('dropnullfields6::schema')
        # dropnullfields6.printSchema()
        # dropnullfields6.show(5)

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "user_communication",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/communication/phone/",
                                                                   transformation_ctx="datasink4")
        df_data_source = dyf_crm_contacts.toDF()
        flag = df_data_source.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://dts-odin/flag/student_status/user_profile/communication/phone_crm_contact.parquet", mode="overwrite")


if __name__ == "__main__":
    main()