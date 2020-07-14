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
    is_dev=True
    limit=True
    # information database???????
    dyf_crm_contacts = glueContext.create_dynamic_frame.from_catalog(database='crm_native',
                                                                      table_name='contacts')

    # dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
    #                                 f=lambda x: x["Id"] < 1102)

    # print('dyf_crm_contacts::fdfdfdfdfdfdfd----------------')
    # dyf_crm_contacts.printSchema()
    dyf_crm_contacts = dyf_crm_contacts.resolveChoice(specs=[('Id', 'cast:int')])

    print('dyf_crm_contacts')
    dyf_crm_contacts.printSchema()

    #doc moc flag tu s3
    try:
        df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_user_profile.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                        f=lambda x: x["Id"] > read_from_index)
    except:
        print('read flag file error ')
    print('the number of new contacts: ', dyf_crm_contacts.count())

    crm_contacts_number = dyf_crm_contacts.count()
    print('crm_contacts_number: ', crm_contacts_number)
    if crm_contacts_number < 1:
        print('Stopping--- crm_contacts_number < 1')
        return

    dyf_crm_contacts = dyf_crm_contacts.select_fields(['_key', 'Id', 'Code', 'Birthday', 'Gender', 'Job', 'CreatedDate'])
    dy_crm_contacts_cache = dyf_crm_contacts.toDF()
    dy_crm_contacts_cache = dy_crm_contacts_cache.dropDuplicates(['Code'])
    dy_crm_contacts_cache = dy_crm_contacts_cache.cache()
    dyf_crm_contacts = DynamicFrame.fromDF(dy_crm_contacts_cache, glueContext, 'dyf_crm_contacts')

    today = date.today()
    d4 = today.strftime("%Y-%m-%d")
    print("d4 =", d4)




    # print('Chay vao day nhe------------------')
    # print('dyf_crm_contacts::----------------')
    # dyf_crm_contacts.printSchema()
    # try:
    #--------------------------------------------------------------------------------------------------------------#
    dyf_crm_contacts = Filter.apply(frame=dyf_crm_contacts,
                                          f=lambda x: x["Id"] is not None and x["Id"] != ''
                                                      and x["Code"] is not None and x["Code"] != '')
    # --------------------------------------------------------------------------------------------------------------#

    # --------------------------------------------------------------------------------------------------------------#
    if(dyf_crm_contacts.count() > 0):
        dy_crm_contacts = dyf_crm_contacts.toDF()
        # dy_crm_contacts = dy_crm_contacts.dropDuplicates(['Code'])
        dy_crm_contacts = dy_crm_contacts.withColumn('source_type', f.lit(1))
        dy_crm_contacts = dy_crm_contacts.withColumn('is_root', f.lit(1))
        dy_crm_contacts=dy_crm_contacts.withColumn('description', f.lit(d4))
        dy_crm_contacts = dy_crm_contacts.withColumn('last_update_date', f.lit(d4))
        dy_crm_contacts_cache_2 = dy_crm_contacts.cache()
        dyf_crm_contacts = DynamicFrame.fromDF(dy_crm_contacts_cache_2, glueContext, 'dyf_crm_contacts')


        applymapping2 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("Gender", 'int', 'gender', 'string'),
                                                     ("is_root", 'int', 'is_root', 'int'),
                                                     ("Birthday", 'string', 'birthday', 'date'),
                                                     ("Job", 'string', 'job', 'string'),
                                                     ("last_update_date", 'string', 'last_update_date', 'timestamp')])
        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "user_profile",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/user/profile/",
                                                                   transformation_ctx="datasink4")

        #insert into source_id


        # print('dyf_crm_contacts::-------source_type---------')
        # dyf_crm_contacts.printSchema()

        applymapping3 = ApplyMapping.apply(frame=dyf_crm_contacts,
                                           mappings=[("Id", "int", "user_id", "bigint"),
                                                     ("source_type", 'int', 'source_type', 'int'),
                                                     ("Code", 'string', 'source_id', 'string'),
                                                     ("description", 'string', 'description', 'string')])

        resolvechoice3 = ResolveChoice.apply(frame=applymapping3, choice="make_cols",
                                             transformation_ctx="resolvechoice3")
        dropnullfields7 = DropNullFields.apply(frame=resolvechoice3, transformation_ctx="resolvechoice3")


        # datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields7,
        #                                                            catalog_connection="glue_redshift",
        #                                                            connection_options={"dbtable": "user_map",
        #                                                                                "database": "dts_odin"},
        #                                                            redshift_tmp_dir="s3n://dts-odin/temp/user/map/",
        #                                                            transformation_ctx="datasink5")

        #lay max _key tren datasource
        flag = dy_crm_contacts_cache.agg({"Id": "max"}).collect()[0][0]

        # ghi de flag moi vao s3
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "int").toDF('flag')

        df.write.parquet("s3a://dtsodin/flag/flag_user_profile.parquet", mode="overwrite")
        dy_crm_contacts_cache_2.unpersist()
        dy_crm_contacts_cache.unpersist()

if __name__ == "__main__":
    main()