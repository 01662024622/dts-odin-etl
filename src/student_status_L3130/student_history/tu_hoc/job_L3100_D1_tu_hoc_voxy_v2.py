import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
import pyspark.sql.functions as f

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source
    dyf_source_voxy = glueContext.create_dynamic_frame.from_catalog(database='dts_odin_tu_hoc', table_name='voxy_log_api')

    dyf_source_voxy = dyf_source_voxy.resolveChoice(specs=[('_key', 'cast:long')])
    # print schema and select fields
    print('original schema')
    dyf_source_voxy.printSchema()
    print('the number of new contacts: ', dyf_source_voxy.count())
    # try:
    #     df_flag = spark.read.parquet("s3a://dtsodin/flag/student_status/tu_hoc/tu_hoc_voxy_api.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     dyf_source_voxy = Filter.apply(frame=dyf_source_voxy,
    #                                   f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')
    print('the number of new contacts: ', dyf_source_voxy.count())

    dyf_source_voxy = dyf_source_voxy.select_fields(
        ['_key', 'id', 'user_id', 'email', 'time_created', 'total_activities_completed', 'total_hours_studied'])

    dyf_source_voxy = dyf_source_voxy.resolveChoice(
        specs=[('total_activities_completed', 'cast:long'), ('total_hours_studied', 'cast:double')])

    dy_source_voxy_cache = dyf_source_voxy.toDF()
    dy_source_voxy_cache = dy_source_voxy_cache.dropDuplicates(['id'])
    dy_source_voxy_cache = dy_source_voxy_cache.cache()
    dyf_source_voxy = DynamicFrame.fromDF(dy_source_voxy_cache, glueContext, 'dyf_source_voxy')

    print('dyf_source_voxy::number: ', dyf_source_voxy.count())

    if ( dyf_source_voxy.count() > 0 ):
        dyf_source_voxy = Filter.apply(frame=dyf_source_voxy,
                                                  f=lambda x: x["user_id"] is not None and x["user_id"] != ''
                                                              and x["total_activities_completed"] is not None and x["total_activities_completed"] > 0
                                                              and x["total_hours_studied"] is not None and x["total_hours_studied"] > 0.0
                                                              and x["email"] is not None and x["email"] != '')

        if(dyf_source_voxy.count() > 0):
            print('dyf_source_voxy::time_created -----------------test-----')
            dyf_source_voxy.printSchema()
            print('dyf_source_voxy number', dyf_source_voxy.count())

            #convert data
            data_df_voxy = dyf_source_voxy.toDF()
            data_df_voxy = data_df_voxy.withColumn('id_time',
                                                   from_unixtime(unix_timestamp(data_df_voxy.time_created, "yyyy-MM-dd"),
                                                                 "yyyyMMdd"))

            data_df_voxy = data_df_voxy.withColumn('correct_email', when(data_df_voxy.email.startswith('vn_'), f.translate(data_df_voxy.email, 'vn_', '')).otherwise(data_df_voxy.email))

            dyf_source_voxy = DynamicFrame.fromDF(data_df_voxy, glueContext, 'dyf_source_voxy')

            print('data_voxy::schema')
            dyf_source_voxy.printSchema()
            dyf_source_voxy.show(2)
            #


            #--------------Get student Id---------------------------------------------------------------------------------#
            dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                            table_name='student_contact')

            dy_student_contact_cache = dyf_student_contact.toDF()
            dy_student_contact_cache = dy_student_contact_cache.cache()
            dyf_student_contact = DynamicFrame.fromDF(dy_student_contact_cache, glueContext, 'dyf_student_contact')

            print("dyf_student_contact schema original -----------")
            dyf_student_contact.printSchema()
            dyf_student_contact.show(2)

            dyf_student_contact = dyf_student_contact.select_fields(
                ['contact_id', 'student_id', 'user_name'])

            # dyf_student_contact = Filter.apply(frame=dyf_student_contact,
            #                                f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
            #                                            and x["student_id"] > 0 and x["student_id"] > 0
            #                                            and x["user_name"] > 0 and x["user_name"] > 0)
            #-------------------------------------------------------------------------------------------------------------#

            join_voxy_log_student_id = Join.apply(dyf_source_voxy, dyf_student_contact, 'correct_email', 'user_name')

            print('join_voxy_log_student_id shema')
            join_voxy_log_student_id.printSchema()
            join_voxy_log_student_id.show(2)

            print('join_voxy_log_student_id number: ', join_voxy_log_student_id.count())


            # # chon field
            applymapping1 = ApplyMapping.apply(frame=join_voxy_log_student_id,
                                               mappings=[("student_id", "string", "student_id", "string"),
                                                         ("user_name", "string", "user_name", "string"),
                                                         ("id_time", "string", "id_time", "bigint"),
                                                         ("total_activities_completed", "long", "soca", "long"),
                                                         ("total_hours_studied", "double", "sogio", "double")])
            #
            #
            #
            resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice2")


            print('resolvechoice2::number: ', resolvechoice2.count())

            dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

            print('dropnullfields3::number: ', resolvechoice2.count())

            print('dropnullfields3::printSchema')
            dropnullfields3.printSchema()
            dropnullfields3.show(2)

            #
            # # ghi data vao redshift
            # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={"dbtable": "temp_staging_lich_su_tu_hoc_voxy",
            #                                                                                "database": "dts_odin"
            #                                                                                 ,
            #                                                                                "postactions": """INSERT into mapping_changed_status_student(student_id, user_id, change_status_date_id, to_status_id, measure1, measure2)
            #                                                                                                        SELECT voxy.student_id :: BIGINT, um.user_id, voxy.id_time, 50, voxy.soca, voxy.sogio
            #                                                                                                        FROM temp_staging_lich_su_tu_hoc_voxy voxy
            #                                                                                                        LEFT JOIN user_map um
            #                                                                                                            ON um.source_type = 2
            #                                                                                                            AND um.source_id = voxy.student_id;
            #                                                                                                        DROP TABLE IF EXISTS public.temp_staging_lich_su_tu_hoc_voxy"""
            #                                                                                },
            #                                                            redshift_tmp_dir="s3n://dtsodin/temp/tu-hoc/voxy/",
            #                                                            transformation_ctx="datasink4")
            #
            #
            # df_datasource = dyf_source_voxy.toDF()
            # flag = data_df_voxy.agg({"_key": "max"}).collect()[0][0]
            # flag_data = [flag]
            # df = spark.createDataFrame(flag_data, "long").toDF('flag')
            # df.write.parquet("s3a://dtsodin/flag/student_status/tu_hoc/tu_hoc_voxy_api.parquet", mode="overwrite")
            #
            # dy_source_voxy_cache.unpersist()
            # dy_student_contact_cache.unpersist()


if __name__ == "__main__":
    main()