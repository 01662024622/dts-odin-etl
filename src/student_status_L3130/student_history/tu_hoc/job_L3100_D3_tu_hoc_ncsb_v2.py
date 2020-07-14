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
    dyf_ds_results = glueContext.create_dynamic_frame.from_catalog(database='dts-odin_ncsbasic', table_name='results')

    dyf_ds_results = dyf_ds_results.resolveChoice(specs=[('_key', 'cast:long')])
    # try:
    #     df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_ncsb.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     dyf_ds_results = Filter.apply(frame=dyf_ds_results,
    #                                    f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')
    #
    # dyf_ds_results = dyf_ds_results.select_fields(
    #     ['_key', '_id', 'userid', 'time_begin', 'time_end', 'timecreated']).rename_field(
    #     '_id', 'id')

    dy_cache = dyf_ds_results.toDF()
    dy_cache = dy_cache.cache()
    dyf_ds_results = DynamicFrame.fromDF(dy_cache, glueContext, 'dyf_ds_results')

    #doc moc flag tu s3
    print('dyf_ds_results::schema')
    dyf_ds_results.printSchema()
    dyf_ds_results.show(5)

    if (dyf_ds_results.count() > 0):
        #--------------------------------------------------------------------------------------------------------------#
        dyf_student_contact_email = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                                 table_name='student_contact_email')

        dyf_student_contact_email = dyf_student_contact_email.select_fields(['email', 'contact_id'])
        dyf_student_contact_email = Filter.apply(frame=dyf_student_contact_email,
                                                 f=lambda x: x["email"] is not None
                                                             and x["email"] != '')
        df_student_contact_email = dyf_student_contact_email.toDF()
        df_student_contact_email = df_student_contact_email.dropDuplicates(['contact_id', 'email'])
        dyf_student_contact_email = DynamicFrame.fromDF(df_student_contact_email, glueContext,
                                                        "dyf_student_contact_email")
        # -------------------------------------------------------------------------------------------------------------#

        # -------------------------------------------------------------------------------------------------------------#
        dyf_users = glueContext.create_dynamic_frame.from_catalog(database='dts-odin_ncsbasic', table_name='users')
        dyf_users = dyf_users.select_fields(['_id', 'email'])
        # -------------------------------------------------------------------------------------------------------------#

        # -------------------------------------------------------------------------------------------------------------#
        dyf_ds_results_nscb = Filter.apply(frame=dyf_ds_results, f=lambda x: x["time_begin"] is not None and x["time_begin"] != ''
                                                                and x["time_end"] is not None and x["time_end"] != ''
                                                                and x["time_begin"] < x["time_end"]
                                                                and x["timecreated"] is not None and x["timecreated"] != '')
        # -------------------------------------------------------------------------------------------------------------#

        # -------------------------------------------------------------------------------------------------------------#
        # ds_df_results = ds_results.toDF()
        # ds_df_results = ds_df_results.where('time_begin IS NOT NULL AND time_end IS NOT NULL')
        # ds_results_nscb = DynamicFrame.fromDF(ds_df_results, glueContext, 'ds_results_nscb')

        # map ls ncsb vs contact_id
        join_ncsb1 = Join.apply(dyf_ds_results_nscb, dyf_users, 'userid', '_id')
        join_ncsb2 = Join.apply(join_ncsb1, dyf_student_contact_email, 'email', 'email')

        print('join_ncsb2::schema')
        join_ncsb2.printSchema()
        join_ncsb2.show(5)

        # convert data
        join_ncsb2 = Filter.apply(frame=join_ncsb2, f=lambda x: x["contact_id"] is not None)

        data_df_ncsb = join_ncsb2.toDF()
        data_df_ncsb = data_df_ncsb.withColumn('sogio', (data_df_ncsb.time_end - data_df_ncsb.time_begin) / 3600)
        data_df_ncsb = data_df_ncsb.withColumn("giovao", from_unixtime(data_df_ncsb.time_begin))
        data_df_ncsb = data_df_ncsb.withColumn("ngay_tao", from_unixtime(data_df_ncsb.timecreated))
        data_df_ncsb = data_df_ncsb.withColumn('id_time', from_unixtime(data_df_ncsb.time_begin, "yyyyMMdd"))
        # data_df_ncsb = data_df_ncsb.where("contact_id IS NOT NULL")
        data_df_ncsb = data_df_ncsb.where("sogio > 0.0")

        data_df_ncsb = data_df_ncsb.groupby('contact_id', 'id_time').agg(f.sum('sogio').alias("tong_so_gio"),
                                                                           f.count('contact_id'))
        data_df_ncsb = data_df_ncsb.dropDuplicates(['contact_id', 'id_time'])
        data_ncsb = DynamicFrame.fromDF(data_df_ncsb, glueContext, 'data_ncsb')

        data_ncsb = data_ncsb.resolveChoice(specs=[('tong_so_gio', 'cast:float')])
        # -------------------------------------------------------------------------------------------------------------#

        # -------------------------------------------------------------------------------------------------------------#
        # tinh bang "fact_hieusuathoctap"
        # df_hieusuathoctap = dropnullfields1.toDF()

        print ('data_ncsb::data_ncsb::data_ncsb::printSchema------------------')
        data_ncsb.printSchema()

        # print ('data_ncsb::data_ncsb::data_ncsb::show------------------')
        data_ncsb.show(10)

        print('data_ncsb::number: ', data_ncsb.count())

        # tinh so ca hoc, thoi gian hoc cua hoc vien trong ngay id_time

        applymapping2 = ApplyMapping.apply(frame=data_ncsb,
                                           mappings=[("contact_id", "string", "contact_id", "string"),
                                                     ("id_time", 'string', 'id_time', 'bigint'),
                                                     ("count(contact_id)", 'long', 'soca', 'int'),
                                                     ("tong_so_gio", 'float', 'sogio', 'float')])

        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields2 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        datasink2 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields2,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "temp_staging_lich_su_tu_hoc_ncsb_v2",
                                                                                       "database": "dts_odin",
                                                                                       "postactions": """INSERT into mapping_changed_status_student(user_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                                            SELECT um.user_id, hwb.id_time, 52, hwb.soca, hwb.sogio
                                                                                                            FROM temp_staging_lich_su_tu_hoc_ncsb_v2 hwb
                                                                                                            LEFT JOIN user_map um
                                                                                                                 ON um.source_type = 1
                                                                                                                 AND um.source_id = hwb.contact_id
                                                                                                            WHERE um.user_id is not null;
                                                                                                             DROP TABLE IF EXISTS public.temp_staging_lich_su_tu_hoc_ncsb_v2"""
                                                                                       },
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/tu-hoc/ncsb_2",
                                                                   transformation_ctx="datasink4")


        df_datasource = dyf_ds_results.toDF()
        flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_ncsb.parquet", mode="overwrite")

        dy_cache.unpersist()

if __name__ == "__main__":
    main()