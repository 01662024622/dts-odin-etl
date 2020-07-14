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

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source

    #------------------------------------------------------------------------------------------------------------------#
    dyf_native_talk = glueContext.create_dynamic_frame.from_catalog(database='native_talk',
                                                                table_name='native_talk_history_log_api')

    dyf_native_talk = dyf_native_talk.resolveChoice(specs=[('id', 'cast:long')])

    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_native_talk.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_native_talk = Filter.apply(frame=dyf_native_talk,
                                       f=lambda x: x["id"] > read_from_index)
    except:
        print('read flag file error ')

    dyf_native_talk = dyf_native_talk.select_fields(
        ['id', 'learning_date', 'speaking_dialog_score', 'username', 'updated_time'])

    dy_cache = dyf_native_talk.toDF()
    dy_cache = dy_cache.cache()
    dyf_native_talk = DynamicFrame.fromDF(dy_cache, glueContext, 'dyf_native_talk')

    print('dy_cache------------')
    dy_cache.printSchema()
    print('dy_cache: ', dy_cache.count())
    dy_cache.show(2)

    #------------------------------------------------------------------------------------------------------------------#

    if (dyf_native_talk.count() > 0):

        #---------------------------------------------------------datasource0-----------------------------------------------------#
        dyf_native_talk = Filter.apply(frame=dyf_native_talk,
                                              f=lambda x: x["username"] is not None and x["username"] != ''
                                                          and x["speaking_dialog_score"] is not None
                                                          and x["learning_date"] is not None and x["learning_date"] != '')
        # ----------------------------------datasource1---------------------------------------------------------------------------#
        if (dyf_native_talk.count() > 0):
            dyf_nt_account_mapping = glueContext.create_dynamic_frame.from_catalog(database='native_talk',
                                                                        table_name='native_talk_account_mapping')

            dyf_nt_account_mapping = dyf_nt_account_mapping.select_fields(['contact_id', 'username']).rename_field('username', 'nativetalk_user')
            dy_cache_2 = dyf_nt_account_mapping.toDF()
            dy_cache_2 = dy_cache_2.cache()
            dyf_nt_account_mapping = DynamicFrame.fromDF(dy_cache_2, glueContext, 'dyf_nt_account_mapping')

            dyf_nt_account_mapping = Filter.apply(frame=dyf_nt_account_mapping,
                                                  f=lambda x: x["nativetalk_user"] is not None and x["nativetalk_user"] != '')
            # ----------------------------------datasource1---------------------------------------------------------------------------#

            # -------------------------------------------------------------------------------------------------------------#
            join = Join.apply(dyf_native_talk, dyf_nt_account_mapping, 'username', 'nativetalk_user')
            if(join.count() > 0):
                df_nativetalk = join.toDF()
                df_nativetalk = df_nativetalk.withColumn('sogio', f.lit(0.083333)) #5 phut
                df_nativetalk = df_nativetalk.withColumn('id_time',
                                                         from_unixtime(
                                                             unix_timestamp(df_nativetalk.learning_date, "yyyy-MM-dd"),
                                                             "yyyyMMdd"))
                df_nativetalk = df_nativetalk.where("contact_id IS NOT NULL")

                data_nativetalk = DynamicFrame.fromDF(df_nativetalk, glueContext, 'data_nativetalk')
                data_nativetalk = data_nativetalk.resolveChoice(specs=[('sogio', 'cast:float')])
                # -------------------------------------------------------------------------------------------------------------#
                print('data_nativetalk----------')
                data_nativetalk.printSchema()


                # tinh bang "fact_hieusuathoctap"
                df_hieusuathoctap = data_nativetalk.toDF()
                # tinh so ca hoc, thoi gian hoc cua hoc vien trong ngay id_time
                df_hieusuathoctap = df_hieusuathoctap.groupby('contact_id', 'id_time').agg(f.sum('sogio'),
                                                                                               f.count('contact_id'))

                df_hieusuathoctap = df_hieusuathoctap.withColumn('tu_hoc_type_id', f.lit(400))
                data_hieusuathoctap = DynamicFrame.fromDF(df_hieusuathoctap, glueContext, 'data_hieusuathoctap')
                data_hieusuathoctap = data_hieusuathoctap.resolveChoice(specs=[('sum(sogio)', 'cast:double')])

                print('data_hieusuathoctap::data_hieusuathoctap::data_hieusuathoctap------------------------------------------')
                data_hieusuathoctap.printSchema();

                applymapping2 = ApplyMapping.apply(frame=data_hieusuathoctap,
                                                   mappings=[("contact_id", "string", "contact_id", "string"),
                                                             ("id_time", 'string', 'id_time', 'bigint'),
                                                             ("count(contact_id)", 'long', 'soca', 'int'),
                                                             ("sum(sogio)", 'double', 'sogio', 'double'),
                                                             ("tu_hoc_type_id", 'int', "tu_hoc_type_id", "int")])


                resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                                     transformation_ctx="resolvechoice2")
                dropnullfields2 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

                print('dropnullfields2 number: ', dropnullfields2.count())

                datasink2 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields2,
                                                                           catalog_connection="glue_redshift",
                                                                           connection_options={"dbtable": "temp_staging_lich_su_tu_hoc_native_talk",
                                                                                               "database": "dts_odin",
                                                                                               "postactions": """INSERT into mapping_changed_status_student(user_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                                                            SELECT um.user_id, hwb.id_time, 53, hwb.soca, round(hwb.sogio, 4)
                                                                                                                            FROM temp_staging_lich_su_tu_hoc_native_talk hwb
                                                                                                                            LEFT JOIN user_map um
                                                                                                                                ON um.source_type = 1
                                                                                                                                AND um.source_id = hwb.contact_id;
                                                                                                                 DROP TABLE IF EXISTS public.temp_staging_lich_su_tu_hoc_native_talk    
                                                                                                                """
                                                                                               },
                                                                           redshift_tmp_dir="s3n://dts-odin/temp/tu-hoc/hwb/",
                                                                           transformation_ctx="datasink2")

                df_datasource = dyf_native_talk.toDF()
                flag = df_datasource.agg({"id": "max"}).collect()[0][0]
                print('flag: ', flag)
                flag_data = [flag]
                df = spark.createDataFrame(flag_data, "long").toDF('flag')
                df.write.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_native_talk.parquet", mode="overwrite")
                dy_cache.unpersist()
                dy_cache_2.unpersist()

if __name__ == "__main__":
    main()