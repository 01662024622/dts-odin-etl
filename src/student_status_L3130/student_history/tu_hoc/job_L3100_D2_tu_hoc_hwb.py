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
    dyf_mdl_le_exam_attemp = glueContext.create_dynamic_frame.from_catalog(database='lms_pisn_0218',
                                                                      table_name='mdl_le_exam_attemp')

    dy_exam_attemp_cache = dyf_mdl_le_exam_attemp.toDF()
    dy_exam_attemp_cache = dy_exam_attemp_cache.cache()
    dyf_mdl_le_exam_attemp = DynamicFrame.fromDF(dy_exam_attemp_cache, glueContext, 'dyf_mdl_le_exam_attemp')

    dyf_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.resolveChoice(specs=[('_key', 'cast:long')])
    print('original schema')
    dyf_mdl_le_exam_attemp.printSchema()

    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_hwb.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_mdl_le_exam_attemp = Filter.apply(frame=dyf_mdl_le_exam_attemp,
                                       f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')

    print('the number of new contacts: ', dyf_mdl_le_exam_attemp.count())

    dyf_mdl_le_exam_attemp = dyf_mdl_le_exam_attemp.select_fields(['_key', 'id', 'user_id', 'created_at'])\
        .rename_field('user_id', 'hwb_id')

    if (dyf_mdl_le_exam_attemp.count() > 0):
        # loc du lieu
        dyf_mdl_le_exam_attemp = Filter.apply(frame=dyf_mdl_le_exam_attemp,
                                                  f=lambda x: x["hwb_id"] is not None and x["hwb_id"] != '')

        print('dyf_mdl_le_exam_attemp schema')
        dyf_mdl_le_exam_attemp.printSchema()
        dyf_mdl_le_exam_attemp.show(10)
        data_df_hwb = dyf_mdl_le_exam_attemp.toDF()
        data_df_hwb = data_df_hwb.withColumn('id_time',
                                             from_unixtime(unix_timestamp(data_df_hwb.created_at, "yyyy-MM-dd"),
                                                           "yyyyMMdd"))
        data_df_hwb = data_df_hwb.withColumn('sogio', f.lit(0.33))

        data_df_hwb = data_df_hwb.groupby('hwb_id', 'id_time').agg(f.sum('sogio'),
                                                                                       f.count('hwb_id'))

        dyf_lich_su_hoc_hwb = DynamicFrame.fromDF(data_df_hwb, glueContext, 'dyf_lich_su_hoc_hwb')
        dyf_lich_su_hoc_hwb = dyf_lich_su_hoc_hwb.resolveChoice(specs=[('sum(sogio)', 'cast:double')])



        # print('dyf_lich_su_hoc_hwb::printSchema')
        # dyf_lich_su_hoc_hwb.printSchema()
        # dyf_lich_su_hoc_hwb.show(5)

        # --------------------------------------------------------------------------------------------------------------#
        dyf_mdl_mdl_user = glueContext.create_dynamic_frame.from_catalog(database='lms_pisn_0218',
                                                                         table_name='mdl_user')

        dy_exam_mdl_user_cache = dyf_mdl_mdl_user.toDF()
        dy_exam_mdl_user_cache = dy_exam_mdl_user_cache.cache()
        dyf_mdl_mdl_user = DynamicFrame.fromDF(dy_exam_mdl_user_cache, glueContext, 'dyf_mdl_mdl_user')

        # print('dyf_mdl_mdl_user original--------------')
        # dyf_mdl_mdl_user.printSchema()
        # dyf_mdl_mdl_user.show(2)

        dyf_mdl_mdl_user = dyf_mdl_mdl_user.select_fields(['id', 'email']) \
            .resolveChoice(specs=[('id', 'cast:string')])

        dyf_contact_email = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                         table_name='student_contact_email')

        dyf_contact_email = dyf_contact_email.select_fields(['contact_id', 'email'])

        dy_contact_email_cache = dyf_contact_email.toDF()
        dy_contact_email_cache = dy_contact_email_cache.cache()
        dyf_contact_email = DynamicFrame.fromDF(dy_contact_email_cache, glueContext, 'dyf_contact_email')


        # print('dyf_contact_email original--------------')
        # dyf_contact_email.printSchema()
        # dyf_contact_email.show(2)

        join = Join.apply(Join.apply(dyf_lich_su_hoc_hwb, dyf_mdl_mdl_user, 'hwb_id', 'id'),
                          dyf_contact_email, 'email', 'email')



        # print('join::join::join------')
        # join.printSchema()
        # join.show(10)
        # print('join::count(): ', join.count())


        applymapping2 = ApplyMapping.apply(frame=join,
                                           mappings=[("contact_id", "string", "contact_id", "string"),
                                                     ("id_time", 'string', 'id_time', 'bigint'),
                                                     ("count(hwb_id)", 'long', 'soca', 'int'),
                                                     ("sum(sogio)", 'double', 'sogio', 'double')])
        #
        #
        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields6 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

        datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields6,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "temp_staging_lich_su_tu_hoc_hwb",
                                                                                       "database": "dts_odin"
                                                                       ,
                                                                                       "postactions": """INSERT into mapping_changed_status_student(description, user_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                                           SELECT 'contact_id: ' + hwb.contact_id, um.user_id, hwb.id_time, 51, hwb.soca, hwb.sogio
                                                                                                           FROM temp_staging_lich_su_tu_hoc_hwb hwb
                                                                                                           LEFT JOIN user_map um
                                                                                                               ON um.source_type = 1
                                                                                                               AND um.source_id = hwb.contact_id;
                                                                                                               DROP TABLE IF EXISTS public.temp_staging_lich_su_tu_hoc_hwb"""
                                                                                       },
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/tu-hoc/hwb/",
                                                                   transformation_ctx="datasink4")

        df_datasource = dyf_mdl_le_exam_attemp.toDF()
        flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://dts-odin/flag/student_status/tu_hoc/tu_hoc_hwb.parquet", mode="overwrite")

        dy_exam_attemp_cache.unpersist()
        dy_exam_mdl_user_cache.unpersist()
        dy_contact_email_cache.unpersist()


if __name__ == "__main__":
    main()