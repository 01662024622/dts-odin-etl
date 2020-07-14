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
from pyspark.sql.types import DateType


## @params: [TempDir, JOB_NAME]
# args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    # job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    dyf_care_call = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                         table_name='care_call')

    dyf_care_call = dyf_care_call.resolveChoice(specs=[('_key', 'cast:long')])
    # print schema and select fields
    print('original schema')
    dyf_care_call.printSchema()
    dyf_care_call.show(10)

    # try:
    #     df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/temp_ls_a1_dong_tien_tc.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     dyf_care_call = Filter.apply(frame=dyf_care_call,
    #                                            f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')
    # print('the number of new contacts: ', dyf_care_call.count())

    dyf_care_call = dyf_care_call.select_fields(
        ['_key', 'id', 'phone', 'duration', 'call_status', 'time_created']).rename_field('time_created', 'call_date')

    dy_source_care_call_cache = dyf_care_call.toDF()
    dy_source_care_call_cache = dy_source_care_call_cache.dropDuplicates(['id'])
    dy_source_care_call_cache = dy_source_care_call_cache.cache()
    dyf_care_call = DynamicFrame.fromDF(dy_source_care_call_cache, glueContext, 'dyf_care_call')
    #
    if (dyf_care_call.count() > 0):
        dyf_care_call = Filter.apply(frame=dyf_care_call,
                                            f=lambda x: x["phone"] is not None and x["phone"] != ''
                                                        and (x["call_status"] == 'success' or x["call_status"] == 'call_success')
                                                        and x["call_date"] is not None and x["call_date"] != ''
                                                        and x["duration"] is not None and x["duration"] > 30
                                                        )
    #
        print('dyf_care_call::corrcect')
        print('dyf_care_call number', dyf_care_call.count())
        if (dyf_care_call.count() > 0):

            dyf_ad_contact_phone = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                                 table_name='student_contact_phone')

            dyf_ad_contact_phone = dyf_ad_contact_phone.select_fields(
                ['phone', 'contact_id'])

            dyf_ad_contact_phone = Filter.apply(frame=dyf_ad_contact_phone,
                                                f=lambda x: x["phone"] is not None and x["phone"] != ''
                                                            and x["contact_id"] is not None and x["contact_id"] != ''
                                                )

            print('dyf_ad_contact_phone::schema')
            dyf_ad_contact_phone.printSchema()


    #         dyf_advisor_ip_phone = glueContext.create_dynamic_frame.from_catalog(database='callcenter',
    #                                                                              table_name='advisor_ip_phone')
    #
    #         dyf_advisor_ip_phone = Filter.apply(frame=dyf_advisor_ip_phone,
    #                                             f=lambda x: x["ip_phone"] is not None and x["ip_phone"] != '')
    #
    #
    #
    #
    #
    #
    #-----------------------------------------------------------------------------------------------------------#

            join_call_contact = Join.apply(dyf_care_call, dyf_ad_contact_phone, 'phone', 'phone')
            # join_call_contact = join_call_contact.select_fields(['id_time', 'answertime', 'calldate', 'phonenumber_correct', 'calldate', 'ipphone', 'contact_id'])
            # print('join_call_contact::schema------------')
            join_call_contact.printSchema()
            join_call_contact.show(2)
            print('join: ', join_call_contact.count())
    #
    #
    #         #-----------------------------------------------------------------------------------------------------------#
    #
            dyf_source_ls_dong_tien = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                                    table_name='nvn_poss_lich_su_dong_tien')

            dyf_source_ls_dong_tien = Filter.apply(frame=dyf_source_ls_dong_tien,
                                                   f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                               and x["ngay_thanhtoan"] is not None and x[
                                                                   "ngay_thanhtoan"] != '')

            dyf_source_ls_dong_tien = dyf_source_ls_dong_tien.select_fields(
                ['_key', 'id', 'contact_id', 'ngay_thanhtoan', 'ngay_tao', 'makh']).rename_field('ngay_tao', 'ngay_a0')

            dy_source_ls_dt_cache = dyf_source_ls_dong_tien.toDF()
            dy_source_ls_dt_cache = dy_source_ls_dt_cache.dropDuplicates(['id'])
            dy_source_ls_dt_cache = dy_source_ls_dt_cache.cache()
            dyf_source_ls_dong_tien = DynamicFrame.fromDF(dy_source_ls_dt_cache, glueContext, 'dyf_source_ls_dong_tien')
    #
            join_call_contact_ao = Join.apply(join_call_contact, dyf_source_ls_dong_tien, 'contact_id', 'contact_id')
    #
            print('join_call_contact_ao::schema------------')
            join_call_contact_ao.printSchema()
            join_call_contact_ao.show(2)
            print('join: ', join_call_contact_ao.count())
    #
    #         # join_call_contact_ao = join_call_contact_ao.resolveChoice(specs=[('calldate', 'cast:timestamp'),
    #         #                                                                  ('ngay_a0', 'cast:timestamp')])
    #
    #
            join_call_contact_ao = Filter.apply(frame=join_call_contact_ao,
                                                   f=lambda x: x["call_date"] is not None and x["ngay_a0"] is not None
                                                               and x["call_date"] > x["ngay_a0"])
    #
            print('join_call_contact_ao::after filter calldate > ngay_a0------------')
            # join_call_contact_ao.printSchema()
            join_call_contact_ao.show(2)
            print('join_call_contact_ao: ', join_call_contact_ao.count())
    #
    #         #get lich su chao mung thanh cong
            df_join_call_contact_ao = join_call_contact_ao.toDF()
            df_join_call_contact_ao = df_join_call_contact_ao.groupby('contact_id', 'makh').agg(
                f.min('call_date').alias("ngay_a1"))

            df_join_call_contact_ao = df_join_call_contact_ao.withColumn('id_time',
                                                 from_unixtime(unix_timestamp(df_join_call_contact_ao.ngay_a1,
                                                                              "yyyy-MM-dd HH:mm:ss"),
                                                               "yyyyMMdd"))
            dyf_result = DynamicFrame.fromDF(df_join_call_contact_ao, glueContext,
                                                                     'dyf_result')
    #
    #         print('dyf_result------------')
            # join_call_contact_ao.printSchema()
            dyf_result.show(2)
            print('dyf_result: ', dyf_result.count())
    #
    #
    #
    #
    #         # # chon field
            applymapping1 = ApplyMapping.apply(frame=dyf_result,
                                               mappings=[("contact_id", "string", "contact_id", "string"),
                                                         ("id_time", "string", "id_time", "bigint"),
                                                         ("makh", "int", "makh", "int"),
                                                         ("ngay_a1", "string", "ngay_a1", "timestamp")])
    #
            resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice2")
            dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

            # print('dropnullfields3::printSchema')
            # dropnullfields3.printSchema()
            # dropnullfields3.show(2)

            # ghi data vao redshift
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "temp_ls_dong_tien_a1_v3",
                                                                           "database": "dts_odin",
                                                                           "postactions": """
                                                                                        INSERT into mapping_changed_status_student(description, user_id, change_status_date_id, to_status_id, timestamp1)
                                                                                        SELECT 'contact_id: ' + temp_a1.contact_id +' - makh: ' + temp_a1.makh, um.user_id ,temp_a1.id_time, 2, temp_a1.ngay_a1
                                                                                        FROM temp_ls_dong_tien_a1_v3 temp_a1
                                                                                        LEFT JOIN user_map um
                                                                                             ON um.source_type = 1
                                                                                             AND um.source_id = temp_a1.contact_id
                                                                                        ;
                                                                                        DROP TABLE IF EXISTS public.temp_ls_dong_tien_a1_v3;
                                                                                        CALL update_a1_exception_from_eg()
                                                                           """
                                                                           },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp/temp_ls_dong_tien/v2",
                                                                       transformation_ctx="datasink4")
            df_datasource = dyf_care_call.toDF()
            flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            df.write.parquet("s3a://dts-odin/flag/student_status/temp_ls_a1_dong_tien_tc.parquet", mode="overwrite")
            dy_source_care_call_cache.unpersist()


if __name__ == "__main__":
    main()
