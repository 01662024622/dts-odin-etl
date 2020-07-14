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
    dyf_source_ls_dong_tien = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                            table_name='nvn_poss_lich_su_dong_tien')
    dyf_source_ls_dong_tien = dyf_source_ls_dong_tien.resolveChoice(specs=[('_key', 'cast:long')])
    # print schema and select fields
    print('original schema')
    dyf_source_ls_dong_tien.printSchema()

    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/nvn_poss_lich_su_dong_tien.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_source_ls_dong_tien = Filter.apply(frame=dyf_source_ls_dong_tien,
                                               f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')
    print('the number of new contacts: ', dyf_source_ls_dong_tien.count())

    dyf_source_ls_dong_tien = dyf_source_ls_dong_tien.select_fields(
        ['_key', 'id', 'contact_id', 'ngay_thanhtoan', 'ngay_tao', 'makh'])

    dy_source_ls_dt_cache = dyf_source_ls_dong_tien.toDF()
    dy_source_ls_dt_cache = dy_source_ls_dt_cache.dropDuplicates(['id'])
    dy_source_ls_dt_cache = dy_source_ls_dt_cache.cache()
    dyf_source_ls_dong_tien = DynamicFrame.fromDF(dy_source_ls_dt_cache, glueContext, 'dyf_source_ls_dong_tien')

    if (dyf_source_ls_dong_tien.count() > 0):
        dyf_source_ls_dong_tien = Filter.apply(frame=dyf_source_ls_dong_tien,
                                               f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                           and x["ngay_thanhtoan"] is not None and x[
                                                               "ngay_thanhtoan"] != '')

        if (dyf_source_ls_dong_tien.count() > 0):
            print('dyf_source_ls_dong_tien::corrcect')
            print('dyf_source_voxy number', dyf_source_ls_dong_tien.count())

            # convert data
            df_dong_tien = dyf_source_ls_dong_tien.toDF()
            df_dong_tien = df_dong_tien.withColumn('id_time',
                                                   from_unixtime(
                                                       unix_timestamp(df_dong_tien.ngay_thanhtoan, "yyyy-MM-dd"),
                                                       "yyyyMMdd"))
            dyf_source_ls_dong_tien = DynamicFrame.fromDF(df_dong_tien, glueContext, 'dyf_source_ls_dong_tien')

            print('data_voxy::schema')
            dyf_source_ls_dong_tien.printSchema()
            dyf_source_ls_dong_tien.show(2)

            # # chon field
            applymapping1 = ApplyMapping.apply(frame=dyf_source_ls_dong_tien,
                                               mappings=[("contact_id", "string", "contact_id", "string"),
                                                         ("id_time", "string", "id_time", "bigint"),
                                                         ("ngay_thanhtoan", "string", "ngay_thanhtoan", "timestamp"),
                                                         ("makh", "int", "makh", "int")])

            resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice2")
            dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

            print('dropnullfields3::printSchema')
            dropnullfields3.printSchema()
            dropnullfields3.show(2)

            #
            # # ghi data vao redshift
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "temp_ls_dong_tien_v2",
                                                                           "database": "dts_odin",
                                                                           "postactions": """INSERT INTO mapping_changed_status_student(description, user_id, change_status_date_id, timestamp1, to_status_id)
                                                                                                            SELECT 'contact_id =' +  temp_a0.contact_id  + ' - makh = ' + temp_a0.makh, up.user_id, temp_a0.id_time, temp_a0.ngay_thanhtoan, 1
                                                                                                            FROM temp_ls_dong_tien_v2 temp_a0
                                                                                                            LEFT JOIN user_map up
                                                                                                                ON up.source_type = 1
                                                                                                                AND temp_a0.contact_id = up.source_id;
                                                                                                            DROP TABLE IF EXISTS public.temp_ls_dong_tien_v2"""
                                                                           },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp/temp_ls_dong_tien_v2",
                                                                       transformation_ctx="datasink4")
            df_datasource = dyf_source_ls_dong_tien.toDF()
            flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            df.write.parquet("s3a://dts-odin/flag/student_status/nvn_poss_lich_su_dong_tien.parquet", mode="overwrite")
            dy_source_ls_dt_cache.unpersist()


if __name__ == "__main__":
    main()