import sys
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.dynamicframe_filter import Filter
from awsglue.transforms.resolve_choice import ResolveChoice
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
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    mdl_reply_rating = glueContext.create_dynamic_frame.from_catalog(database="native_smile",
                                                                     table_name="tblreply_rating")

    mdl_reply_rating = mdl_reply_rating.select_fields(
        ['_key', 'id', 'userid', 'ratingid', 'time_rating'])

    mdl_reply_rating = mdl_reply_rating.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_rating_class_h2472.parquet")
    max_key = df_flag.collect()[0]['flag']

    print("max_key: ", max_key)

    mdl_reply_rating = Filter.apply(frame=mdl_reply_rating,
                                    f=lambda x: x["_key"] > max_key)
    if (mdl_reply_rating.count() > 0):
        print(mdl_reply_rating.count())
        try:
            mdl_dm_tu_dien = glueContext.create_dynamic_frame.from_catalog(database="dts_odin",
                                                                           table_name="tma_dm_tu_dien")
            mdl_dm_tu_dien = mdl_dm_tu_dien.select_fields(
                ['id', 'ma_tu_dien', 'id_dm_loai_tu_dien']).rename_field('id',
                                                                         'tbl_tu_dien_id')
            mdl_dm_tu_dien = Filter.apply(frame=mdl_dm_tu_dien,
                                          f=lambda x: x["id_dm_loai_tu_dien"] == 7)

            mdl_reply_rating = Filter.apply(frame=mdl_reply_rating,
                                            f=lambda x: x["ratingid"] in (14, 15, 16, 17, 18))

            df_mdl_reply_rating = mdl_reply_rating.toDF()

            df_mdl_dm_tu_dien = mdl_dm_tu_dien.toDF()

            df_join_rating_tu_dien = df_mdl_reply_rating.join(df_mdl_dm_tu_dien, (
                    df_mdl_reply_rating['ratingid'] == df_mdl_dm_tu_dien['tbl_tu_dien_id']), 'left_outer')

            df_join_rating_tu_dien = df_join_rating_tu_dien.withColumn('id_time',
                                                                       from_unixtime(
                                                                           df_join_rating_tu_dien[
                                                                               'time_rating']))

            df_join_rating_tu_dien = df_join_rating_tu_dien.withColumn('mapping_id_time',
                                                                       from_unixtime(
                                                                           df_join_rating_tu_dien[
                                                                               'time_rating'],
                                                                           "yyyyMMdd"))

            # So lan rating 1 2 3 4 5* cua tung hoc vien theo tung ngay
            # start

            df_join_rating_tu_dien = df_join_rating_tu_dien.groupby('userid', 'mapping_id_time',
                                                                    'ratingid').agg(
                f.count('ratingid'))

            df_join_rating_tu_dien.printSchema()

            df_join_rating_tu_dien = df_join_rating_tu_dien.withColumn('points', when(
                df_join_rating_tu_dien['ratingid'] == 14, f.lit(1)).when(
                df_join_rating_tu_dien['ratingid'] == 15, f.lit(2)).when(
                df_join_rating_tu_dien['ratingid'] == 16, f.lit(3)).when(
                df_join_rating_tu_dien['ratingid'] == 17, f.lit(4)).when(
                df_join_rating_tu_dien['ratingid'] == 18, f.lit(5)))

            df_join_rating_tu_dien = df_join_rating_tu_dien.withColumn('to_status_id', f.lit(505))

            dyf_join_rating_user_tu_dien = DynamicFrame.fromDF(df_join_rating_tu_dien,
                                                               glueContext, "datasource0")

            applymapping_count_rating = ApplyMapping.apply(frame=dyf_join_rating_user_tu_dien,
                                                           mappings=[("userid", "long", "student_id", "long"),
                                                                     ("mapping_id_time", "string", "change_status_date_id", "long"),
                                                                     ("points", "int", "measure1", "double"),
                                                                     ("to_status_id", "int", "to_status_id", "long"),
                                                                     ("count(ratingid)", "long", "measure2", "double")])

            resolvechoice_count_rating = ResolveChoice.apply(frame=applymapping_count_rating, choice="make_cols",
                                                             transformation_ctx="resolvechoice_count_rating")

            dropnullfields_count_rating_star = DropNullFields.apply(frame=resolvechoice_count_rating,
                                                                    transformation_ctx="dropnullfields3")

            print(dropnullfields_count_rating_star.count())
            dropnullfields_count_rating_star.toDF().show()

            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_count_rating_star,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_changed_status_student",
                                                                           "database": "dts_odin",
                                                                           "postactions": """ UPDATE mapping_changed_status_student 
                                                                                                SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 2 AND source_id = student_id ) 
                                                                                                WHERE
                                                                                                    user_id IS NULL """
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
                                                                       transformation_ctx="datasink4")

            # end

            # ghi flag
            # lay max key trong data source
            mdl_reply_rating = mdl_reply_rating.toDF()
            flag = mdl_reply_rating.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_rating_class_h2472.parquet", mode="overwrite")

        except Exception as e:
            print("No new data")
            print(e)


if __name__ == "__main__":
    main()
