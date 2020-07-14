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

    mdl_rating_class = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                     table_name="mdl_rating_class")

    mdl_rating_class = mdl_rating_class.select_fields(
        ['_key', 'id', 'points', 'vote', 'student_id', 'teacher_id', 'room_id', 'timecreated', 'opinion'])

    mdl_rating_class = mdl_rating_class.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_rating_class.parquet")

    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # data = mdl_rating_class.toDF()
    # data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    # data = data.where(data['_key'] < 100)
    #
    # mdl_rating_class = DynamicFrame.fromDF(data, glueContext, "mdl_rating_class")
    max_key = df_flag.collect()[0]['flag']

    print("max_key: ", max_key)

    # mdl_rating_class = Filter.apply(frame=mdl_rating_class,
    #                                 f=lambda x: x["_key"] > max_key)
    if (mdl_rating_class.count() > 0):
        print(mdl_rating_class.count())
        try:
            # mdl_logsservice_move_user = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
            #                                                                           table_name="mdl_logsservice_move_user")
            # mdl_logsservice_move_user = mdl_logsservice_move_user.select_fields(
            #     ['id', 'userid', 'roomidto', 'role_in_class', 'device_type']).rename_field('id',
            #                                                                                'log_id')

            # mdl_rating_class = Filter.apply(frame=mdl_rating_class,
            #                                 f=lambda x: x['vote'] == 0 or x['vote'] == 4)

            df_mdl_rating_class = mdl_rating_class.toDF()

            # df_mdl_logsservice_move_user = mdl_logsservice_move_user.toDF()
            #
            # join_rating_class_logsservice_move_user = df_mdl_rating_class.join(df_mdl_logsservice_move_user, (
            #         df_mdl_rating_class['student_id'] == df_mdl_logsservice_move_user['userid']) & (
            #                                                                            df_mdl_rating_class['room_id'] ==
            #                                                                            df_mdl_logsservice_move_user[
            #                                                                                'roomidto']), 'left_outer')
            #
            # join_rating_class_logsservice_move_user = join_rating_class_logsservice_move_user.withColumn('role_class',
            #                                                                                              when((
            #                                                                                                      join_rating_class_logsservice_move_user[
            #                                                                                                          'role_in_class'] == 'AUDIT'),
            #                                                                                                  'Audience').otherwise(
            #                                                                                                  'Practical'))
            #
            # join_rating_class_logsservice_move_user = join_rating_class_logsservice_move_user.withColumn('device',
            #                                                                                              when((
            #                                                                                                      join_rating_class_logsservice_move_user[
            #                                                                                                          'device_type'] == 'MOBILE'),
            #                                                                                                  'MOBLIE').otherwise(
            #                                                                                                  'WEB'))

            df_mdl_rating_class = df_mdl_rating_class.withColumn('id_time',
                                                                 from_unixtime(
                                                                     df_mdl_rating_class[
                                                                         'timecreated']))

            df_mdl_rating_class = df_mdl_rating_class.withColumn('mapping_id_time',
                                                                 from_unixtime(
                                                                     df_mdl_rating_class[
                                                                         'timecreated'],
                                                                     "yyyyMMdd"))

            # data_rating_class_logsservice_move_user = DynamicFrame.fromDF(df_mdl_rating_class,
            #                                                               glueContext, "datasource0")

            # Tat ca nhung lan rating cua hoc vien sau khi loc dieu kien o tren
            # applymapping = ApplyMapping.apply(frame=data_rating_class_logsservice_move_user,
            #                                   mappings=[("student_id", "string", "id_hoc_vien", "bigint"),
            #                                             ("room_id", "string", "id_lop", "bigint"),
            #                                             ("vote", "int", "danh_gia", "int"),
            #                                             ("points", "int", "diem", "int"),
            #                                             ("id_time", "string", "gio_tao", "timestamp"),
            #                                             ("opinion", "string", "mo_ta", "string"),
            #                                             ("role_class", "string", "vai_tro", "string"),
            #                                             ("device", "string", "thiet_bi", "string"),
            #                                             ("mapping_id_time", "string", "id_time_mapping", "int")])
            #
            # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
            #                                     transformation_ctx="resolvechoice2")
            #
            # dropnullfields3 = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")
            #
            # print(dropnullfields3.count())
            # dropnullfields3.toDF().show()
            #
            # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={"dbtable": "rating_class",
            #                                                                                "database": "dts_odin"},
            #                                                            redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
            #                                                            transformation_ctx="datasink4")

            # Diem trung binh rating cua tung hoc vien theo tung ngay
            # start

            # avg_count_rating = data_rating_class_logsservice_move_user.toDF()
            #
            # avg_count_rating = avg_count_rating.groupby('student_id', 'mapping_id_time').agg(f.count('points'),
            #                                                                                  f.avg('points'))
            #
            # avg_count_rating.printSchema()
            #
            # avg_count_rating = avg_count_rating.withColumn('to_status_id', f.lit(13))
            #
            # df_data_join_avg_rating = DynamicFrame.fromDF(avg_count_rating,
            #                                               glueContext, "datasource0")
            #
            # applymapping2 = ApplyMapping.apply(frame=df_data_join_avg_rating,
            #                                    mappings=[("student_id", "string", "student_id", "bigint"),
            #                                              ("mapping_id_time", "string", "change_status_date_id", "int"),
            #                                              ("to_status_id", "int", "to_status_id", "int"),
            #                                              ("avg(points)", "double", "measure1", "double"),
            #                                              ("count(points)", "long", "measure2", "int")])
            #
            # resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
            #                                      transformation_ctx="resolvechoice2")
            #
            # avg_count_rating = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")
            #
            # print(avg_count_rating.count())
            # avg_count_rating.toDF().show()
            #
            # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=avg_count_rating,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "test_rating_avg",
            #                                                                "database": "dts_odin",
            #                                                                "postactions": """DROP TABLE IF EXISTS temp_join_rating_old_new;
            #
            #                                                                                     CREATE TABLE temp_join_rating_old_new AS
            #                                                                                     SELECT mcss.id as "id", tra.student_id as "student_id", tra.change_status_date_id as "change_status_date_id",tra.to_status_id 	as "to_status_id",
            #                                                                                     COALESCE(mcss.measure2 + tra.measure2, tra.measure2) as measure2_a,
            #                                                                                     COALESCE((mcss.measure1 * mcss.measure2) + (tra.measure1 * tra.measure2), tra.measure1 * tra.measure2) / COALESCE(measure2_a) 	as measure1_a
            #                                                                                     FROM test_rating_avg tra
            #                                                                                     LEFT JOIN mapping_changed_status_student mcss
            #                                                                                     ON mcss.student_id = tra.student_id
            #                                                                                     AND mcss.change_status_date_id = tra.change_status_date_id
            #                                                                                     AND mcss.to_status_id = 13;
            #
            #                                                                                     DELETE mapping_changed_status_student
            #                                                                                     WHERE mapping_changed_status_student.id in (SELECT id FROM temp_join_rating_old_new WHERE id is not null);
            #
            #                                                                                     INSERT INTO mapping_changed_status_student(student_id, change_status_date_id, to_status_id, measure1, measure2)
            #                                                                                     SELECT student_id, change_status_date_id, to_status_id, measure1_a, measure2_a
            #                                                                                     FROM temp_join_rating_old_new;
            #
            #                                                                                     DROP TABLE IF EXISTS temp_join_rating_old_new, test_rating_avg;"""},
            #                                                            redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
            #                                                            transformation_ctx="datasink4")

            # end

            # So lan rating 1 2 3 4 5* cua tung hoc vien theo tung ngay
            # start

            # count_rating_star = data_rating_class_logsservice_move_user.toDF()

            df_mdl_rating_class = df_mdl_rating_class.groupby('student_id', 'mapping_id_time', 'points').agg(
                f.count('points'))

            df_mdl_rating_class.printSchema()

            df_mdl_rating_class = df_mdl_rating_class.withColumn('to_status_id', when(
                df_mdl_rating_class['points'] == 1, f.lit(20)).when(
                df_mdl_rating_class['points'] == 2, f.lit(21)).when(
                df_mdl_rating_class['points'] == 3, f.lit(22)).when(
                df_mdl_rating_class['points'] == 4, f.lit(23)).when(
                df_mdl_rating_class['points'] == 5, f.lit(24)).otherwise(f.lit(20)))

            df_data_count_rating_star = DynamicFrame.fromDF(df_mdl_rating_class,
                                                            glueContext, "datasource0")

            applymapping_count_rating = ApplyMapping.apply(frame=df_data_count_rating_star,
                                                           mappings=[("student_id", "string", "student_id", "bigint"),
                                                                     ("mapping_id_time", "string", "change_status_date_id",
                                                                      "int"),
                                                                     ("points", "int", "points", "int"),
                                                                     ("to_status_id", "int", "to_status_id", "int"),
                                                                     ("teacher_id", "string", "teacher_id", "bigint"),
                                                                     ("count(points)", "long", "measure", "int")])

            resolvechoice_count_rating = ResolveChoice.apply(frame=applymapping_count_rating, choice="make_cols",
                                                             transformation_ctx="resolvechoice_count_rating")

            dropnullfields_count_rating_star = DropNullFields.apply(frame=resolvechoice_count_rating,
                                                                    transformation_ctx="dropnullfields3")

            print(dropnullfields_count_rating_star.count())
            dropnullfields_count_rating_star.toDF().show()

            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_count_rating_star,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "test_rating_star_temp",
                                                                           "database": "dts_odin",
                                                                           "postactions": """DROP TABLE IF EXISTS temp_rating_star;
                                                                                                CREATE TABLE temp_rating_star AS
                                                                                                SELECT mcss.id as "id", trs.student_id as "student_id", trs.change_status_date_id as "change_status_date_id", trs.to_status_id as "to_status_id", trs.points as "points",
                                                                                                COALESCE(mcss.measure2 + trs.measure, trs.measure) as "measure2_a", trs.teacher_id as "teacher_id"
                                                                                                FROM test_rating_star_temp trs
                                                                                                LEFT JOIN mapping_changed_status_student mcss ON mcss.student_id = trs.student_id
                                                                                                AND mcss.change_status_date_id = trs.change_status_date_id
                                                                                                AND mcss.to_status_id IN (20,21,22,23,24);
                                                                                                DELETE mapping_changed_status_student
                                                                                                WHERE mapping_changed_status_student.id in (SELECT id FROM temp_rating_star WHERE id is not null);
                                                                                                INSERT INTO mapping_changed_status_student(student_id, change_status_date_id, to_status_id, measure1, measure2, teacher_id)
                                                                                                SELECT student_id, change_status_date_id, to_status_id, points, measure2_a, teacher_id
                                                                                                FROM temp_rating_star;
                                                                                                DROP TABLE IF EXISTS temp_rating_star;
                                                                                                DROP TABLE IF EXISTS test_rating_star_temp;"""},
                                                                       redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
                                                                       transformation_ctx="datasink4")

            # end

            # ghi flag
            # lay max key trong data source
            mdl_rating_class_tmp = mdl_rating_class.toDF()
            flag = mdl_rating_class_tmp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_rating_class.parquet", mode="overwrite")
        except Exception as e:
            print("No new data")
            print(e)

if __name__ == "__main__":
    main()
