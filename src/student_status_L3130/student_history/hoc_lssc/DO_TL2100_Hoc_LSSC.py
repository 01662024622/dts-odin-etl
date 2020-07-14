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
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    # ETL trang thai Co hoc LS/SC; co hoc thanh cong LS/SC:
    ############# lay du lieu bang mdl_logsservice_in_out
    mdl_logsservice_in_out = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                           table_name="mdl_logsservice_in_out_cutoff")

    # Chon cac truong can thiet
    mdl_logsservice_in_out = mdl_logsservice_in_out.select_fields(
        ['_key', 'id', 'userid', 'roomid', 'time_in', 'time_out', 'date_in', 'action'])
    mdl_logsservice_in_out = mdl_logsservice_in_out.resolveChoice(specs=[('_key', 'cast:long')])

    df_flag_1 = spark.read.parquet("s3://dts-odin/flag/flag_LS_LSSC_CutOff.parquet")
    max_key = df_flag_1.collect()[0]['flag']
    print("max_key:  ", max_key)
    # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    mdl_logsservice_in_out = Filter.apply(frame=mdl_logsservice_in_out, f=lambda x: x["_key"] > max_key)
    # data = mdl_logsservice_in_out.toDF()
    # data = data.cacahe()
    # mdl_logsservice_in_out = DynamicFrame.fromDF(data, glueContext, "mdl_logsservice_in_out")
    print("Count data 1:  ", mdl_logsservice_in_out.count())
    # mdl_logsservice_in_out.toDF().show()
    if (mdl_logsservice_in_out.count() > 0):
        try:
            mdl_tpebbb = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                       table_name="mdl_tpebbb")
            mdl_tpebbb = mdl_tpebbb.select_fields(['id', 'timeavailable', 'calendar_code', 'roomtype']).rename_field('id',
                                                                                                                     'room_id')
            mdl_tpe_calendar_teach = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                                   table_name="mdl_tpe_calendar_teach")
            mdl_tpe_calendar_teach = mdl_tpe_calendar_teach.select_fields(['status', 'calendar_code', 'type_class']).rename_field(
                'calendar_code', 'code_calendar')

            mdl_logsservice_room_start = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                                       table_name="mdl_logsservice_room_start")
            mdl_logsservice_room_start = mdl_logsservice_room_start.select_fields(['roomid', 'timecreated']).rename_field(
                'roomid', 'id_room')

            mdl_role_assignments = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                                 table_name="mdl_role_assignments")
            mdl_role_assignments = mdl_role_assignments.select_fields(['userid', 'roleid']).rename_field('userid',
                                                                                                         'user_id')

            # Loc du lieu
            mdl_tpe_calendar_teach = Filter.apply(frame=mdl_tpe_calendar_teach, f=lambda x: x["status"] >= 0)

            data_tpe_bbb = Filter.apply(frame=mdl_tpebbb, f=lambda x: x["roomtype"] == 'ROOM' and (
                        x["calendar_code"] is not None and x["calendar_code"] != ''))

            join_calendar_teach = Join.apply(data_tpe_bbb, mdl_tpe_calendar_teach, 'calendar_code',
                                             'code_calendar').drop_fields(['calendar_code', 'code_calendar'])

            data_in_out = Filter.apply(frame=mdl_logsservice_in_out, f=lambda x: x["time_out"] is not None and (
                        x["userid"] is not None and x["userid"] != '') and (x["roomid"] is not None and x["roomid"] != ''))

            data_mdl_role_assignments = Filter.apply(frame=mdl_role_assignments,
                                                     f=lambda x: x["roleid"] == '5' and x["user_id"] is not None)

            join_data_role = Join.apply(data_in_out, data_mdl_role_assignments, 'userid', 'user_id')

            # map ls lssc vs thong tin lop
            join_data_tpebbb = Join.apply(join_data_role, join_calendar_teach, 'roomid', 'room_id')

            mdl_logsservice_room_start = Filter.apply(frame=mdl_logsservice_room_start,
                                                      f=lambda x: x["id_room"] is not None and x["id_room"] != '')

            df_data_roomstart = mdl_logsservice_room_start.toDF()
            df_data_tpebbb = join_data_tpebbb.toDF()
            print("Count data 222:  ", df_data_tpebbb.count())
            # df_data_tpebbb.show()
            # map ls lssc vs thong tin mo lop
            join_bbb = df_data_tpebbb.join(df_data_roomstart, df_data_tpebbb.roomid == df_data_roomstart.id_room,
                                           'left_outer')

            data_bbb = DynamicFrame.fromDF(join_bbb, glueContext, "data_bbb")

            # convert data
            df_bbb = data_bbb.toDF()
            df_bbb = df_bbb.withColumn('time_start', when(f.col("timecreated").isNull(), df_bbb['timeavailable']).otherwise(
                df_bbb['timecreated']))
            df_bbb = df_bbb.withColumn('timein', when(df_bbb.time_in < df_bbb.time_start, df_bbb['time_start']).otherwise(
                df_bbb['time_in']))
            df_bbb = df_bbb.withColumn('time_study',
                                       when((df_bbb.time_out < df_bbb.time_in) | (df_bbb.time_out < df_bbb.time_start),
                                            f.lit(0)).otherwise(df_bbb.time_out - df_bbb.timein)) \
                .withColumn('id_time', from_unixtime(unix_timestamp(df_bbb.date_in, "yyyy-MM-dd"), "yyyyMMdd")) \
                .withColumn('date_login', from_unixtime(df_bbb.timein)) \
                .withColumn('date_logout', from_unixtime(df_bbb.time_out))

            # df_bbb.cache()
            data_lssc_bbb = DynamicFrame.fromDF(df_bbb, glueContext, "data_lssc_bbb")

            data_lssc_bbb = data_lssc_bbb.resolveChoice(specs=[('time_study', 'cast:long')])
            data_lssc_bbb.printSchema()
            # chon cac truong va kieu du lieu day vao db
            applymapping = ApplyMapping.apply(frame=data_lssc_bbb,
                                              mappings=[("id", "string", "id", "bigint"),
                                                        ("userid", "string", "student_id", "string"),
                                                        ("roomid", 'string', 'room_id', 'string'),
                                                        ("id_time", 'string', 'date_id', 'bigint'),
                                                        ("date_login", "string", "time_in", "timestamp"),
                                                        ("date_logout", "string", "time_out", "timestamp"),
                                                        ("time_study", "long", "time_study", "long"),
                                                        ("type_class", "string", "class_type", "string"),
                                                        ("date_in", "string", "created_time", "timestamp"),
                                                        ("action", "string", "action", "string")])

            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

            print("Count data:  ", dropnullfields.count())
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "fact_lich_su_hoc",
                                                                           "database": "dts_odin",
                                                                           "postactions": """ call proc_insert_lssc_thanh_cong_dau_tien()"""
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_logsservice_in_out/",
                                                                       transformation_ctx="datasink5")


            df_lssc = dropnullfields.toDF()
            # luu bang chuyen trang thai co hoc ls/sc:
            print("Count data data_df_lsscStudy:  ")
            df_lssc = df_lssc.groupby('student_id', 'room_id', 'date_id', 'class_type').agg(f.sum('time_study').alias("measure2_tmp"), f.count('room_id').alias("measure1"))
            df_lssc = df_lssc.withColumn('to_status_id',
                                         when(df_lssc.class_type == 'LS', f.lit(30)).otherwise(f.lit(31)))
            df_lssc = df_lssc.withColumn('measure2', df_lssc.measure2_tmp/60)
            print('co_hoc_lssc schema1: ')
            df_lssc.printSchema()
            data_df_lsscStudy = DynamicFrame.fromDF(df_lssc, glueContext, "data_df_lsscStudy")
            data_df_lsscStudy = data_df_lsscStudy.resolveChoice(specs=[('measure1', 'cast:double')])
            data_df_lsscStudy = data_df_lsscStudy.resolveChoice(specs=[('measure2', 'cast:double')])
            print('co_hoc_lssc schema: ')
            data_df_lsscStudy.printSchema()
            applymappingStudy = ApplyMapping.apply(frame=data_df_lsscStudy,
                                                     mappings=[("student_id", "string", "student_id", "bigint"),
                                                               ("date_id", "bigint", "change_status_date_id", "bigint"),
                                                               ("to_status_id", "int", "to_status_id", "bigint"),
                                                               ("measure1", 'double', 'measure1', 'double'),
                                                               ("measure2", 'double', 'measure2', 'double')])

            resolvechoiceStudy = ResolveChoice.apply(frame=applymappingStudy, choice="make_cols",
                                                       transformation_ctx="resolvechoiceStudy")
            dropnullfieldsStudy = DropNullFields.apply(frame=resolvechoiceStudy,
                                                         transformation_ctx="dropnullfieldsStudy")
            dropnullfieldsStudy.printSchema()
            # dropnullfieldsStudy.toDF().show()
            # insert trang thai co hoc thanh cong ls hoac sc
            datasinkStudy = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfieldsStudy,
                                                                             catalog_connection="glue_redshift",
                                                                             connection_options={
                                                                                 "dbtable": "temp_mapping_status",
                                                                                 "database": "dts_odin",
                                                                                 "postactions": """ insert into mapping_changed_status_student(student_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                                            select student_id, change_status_date_id, to_status_id, measure1, measure2 from temp_mapping_status;
                                                                                                            update mapping_changed_status_student set user_id = (select user_id from user_map where source_type = 2 and source_id = student_id)
                                                                                                                where user_id is null;
                                                                                                        DROP TABLE IF EXISTS temp_mapping_status
                                                                                                                 """
                                                                             },
                                                                             redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_logsservice_in_out/",
                                                                             transformation_ctx="datasinkStudy")


            # luu bang chuyen trang thai co hoc thanh cong ls/sc:
            # Hoc thanh cong: thoi gian hoc >= 36phut
            df_lssc = dropnullfields.toDF()
            df_lssc = df_lssc.groupby('student_id', 'room_id', 'date_id', 'class_type').agg(f.sum('time_study').alias("sum_time_study"))
            df_lssc = df_lssc.where('sum_time_study >= 2160')
            df_lssc = df_lssc.groupby('student_id', 'date_id', 'class_type').agg(f.sum('sum_time_study').alias("measure2_tmp"), f.count('room_id').alias("measure1"))
            df_lssc = df_lssc.withColumn('to_status_id', when(df_lssc.class_type == 'LS', f.lit(11)).otherwise(f.lit(12)))
            df_lssc = df_lssc.withColumn('measure2', df_lssc.measure2_tmp/60)
            data_df_lssc = DynamicFrame.fromDF(df_lssc, glueContext, "data_df_lssc")

            data_df_lssc = data_df_lssc.resolveChoice(specs=[('measure1', 'cast:double')])
            data_df_lssc = data_df_lssc.resolveChoice(specs=[('measure2', 'cast:double')])
            print('data_df_lssc schema: ')
            data_df_lssc.printSchema()
            applymappingSuccess = ApplyMapping.apply(frame=data_df_lssc,
                                              mappings=[("student_id", "string", "student_id", "bigint"),
                                                        ("date_id", "bigint", "change_status_date_id", "bigint"),
                                                        ("to_status_id", "int", "to_status_id", "bigint"),
                                                        ("measure1", 'double', 'measure1', 'double'),
                                                        ("measure2", 'double', 'measure2', 'double')])

            resolvechoiceSuccess = ResolveChoice.apply(frame=applymappingSuccess, choice="make_cols",
                                                transformation_ctx="resolvechoiceSuccess")
            dropnullfieldsSuccess = DropNullFields.apply(frame=resolvechoiceSuccess, transformation_ctx="dropnullfieldsSuccess")
            dropnullfieldsSuccess.printSchema()
            # dropnullfieldsSuccess.toDF().show()
            ## insert trang thai co hoc thanh cong ls hoac sc
            datasinkSuccess = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfieldsSuccess,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "temp_mapping_status",
                                                                           "database": "dts_odin",
                                                                           "postactions": """ insert into mapping_changed_status_student(student_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                                select student_id, change_status_date_id, to_status_id, measure1, measure2 from temp_mapping_status;
                                                                                                update mapping_changed_status_student set user_id = (select user_id from user_map where source_type = 2 and source_id = student_id)
                                                                                                    where user_id is null;
                                                                                                DROP TABLE IF EXISTS temp_mapping_status
                                                                                                     """
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_logsservice_in_out/",
                                                                       transformation_ctx="datasinkSuccess")


            # data_df_lssc.printSchema()
            # df_lssc = data_df_lssc.toDF()
            # print "Count data:  ", dropnullfields1.count()
            # dropnullfields1.toDF().show()
            # ghi flag
            # lay max key trong data source
            datasourceTmp = mdl_logsservice_in_out.toDF()
            flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_LS_LSSC_CutOff.parquet", mode="overwrite")

        except Exception as e:
            print("No new data")
            print(e)

        # except:
        #     print "No new data"

    ############# mdl_logsservice_in_out_adb

if __name__ == "__main__":
    main()