import sys
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import collect_list
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
from pyspark.sql.types import StringType
import pyspark.sql.functions as f
from datetime import date, datetime, timedelta
import pytz

def main():
    is_dev = True
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    def concaText(student_behavior_date, behavior_id, student_id, contact_id,
                package_code, package_endtime,package_starttime,
                student_level_code, student_status_code, transformed_at):
        text_concat = ""
        if student_behavior_date is not None:
            text_concat += str(student_behavior_date)
        if behavior_id is not None:
            text_concat += str(behavior_id)
        if student_id is not None:
            text_concat += str(student_id)
        if contact_id is not None:
            text_concat += str(contact_id)
        if package_code is not None:
            text_concat += str(package_code)
        if package_endtime is  not None:
            text_concat += str(package_endtime)
        if package_starttime is not None:
            text_concat += str(package_starttime)
        if student_level_code is not None:
            text_concat += str(student_level_code)
        if student_status_code is not None:
            text_concat += str(student_status_code)
        if transformed_at is not None:
            text_concat += str(transformed_at)
        return text_concat

    concaText = f.udf(concaText, StringType())

    # ----------------------------------------------DYF-----------------------------------------------------------------#
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                        table_name="student_contact")

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id','student_id' ,'user_name','level_study'])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                   and x["user_name"] is not None and x["user_name"] != '')

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.drop_duplicates()

    df_student_contact.show(3)
    # ---------------------------------------------DYF------------------------------------------------------------------#

    dyf_student_package = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
                                                                        table_name="student_package")

    print('dyf_student_package__0')
    dyf_student_package.printSchema()

    dyf_student_package = dyf_student_package \
        .select_fields(['student_id', 'package_code', 'start_time', 'end_time']) \
        .rename_field('student_id', 'student_id_pk')

    dyf_student_package = dyf_student_package.resolveChoice(
        specs=[('start_time', 'cast:long'), ('end_time', 'cast:long')])

    df_student_package = dyf_student_package.toDF()

    # -------------------------------------------------------------------------------------------------------------------#

    # ----------------------------------------------DYF-----------------------------------------------------------------#
    dyf_student_package_status = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
                                                                               table_name="student_status")

    dyf_student_package_status = dyf_student_package_status \
        .select_fields(['contact_id', 'status_code', 'start_date', 'end_date']) \
        .rename_field('contact_id', 'contact_id_ps')

    print('dyf_student_package_status::drop_duplicates')

    df_student_package_status = dyf_student_package_status.toDF()
    # ------------------------------------------------------------------------------------------------------------------#


    # --------------------------------------------DYF--------------------------------------------------------------------#
    dyf_ghinhan_hp = glueContext.create_dynamic_frame.from_catalog(
        database="poss",
        table_name="ghinhan_hp"
    )
    dyf_ghinhan_hp = dyf_ghinhan_hp \
        .select_fields(['ngay_thanhtoan', 'khoa_hoc_makh', 'trang_thai','so_tien']) \
        # .rename_field('contact_id', 'contact_id_ps')
    dyf_ghinhan_hp = Filter.apply(frame=dyf_ghinhan_hp,
                                       f=lambda x: x["trang_thai"] == 1 )
    df_ghinhan_hp = dyf_ghinhan_hp.toDF()
    # -------------------------------------------------------------------------------------------------------------------#

    # --------------------------------------------DYF--------------------------------------------------------------------#
    dyf_khoa_hoc = glueContext.create_dynamic_frame.from_catalog(
        database="poss",
        table_name="khoa_hoc"
    )
    dyf_khoa_hoc = dyf_khoa_hoc \
        .select_fields(['makh', 'mahv', 'trang_thai']) \
        # .rename_field('contact_id', 'contact_id_ps')
    dyf_khoa_hoc = Filter.apply(frame=dyf_khoa_hoc,
                                  f=lambda x: x["trang_thai"] == 1)
    df_khoa_hoc = dyf_khoa_hoc.toDF()
    # -------------------------------------------------------------------------------------------------------------------#

    # --------------------------------------------DYF--------------------------------------------------------------------#
    dyf_hoc_vien = glueContext.create_dynamic_frame.from_catalog(
        database="poss",
        table_name="hoc_vien"
    )
    dyf_hoc_vien = dyf_hoc_vien \
        .select_fields(['mahv', 'crm_id', 'trang_thai']) \
        .rename_field('mahv', 'mahv_hv')
    dyf_hoc_vien = Filter.apply(frame=dyf_hoc_vien,
                                  f=lambda x: x["trang_thai"] == 1)
    df_hoc_vien = dyf_hoc_vien.toDF()
    # -------------------------------------------------------------------------------------------------------------------#


    # # # doc flag tu s3
    # df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_ai_study_step_MICRO_AIT_AIP.parquet")
    # max_key = df_flag.collect()[0]['flag']
    # print("max_key: ", max_key)
    # dyf_ai_study_step = Filter.apply(frame=dyf_ai_study_step,
    #                                 f=lambda x: x["_key"] > max_key)




    ############___________________join______________############################################

    df_join = df_ghinhan_hp.join(df_khoa_hoc,df_ghinhan_hp.khoa_hoc_makh == df_khoa_hoc.makh)

    df_join_2 = df_join.join(df_hoc_vien, df_join.mahv == df_hoc_vien.mahv_hv)



    df_join_3 = df_student_contact.join(df_join_2,df_student_contact.contact_id == df_join_2.crm_id)
    print('df_join_3')
    df_join_3.printSchema()
    df_join_3_1 = df_join_3.groupby('contact_id').agg(
        f.count('contact_id').alias("count_ct")
    )
    df_join_3_1.printSchema()
    df_join_3_1 = df_join_3_1.withColumnRenamed('contact_id', 'contact_id_3_1')
    df_join_3_1 = df_join_3_1.filter(df_join_3_1.count_ct > 1)

    df_join_4 = df_join_3_1.join(df_join_3,df_join_3_1.contact_id_3_1 == df_join_3.contact_id)
    df_join_4 = df_join_4.withColumn('student_behavior_date',unix_timestamp('ngay_thanhtoan', 'yyyy-MM-dd'))
    df_join_4 = df_join_4.withColumn('behavior_id', f.lit(789L))
    df_join_4 = df_join_4.drop_duplicates()
    df_join_4.show(3)
    if is_dev:
        # df_join_5 = df_join_4.join(df_student_package,
        #                                     (df_student_package.student_id_pk == df_join_4.student_id))
        #
        #
        #
        # df_join_6 = df_join_5.join(df_student_package_status,
        #                                            (df_student_package_status.contact_id_ps == df_join_5.contact_id))



        print('join enddddddddddddddddd')
        # --------------------------------------JOIN_END--------------------------------------------------------------------#

        package_starttime_unavailable = 0L
        package_code_unavailable = 'UNAVAILABLE'
        package_endtime_unavailable = 0L
        student_status_code_unavailable = 'UNAVAILABLE'
        student_level_code_unavailable = 'UNAVAILABLE'
        measure2_unavailable = float(0.0)
        measure3_unavailable = float(0.0)
        measure4_unavailable = float(0.0)
        join_result = df_join_4.select(
            'student_behavior_date',
            'behavior_id',
            'student_id',
            'contact_id',
            f.lit(package_code_unavailable).alias('package_code'),
            f.lit(package_endtime_unavailable).alias('package_endtime'),
            f.lit(package_starttime_unavailable).alias('package_starttime'),
            f.lit(student_status_code_unavailable).alias('student_status_code'),
            f.lit(student_level_code_unavailable).alias('student_level_code'),
            f.lit(today_second).cast('long').alias('transformed_at'),

            #--------------------student_general_behavior---------------------------------------------------------------#
            df_join_4.so_tien.cast('float').alias('measure1'),
            f.lit(measure2_unavailable).alias('measure2'),
            f.lit(measure3_unavailable).alias('measure3'),
            f.lit(measure4_unavailable).alias('measure4'),
        )

        join_result = join_result.withColumn('student_behavior_id',
                                             f.md5(concaText(
                                                 join_result.student_behavior_date,
                                                 join_result.behavior_id,
                                                 join_result.student_id,
                                                 join_result.contact_id,
                                                 join_result.package_code,
                                                 join_result.package_endtime,
                                                 join_result.package_starttime,
                                                 join_result.student_level_code,
                                                 join_result.student_status_code,
                                                 join_result.transformed_at)))

        join_result.cache()



        print('join_result')
        join_result.printSchema()
        join_result = join_result.drop_duplicates()


        dyf_join_result = DynamicFrame.fromDF(join_result, glueContext, 'dyf_join_result')

        dyf_join_result = Filter.apply(frame=dyf_join_result,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')
        # ----------------------------_mapping_student_behavior_------------------------------------------------------#
        apply_ouput = ApplyMapping.apply(frame=dyf_join_result,
                                         mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                                   ("student_behavior_date", "long", "student_behavior_date", "long"),
                                                   ("behavior_id", "long", "behavior_id", "long"),
                                                   ("student_id", "string", "student_id", "long"),
                                                   ("contact_id", "string", "contact_id", "string"),
                                                   ("package_endtime", "long", "package_endtime", "long"),
                                                   ("package_starttime", "long", "package_starttime", "long"),
                                                   ("package_code", "string", "package_code", "string"),
                                                   ("student_status_code", "string", "student_status_code", "string"),
                                                   ("student_level_code", "string", "student_level_code", "string"),
                                                   ("transformed_at", "long", "transformed_at", "long")
                                                   ])

        dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")
        df_output = dfy_output.toDF().drop_duplicates()
        df_output.show(3)
        dyf_output = DynamicFrame.fromDF(df_output, glueContext, 'dyf_output')


        glueContext.write_dynamic_frame.from_options(
            frame=dyf_output,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")
        print('--------_mapping_student_behavior_---------_WRITE_TO_S3_COMPLETE_______________________________________')

        # ----------------------------student_general_behavior------------------------------------------------------#

        apply_general = ApplyMapping.apply(frame=dyf_join_result,
                                           mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                                     ("measure1", "float", "measure1", "float"),
                                                     ("measure2_unavailable", "float", "measure2", "float"),
                                                     ("measure3_unavailable", "float", "measure3", "float"),
                                                     ("measure4_unavailable", "float", "measure4", "float"),
                                                     ("behavior_id", "long", "behavior_id", "long")
                                                     ])

        dfy_output2 = ResolveChoice.apply(frame=apply_general, choice="make_cols", transformation_ctx="resolvechoice2")

        print('dfy_output2::')
        dfy_output2.show(5)

        glueContext.write_dynamic_frame.from_options(
            frame=dfy_output2,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_general_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")

        # # ghi flag
        # # lay max key trong data source
        # dyf_ai_study_step_tmp = dyf_ai_study_step.toDF()
        # flag = dyf_ai_study_step_tmp.agg({"_key": "max"}).collect()[0][0]
        #
        # flag_data = [flag]
        # df = spark.createDataFrame(flag_data, "long").toDF('flag')
        #
        # # ghi de _key vao s3
        # df.write.parquet("s3a://dts-odin/flag/flag_ai_study_step_MICRO_AIT_AIP.parquet", mode="overwrite")

        print('--------_mapping_student_test_detail_---------_WRITE_TO_S3_COMPLETE_______________________________________')

if __name__ == "__main__":
    main()
