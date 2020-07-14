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
        ['contact_id', 'student_id', 'user_name','level_study'])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                   and x["student_id"] is not None and x["student_id"] != ''
                                                   and x["user_name"] is not None and x["user_name"] != '')

    df_student_contact = dyf_student_contact.toDF()
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
    df_student_package = df_student_package.drop_duplicates()
    # -------------------------------------------------------------------------------------------------------------------#

    # ----------------------------------------------DYF-----------------------------------------------------------------#
    dyf_student_package_status = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
                                                                               table_name="student_status")

    dyf_student_package_status = dyf_student_package_status \
        .select_fields(['contact_id', 'status_code', 'start_date', 'end_date']) \
        .rename_field('contact_id', 'contact_id_ps')

    print('dyf_student_package_status::drop_duplicates')

    df_student_package_status = dyf_student_package_status.toDF()
    df_student_package_status = df_student_package_status.drop_duplicates()
    # ------------------------------------------------------------------------------------------------------------------#


    # --------------------------------------------DYF--------------------------------------------------------------------#
    dyf_ai_study_step = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study_step"
    )

    dyf_ai_study_step = dyf_ai_study_step.select_fields(
        ['_key', 'user_id', 'session_id', 'tag', 'total_step', 'received_point','started_at', 'ended_at', 'current_step',
         'lc', 'lu', 'learning_object_type', 'lo', 'learning_object', 'correct_answer', 'student_answer',
         'student_answer_details', 'duration', 'max_point', 'created_at']
    )

    # # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_ai_study_step_MICRO_AIT_AIP.parquet")
    max_key = df_flag.collect()[0]['flag']
    print("max_key: ", max_key)
    dyf_ai_study_step = Filter.apply(frame=dyf_ai_study_step,
                                    f=lambda x: x["_key"] > max_key)

    # -------------------------------------------------------------------------------------------------------------------#



    if(dyf_ai_study_step.count()>0):

        dyf_ai_study_step = Filter.apply(frame = dyf_ai_study_step,
                                         f = lambda x: x['tag'] == 'micro'
                                                       or x['tag'] == 'ait'
                                                       or (x['tag'] == 'aip'
                                                           and (x['total_step'] == 15 or x['total_step'] == 30))
                                         )

        df_ai_study_step = dyf_ai_study_step.toDF()

        # df_ai_study_step = df_ai_study_step.limit(10)

        df_part_of_ai_study_step = df_ai_study_step.groupby('tag','user_id','session_id').agg(
            f.sum('duration').alias("total_duration"),
            f.min('started_at').alias("min_started_at"),
            f.max('ended_at').alias("max_ended_at"),
            f.min('created_at').alias("min_created_at")
        )

        df_part_of_ai_study_step = df_part_of_ai_study_step \
            .withColumnRenamed('user_id', 'user_id_part') \
            .withColumnRenamed('session_id', 'session_id_part')

        df_ai_study_step = df_ai_study_step.withColumn('behavior_id', f.when(df_ai_study_step.tag == 'micro', f.lit(18L)).
                                                       when(df_ai_study_step.tag == 'ait', f.lit(19L)).
                                                       when((df_ai_study_step.tag == 'aip')
                                                              &(df_ai_study_step.total_step == 15), f.lit(20L)).
                                                       when((df_ai_study_step.tag == 'aip')
                                                            &(df_ai_study_step.total_step == 30), f.lit(21L)).
                                                       otherwise(f.lit(0L)))
        df_ai_study_step_2 = df_ai_study_step;
        df_ai_study_step_2 = df_ai_study_step_2.drop('tag')


        ############___________________join______________############################################

        df_join_2 = df_ai_study_step_2.join(df_part_of_ai_study_step,
                                          (df_part_of_ai_study_step.user_id_part == df_ai_study_step_2.user_id)
                                          & (df_part_of_ai_study_step.session_id_part == df_ai_study_step_2.session_id),
                                          'left'
                                          )

        print('df_join')
        print('number::df_join: ', df_join_2.count())

        # df_join_2 = df_student_package.join(df_join,
        #                                     (df_student_package.student_id_pk == df_join.user_id)
        #                                     & (df_student_package.start_time < df_join.min_started_at)
        #                                     & (df_student_package.end_time > df_join.min_started_at))
        #
        # print('df_join-2')
        # print('number::df_join_2: ', df_join_2.count())

        df_join_4 = df_student_contact.join(df_join_2,
                                            (df_student_contact.student_id == df_join_2.user_id))
        print('df_join_3')
        print('number::df_join_3: ', df_join_4.count())

        # df_join_4 = df_student_package_status.join(df_join_3,
        #                                            (df_student_package_status.contact_id_ps == df_join_3.contact_id))
        print('df_join_4')
        print('number::df_join: ', df_join_4.count())
        df_join_4.drop_duplicates().show(1)

        print('join enddddddddddddddddd')
        # --------------------------------------JOIN_END--------------------------------------------------------------------#
        df_join_4.printSchema()

        learning_object_id_unavailable = 0L
        attempt_step_id_unavailable = 0L
        source_system = 'ENGLISH_DAILY'
        result_unavailable = 'UNAVAILABLE'
        right_answer_unavailable = 'UNAVAILABLE'
        wrong_answer_unavailable = 'UNAVAILABLE'
        learning_category_code_unavailable = 'UNAVAILABLE'
        package_starttime_unavailable = 0L
        package_code_unavailable = 'UNAVAILABLE'
        package_endtime_unavailable = 0L
        student_status_code_unavailable = 'UNAVAILABLE'
        student_level_code_unavailable = 'UNAVAILABLE'
        measure2_unavailable = float(0.0)
        measure3_unavailable = float(0.0)
        measure4_unavailable = float(0.0)
        join_result = df_join_4.select(
            df_join_4.min_started_at.cast('long').alias('student_behavior_date'),
            'behavior_id',
            df_join_4.user_id.cast('long').alias('student_id'),
            'contact_id',
            f.lit(package_code_unavailable).alias('package_code'),
            f.lit(package_endtime_unavailable).alias('package_endtime'),
            f.lit(package_starttime_unavailable).alias('package_starttime'),
            f.lit(student_status_code_unavailable).alias('student_status_code'),
            f.lit(student_level_code_unavailable).alias('student_level_code'),

            f.lit(today_second).cast('long').alias('transformed_at'),

            #--------------------student_general_behavior---------------------------------------------------------------#
            df_join_4.total_duration.cast('long').alias('measure1'),
            f.lit(measure2_unavailable).alias('measure2'),
            f.lit(measure3_unavailable).alias('measure3'),
            f.lit(measure4_unavailable).alias('measure4'),

            #---------------------------student_test_detail-------------------------------------------------------#
            f.lit(source_system).alias('source_system'),
            df_join_4.tag.alias('test_type'),
            f.lit(attempt_step_id_unavailable).alias('attempt_step_id'),
            df_join_4.current_step.cast('long').alias('current_step'),
            df_join_4.total_step.cast('long').alias('total_step'),
            df_join_4.session_id.alias('attempt_id'),
            f.lit(learning_category_code_unavailable).alias('learning_category_code'),
            df_join_4.lc.cast('long').alias('learning_category_id'),
            f.lit(learning_object_id_unavailable).alias('learning_object_id'),
            df_join_4.lu.alias('learning_unit_code'),
            df_join_4.learning_object_type.alias('learning_object_type_code'),
            df_join_4.lo.alias('learning_object_code'),
            'learning_object',
            'correct_answer',
            'student_answer',
            df_join_4.student_answer_details.alias('student_answer_detail'),
            f.lit(result_unavailable).alias('result'),
            f.lit(right_answer_unavailable).alias('right_answer'),
            f.lit(wrong_answer_unavailable).alias('wrong_answer'),
            df_join_4.duration.cast('long').alias('duration'),
            df_join_4.max_point.cast('long').alias('max_point'),
            df_join_4.received_point.cast('long').alias('received_point'),
            'created_at',
            'behavior_id'
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
                                                   ("student_id", "long", "student_id", "long"),
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
        dyf_output = DynamicFrame.fromDF(df_output, glueContext, 'dyf_output')

        glueContext.write_dynamic_frame.from_options(
            frame=dyf_output,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")
        print('--------_mapping_student_behavior_---------_WRITE_TO_S3_COMPLETE_______________________________________')

        #----------------------------_mapping_student_test_detail_------------------------------------------------------#

        apply_mapping_student_test_detail = ApplyMapping.apply(frame=dyf_join_result,

                                         mappings=[("source_system", "string", "source_system", "string"),
                                                   ("test_type", "string", "test_type", "string"),
                                                   ("attempt_step_id", "long", "attempt_step_id", "long"),
                                                   ("current_step", "long", "current_step", "long"),
                                                   ("total_step", "long", "total_step", "long"),
                                                   ("attempt_id", "string", "attempt_id", "string"),
                                                   ("learning_category_id", "long", "learning_category_id", "long"),
                                                   ("learning_category_code", "string", "learning_category_code","string"),
                                                   ("learning_unit_code", "string", "learning_unit_code", "string"),
                                                   ("learning_object_type_code", "string", "learning_object_type_code", "string"),
                                                   ("learning_object_id", "long", "learning_object_id", "long"),
                                                   ("learning_object_code", "string", "learning_object_code", "string"),
                                                   ("learning_object", "string", "learning_object", "string"),
                                                   ("correct_answer", "string", "correct_answer", "string"),
                                                   ("student_answer", "string", "student_answer", "string"),
                                                   ("student_answer_detail", "string", "student_answer_detail", "string"),
                                                   ("result", "string", "result", "string"),
                                                   ("right_answer", "string", "right_answer", "string"),
                                                   ("wrong_answer", "string", "wrong_answer", "string"),
                                                   ("duration", "long", "duration", "long"),
                                                   ("max_point", "long", "max_point", "long"),
                                                   ("received_point", "long", "received_point", "long"),
                                                   ("created_at", "string", "created_at", "string"),
                                                   ("behavior_id", "long", "behavior_id", "long"),
                                                   ("student_behavior_id", "string", "student_behavior_id", "string")])


        dyf_mapping_student_test_detail = ResolveChoice.apply(frame=apply_mapping_student_test_detail, choice="make_cols", transformation_ctx="resolvechoice2")
        print('dyf_mapping_student_test_detail--------------check')
        df_mapping_student_test_detail = dyf_mapping_student_test_detail.toDF()
        df_mapping_student_test_detail = df_mapping_student_test_detail.drop_duplicates()
        dyf_mapping_student_test_detail_v2 = DynamicFrame.fromDF(df_mapping_student_test_detail, glueContext, 'dyf_mapping_student_test_detail_v2')


        glueContext.write_dynamic_frame.from_options(
            frame=dyf_mapping_student_test_detail_v2,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_test_detail",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")
        # ghi flag
        # lay max key trong data source
        dyf_ai_study_step_tmp = dyf_ai_study_step.toDF()
        flag = dyf_ai_study_step_tmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_ai_study_step_MICRO_AIT_AIP.parquet", mode="overwrite")

        print('--------_mapping_student_test_detail_---------_WRITE_TO_S3_COMPLETE_______________________________________')

if __name__ == "__main__":
    main()
