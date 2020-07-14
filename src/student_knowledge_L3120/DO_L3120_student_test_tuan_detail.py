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
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql.functions import udf
from datetime import datetime

import json
import random

def main():

    def getTimestampType(time_modified):
        time_modified = long(time_modified)
        time_modified = datetime.fromtimestamp(time_modified)
        time_modified = str(time_modified)
        return time_modified

    getTimestampType = udf(getTimestampType, StringType())

    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    dyf_quiz_atm = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_quiz_attempts"
    )
    dyf_quiz_atm = dyf_quiz_atm.select_fields(['_key', 'id', 'userid', 'timemodified', 'uniqueid', 'quiz']).rename_field('id', 'attempt_id')
    dyf_quiz_atm = dyf_quiz_atm.resolveChoice(specs=[('_key', 'cast:long')])

    dyf_question_atm = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_question_attempts"
    )
    dyf_question_atm = dyf_question_atm.select_fields(['id', 'rightanswer', 'responsesummary', 'timemodified', 'maxmark', 'questionusageid', 'questionid']).rename_field('id', 'attempt_step_id')
    # dyf_question_atm = dyf_question_atm.resolveChoice(specs=[('timemodified', 'cast:string')])
    dyf_quiz = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_quiz"
    )
    dyf_quiz = dyf_quiz.select_fields(['name', 'id']).rename_field('id', 'quiz_id')

    dyf_question_steps = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_question_attempt_steps"
    )
    dyf_question_steps = dyf_question_steps.select_fields(['state', 'questionattemptid'])

    dyf_result_ai = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_result_ai"
    )
    dyf_result_ai = dyf_result_ai.select_fields(['id', 'answer', '.speech_result', 'right_word', 'wrong_word', 'result'])

    dyf_top_user = glueContext.create_dynamic_frame.from_catalog(
        database = "moodle",
        table_name = "top_user")
    dyf_top_user = dyf_top_user.select_fields(['id' , 'student_id'])


    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_ai_study_step_st_tuan_detail.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print('read from index: ', max_key)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_quiz_atm  = Filter.apply(frame=dyf_quiz_atm , f=lambda x: x['_key'] > max_key)
    # except:
    #     print('read flag error ')

    print('the number of new contacts: ', dyf_quiz_atm .count())
    # chon cac truong can thiet
    if (dyf_quiz_atm .count() > 0):
        dyf_quiz_atm = Filter.apply(frame = dyf_quiz_atm, f=lambda x: x['userid'] is not None
                                                                      and x['uniqueid'] is not None
                                                                      and x['quiz'] is not None)
        join_01 = Join.apply(dyf_quiz_atm, dyf_question_atm, 'uniqueid', 'questionusageid').drop_fields(['questionusageid'])
        join_01.printSchema()

        join_02 = Join.apply(join_01, dyf_question_steps, 'attempt_step_id', 'questionattemptid')
        join_02 = Join.apply(join_02, dyf_top_user, 'userid', 'id')
        join_02.printSchema()
        df_join_02 = join_02.toDF()
        # df_join_02 = df_join_02.withColumn("source_system", f.lit("Native Test")) \
        #     .withColumn("created_at", unix_timestamp(df_join_02.timemodified, "yyyy-MM-dd HH:mm:ss"))

        df_join_02 = df_join_02.withColumn("source_system", f.lit("Native Test")) \
            .withColumn("created_at", getTimestampType(df_join_02.timemodified))

        join_02 = DynamicFrame.fromDF(df_join_02, glueContext, "join_02")
        # join_02 = join_02.resolveChoice(specs=[('created_at', 'cast:string')])

        join_02.printSchema()
        join_02.show(2)

        join_result_ai_1 = Join.apply(dyf_quiz_atm, dyf_quiz, 'quiz', 'quiz_id').drop_fields(['quiz'])
        join_result_ai = Join.apply(join_result_ai_1, dyf_result_ai, 'attempt_id', 'id')
        join_result_ai = Join.apply(join_result_ai, dyf_top_user, 'userid', 'id' )
        print("TTTTTTTTTTTTTTTTTT")
        join_result_ai.printSchema()
        df_join_result_ai = join_result_ai.toDF()
        # df_join_result_ai = df_join_result_ai.withColumn("source_system", f.lit("Native Test"))\
        #         #                                         .withColumn("created_at", unix_timestamp(df_join_result_ai.timemodified, "yyyy-MM-dd HH:mm:ss"))

        df_join_result_ai = df_join_result_ai.withColumn("source_system", f.lit("Native Test")) \
            .withColumn("created_at", getTimestampType(df_join_result_ai.timemodified))


        join_result_ai = DynamicFrame.fromDF(df_join_result_ai, glueContext, "join_result_ai")

        join_result_ai.printSchema()
        join_result_ai.show(3)

        applymapping1 = ApplyMapping.apply(frame=join_02,
                                           mappings=[("student_id", 'int', 'student_id', 'int'),
                                                     ("attempt_id", "long", "attempt_id", "string"),
                                                     ("rightanswer", "string", "correct_answer", "string"),
                                                     ("responsesummary", "string", "student_answer", "string"),
                                                     ("maxmark", "string", "received_point", "int"),
                                                     ("source_system", "string", "source_system", "string"),
                                                     ("attempt_step_id", "long", "attempt_step_id", "int"),
                                                     ("state", "string", "result", "string"),
                                                     ("created_at", "string", "created_at", "timestamp")
                                                     ])


        resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice1")
        dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")
        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "student_test_detail",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3a://dts-odin/st_detail/",
                                                                   transformation_ctx="datasink5")
        print('START WRITE TO S3-------------------------')

        datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dts-odin/nvn_knowledge/student_test_detail/"},
                                                                 format="parquet",
                                                                 transformation_ctx="datasink6")

        print('END WRITE TO S3-------------------------')

        applymapping2 = ApplyMapping.apply(frame=join_result_ai,
                                           mappings=[("student_id", 'int', 'student_id', 'int'),
                                                     ("attempt_id", "long", "attempt_id", "string"),
                                                     ("name", "string", "test_type", "string"),
                                                     ("answer", "string", "correct_answer", "string"),
                                                     ("speech_result", "string", "student_answer", "string"),
                                                     ("source_system", "string", "source_system", "string"),
                                                     ("id", "int", "attempt_step_id", "int"),
                                                     ("result", "string", "result", "string"),
                                                     ("right_word", "string", "right_answer", "string"),
                                                     ("wrong_word", "string", "wrong_answer", "string"),
                                                     ("created_at", "string", "created_at", "timestamp")
                                                     ])

        resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                             transformation_ctx="resolvechoice1")
        dropnullfields2 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields")
        datasink8 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields2,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "student_test_detail",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3a://dts-odin/st_detail/",
                                                                   transformation_ctx="datasink8")
        print('START WRITE TO S3-------------------------')

        datasink7 = glueContext.write_dynamic_frame.from_options(frame=join_result_ai, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dts-odin/nvn_knowledge/student_test_detail/"},
                                                                 format="parquet",
                                                                 transformation_ctx="datasink7")
        print('END WRITE TO S3-------------------------')


        df_temp = dyf_quiz_atm.toDF()
        flag = df_temp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_ai_study_step_st_tuan_detail.parquet", mode="overwrite")



if __name__ == "__main__":
    main()


