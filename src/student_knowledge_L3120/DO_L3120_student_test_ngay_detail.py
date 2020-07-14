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
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
from pyspark.sql.functions import udf
import json
import random


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # TBHV E ngay

    ########## dyf_ai_study_step
    dyf_ai_study_step = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study_step"
    )

    dyf_ai_study_step = dyf_ai_study_step.select_fields(
        ['_key','tag', 'user_id', 'session_id', 'learning_object', 'learning_object_type', 'student_answer_details', 'correct_answer', 'student_answer', 'duration',
         'max_point', 'received_point', 'current_step', 'total_step', 'lc', 'lu', 'lo', 'created_at'])
    dyf_ai_study_step = dyf_ai_study_step.resolveChoice(
        specs=[('_key', 'cast:long')])
    dyf_ai_study_step.printSchema()
    dyf_ai_study = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study"
    )
    dyf_ai_study = dyf_ai_study.select_fields(['tag', 'session_id'])
    dyf_ai_study.printSchema()


    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_ai_study_step_st_detail.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print('read from index: ', max_key)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_ai_study_step = Filter.apply(frame=dyf_ai_study_step, f=lambda x: x['_key'] > max_key)
    # except:
    #     print('read flag error ')

    print('the number of new contacts: ', dyf_ai_study_step.count())
    # chon cac truong can thiet
    if (dyf_ai_study_step.count() > 0):

        dyf_ai_study_step = Filter.apply(frame = dyf_ai_study_step, f = lambda x: x['user_id'] is not None and x['session_id'] is not None)
        join_study = Join.apply(dyf_ai_study_step, dyf_ai_study, 'session_id', 'session_id')
        join_study.printSchema()

        df_join_study = join_study.toDF()
        df_join_study = df_join_study.withColumn('source_system', f.lit('E ngay'))\
                                    .withColumn('attempt_step_id', f.lit(None))\
                                    .withColumn('result', f.lit(None))\
                                    .withColumn('right_answer', f.lit(None))\
                                    .withColumn('wrong_answer', f.lit(None))
        join_study = DynamicFrame.fromDF(df_join_study, glueContext, 'join_study')
        join_study = join_study.resolveChoice( specs=[('attempt_step_id', 'cast:int'), ('result', 'cast:int'),
                                                      ('right_answer', 'cast:int'), ('wrong_answer', 'cast:int')])
        join_study.printSchema()
        join_study.show(2)
        applymapping1 = ApplyMapping.apply(frame=join_study,
                                           mappings=[("user_id", 'string', 'student_id', 'int'),
                                                     ("lo", "string", "learning_object_code", "string"),
                                                     ("session_id", "string", "attempt_id", "string"),
                                                     ("tag", "string", "test_type", "string"),
                                                     ("current_step", "int", "current_step", "int"),
                                                     ("total_step", "int", "total_step", "int"),
                                                     ("lc", "int", "learning_category_id", "int"),
                                                     ("lu", "string", "learning_unit_code", "string"),
                                                     ("learning_object_type", "string", "learning_object_type_code", "string"),
                                                     ("learning_object", "string", "learning_object", "string"),
                                                     ("correct_answer", "string", "correct_answer", "string"),
                                                     ("student_answer", "string", "student_answer", "string"),
                                                     ("student_answer_details", "string", "student_answer_detail", "string"),
                                                     ("duration", "int", "duration", "int"),
                                                     ("max_point", "int", "max_point", "int"),
                                                     ("received_point", "int", "received_point", "int"),
                                                     ("source_system", "string", "source_system", "string"),
                                                     ("attempt_step_id", "int", "attempt_step_id", "int"),
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


        df_temp = dyf_ai_study_step.toDF()
        flag = df_temp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_ai_study_step_st_detail.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
