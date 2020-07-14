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
    # LO_TYPE: 1: Tu vung; 2: Ngu am; 3: Nghe; 4: Ngu phap
    # Custom function
    def get_length(array_str):
        json_obj = json.loads(array_str)
        # index = 0;
        # for item in json_obj:
        #     index += 1
        length = 0
        if json_obj is not None:
            length = len(json_obj)
        return length

    udf_get_length = udf(get_length, IntegerType())

    arr_aip_tu_vung = ['3', '4', '5', '17']
    arr_aip_ngu_phap = ['6', '7', '8', '9', '18']
    arr_aip_ngu_am = ['16']
    arr_aip_nghe = ['10', '11', '12', '13', '14', '15']

    arr_knowledge = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18']
    arr_comprehension = ['8', '9', '14']

    def do_add_lo_type(code):
        lo_type = -1
        code = str(code)
        for x in arr_aip_tu_vung:
            if x == code:
                lo_type = 1
        for x in arr_aip_ngu_am:
            if x == code:
                lo_type = 2
        for x in arr_aip_nghe:
            if x == code:
                lo_type = 3
        for x in arr_aip_ngu_phap:
            if x == code:
                lo_type = 4
        return lo_type

    add_lo_type = udf(do_add_lo_type, IntegerType())

    def do_add_score_aip(code, type, lo_type, correct_answer, student_answer):
        code = str(code)
        score = 0
        arr = []
        # diem ngu am
        if lo_type == 2 and correct_answer == student_answer:
            score = 2
        if lo_type == 2 and correct_answer != student_answer:
            score = -1
        # truong hop cac diem khac ko phair ngu am
        if lo_type != 2 and correct_answer == student_answer:
            score = 10
        if lo_type != 2 and correct_answer != student_answer:
            score = -5

        if type == 'knowledge':
            arr = arr_knowledge
        for x in arr:
            if x == code:
                return score

        return 0

    add_score_aip = udf(do_add_score_aip, IntegerType())

    def do_add_score_micro(code, type, lo_type, total_step, count_step):
        code = str(code)
        score = 0
        arr = []
        percent_success = 0.7
        if count_step / total_step >= percent_success:
            score = 10
        else:
            score = -5
        if type == 'knowledge':
            arr = arr_knowledge
        if type == 'comprehension':
            arr = arr_comprehension
        for x in arr:
            if x == code:
                return score
        return 0

    add_score_micro = udf(do_add_score_micro, IntegerType())

    def do_add_score_ait(total_step, max_step, received_point, length_answer):
        score = 0
        if total_step == max_step:
            if length_answer <= 3 and received_point >= 3:
                score = 30
            if length_answer <= 3 and received_point <= 2:
                score = 10
            if length_answer >= 4 and received_point <= 2:
                score = -15
        return score

    add_score_ait = udf(do_add_score_ait, IntegerType())

    ########## dyf_ai_study_step
    dyf_ai_study_step = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study_step"
    )
    dyf_ai_study_step = dyf_ai_study_step.select_fields(
        ['_key', 'user_id', 'lesson_id', 'tag', 'current_step', 'total_step', 'learning_object', 'learning_object_type',
         'correct_answer', 'student_answer', 'student_answer_details', 'max_point', 'received_point', 'created_at',
         'page_style', 'session_id'])


    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3://dts-odin/flag/flag_ai_study_step.parquet")
        max_key = df_flag.collect()[0]['flag']
        print('read from index: ', max_key)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        # dyf_ai_study_step = Filter.apply(frame=dyf_ai_study_step, f=lambda x: x['_key'] > max_key)
    except:
        print('read flag error ')

    if dyf_ai_study_step.count() > 0:
        try:
            ## Xu ly tag la: aip
            dyf_aip = Filter.apply(frame=dyf_ai_study_step,
                                   f=lambda x: x['tag'] == 'aip')

            df_aip = dyf_aip.toDF()

            def random_code():
                return random.randint(1, 16)

            add_code = udf(random_code, IntegerType())
            df_aip = df_aip.withColumn("code", add_code())
            df_aip.printSchema()
            df_aip = df_aip.withColumn("lo_type", add_lo_type(df_aip.code))
            df_aip = df_aip.withColumn("knowledge", add_score_aip(df_aip.code, f.lit('knowledge'), df_aip.lo_type,
                                                                  df_aip.correct_answer, df_aip.student_answer)) \
                .withColumn("comprehension",
                            add_score_aip(df_aip.code, f.lit('comprehension'), df_aip.lo_type, df_aip.correct_answer,
                                          df_aip.student_answer)) \
                .withColumn("application",
                            add_score_aip(df_aip.code, f.lit('application'), df_aip.lo_type, df_aip.correct_answer,
                                          df_aip.student_answer)) \
                .withColumn("analysis",
                            add_score_aip(df_aip.code, f.lit('analysis'), df_aip.lo_type, df_aip.correct_answer,
                                          df_aip.student_answer)) \
                .withColumn("synthesis",
                            add_score_aip(df_aip.code, f.lit('synthesis'), df_aip.lo_type, df_aip.correct_answer,
                                          df_aip.student_answer)) \
                .withColumn("evaluation",
                            add_score_aip(df_aip.code, f.lit('evaluation'), df_aip.lo_type, df_aip.correct_answer,
                                          df_aip.student_answer)) \
                .withColumn("date_id",
                            from_unixtime(unix_timestamp(df_aip.created_at, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))

            df_aip.printSchema()
            df_aip.show()
            dyf_aip = DynamicFrame.fromDF(df_aip, glueContext, "dyf_aip")

            applymapping = ApplyMapping.apply(frame=dyf_aip,
                                              mappings=[("created_at", "string", "created_at", "timestamp"),
                                                        ("user_id", 'string', 'student_id', 'long'),
                                                        ("correct_answer", "string", "learning_object", "string"),
                                                        ("date_id", "string", "date_id", "int"),
                                                        ("knowledge", "int", "knowledge", "int"),
                                                        ("comprehension", "int", "comprehension", "int"),
                                                        ("application", "int", "application", "int"),
                                                        ("analysis", "int", "analysis", "int"),
                                                        ("synthesis", "int", "synthesis", "int"),
                                                        ("evaluation", "int", "evaluation", "int")])
            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
            dropnullfields.printSchema()
            dropnullfields.show()
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student_starter_1",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/ai_study_step/",
                                                                       transformation_ctx="datasink5")
        except Exception as e:
            print("###################### Exception ##########################")
            print(e)
        try:
            ## Xu ly tag la: micro
            dyf_micro = Filter.apply(frame=dyf_ai_study_step, f=lambda x: x['tag'] == 'micro')

            df_micro = dyf_micro.toDF()

            df_micro_max_step = df_micro.groupby('user_id', 'lesson_id', 'session_id').agg(
                f.max('current_step').alias("max_step"))
            df_micro_max_step = df_micro_max_step.where("max_step >= 4")
            df_micro_max_step = df_micro_max_step.withColumnRenamed('user_id', 'user_id1') \
                .withColumnRenamed('lesson_id', 'lesson_id1') \
                .withColumnRenamed('session_id', 'session_id1')

            df_micro_received_point = df_micro.where("max_point = received_point")
            df_micro_received_point = df_micro_received_point.groupby('user_id', 'lesson_id', 'session_id').agg(
                f.count('received_point').alias("count_received_point"))
            df_micro_received_point = df_micro_received_point.withColumnRenamed('user_id', 'user_id2') \
                .withColumnRenamed('lesson_id', 'lesson_id2') \
                .withColumnRenamed('session_id', 'session_id2')

            df_micro = df_micro.join(df_micro_max_step, (df_micro['user_id'] == df_micro_max_step['user_id1'])
                                     & (df_micro['lesson_id'] == df_micro_max_step['lesson_id1'])
                                     & (df_micro['session_id'] == df_micro_max_step['session_id1']))

            df_micro = df_micro.join(df_micro_received_point,
                                     (df_micro['user_id'] == df_micro_received_point['user_id2'])
                                     & (df_micro['lesson_id'] == df_micro_received_point['lesson_id2'])
                                     & (df_micro['session_id'] == df_micro_received_point['session_id2']))

            def random_code1():
                return random.randint(17, 18)

            add_code1 = udf(random_code1, IntegerType())
            df_micro = df_micro.withColumn("code", add_code1())
            df_micro = df_micro.withColumn("lo_type", add_lo_type(df_micro.code))
            df_micro = df_micro.withColumn("knowledge",
                                           add_score_micro(df_micro.code, f.lit('knowledge'), df_micro.lo_type,
                                                           df_micro.total_step, df_micro.count_received_point)) \
                .withColumn("comprehension", add_score_micro(df_micro.code, f.lit('comprehension'), df_micro.lo_type,
                                                             df_micro.total_step, df_micro.count_received_point)) \
                .withColumn("application",
                            add_score_micro(df_micro.code, f.lit('application'), df_micro.lo_type, df_micro.total_step,
                                            df_micro.count_received_point)) \
                .withColumn("analysis",
                            add_score_micro(df_micro.code, f.lit('analysis'), df_micro.lo_type, df_micro.total_step,
                                            df_micro.count_received_point)) \
                .withColumn("synthesis",
                            add_score_micro(df_micro.code, f.lit('synthesis'), df_micro.lo_type, df_micro.total_step,
                                            df_micro.count_received_point)) \
                .withColumn("evaluation",
                            add_score_micro(df_micro.code, f.lit('evaluation'), df_micro.lo_type, df_micro.total_step,
                                            df_micro.count_received_point)) \
                .withColumn("date_id",
                            from_unixtime(unix_timestamp(df_micro.created_at, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
            df_micro.printSchema()
            df_micro.show()
            dyf_micro = DynamicFrame.fromDF(df_micro, glueContext, "dyf_micro")
            applymapping = ApplyMapping.apply(frame=dyf_micro,
                                              mappings=[("created_at", "string", "created_at", "timestamp"),
                                                        ("user_id", 'string', 'student_id', 'long'),
                                                        ("learning_object", "string", "learning_object", "string"),
                                                        ("date_id", "string", "date_id", "int"),
                                                        ("knowledge", "int", "knowledge", "int"),
                                                        ("comprehension", "int", "comprehension", "int"),
                                                        ("application", "int", "application", "int"),
                                                        ("analysis", "int", "analysis", "int"),
                                                        ("synthesis", "int", "synthesis", "int"),
                                                        ("evaluation", "int", "evaluation", "int")])
            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
            dropnullfields.printSchema()
            dropnullfields.show()
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student_starter_2",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/ai_study_step/",
                                                                       transformation_ctx="datasink5")
        except Exception as e:
            print("###################### Exception ##########################")
            print(e)

        except Exception as e:
            print("###################### Exception ##########################")
            print(e)

        try:
            ## Xu ly tag la: ait
            # dyf_ai_study_step.show(5)
            dyf_ait = Filter.apply(frame=dyf_ai_study_step,
                                   f=lambda x: x['tag'] == 'ait')
            # dyf_ait = Filter.apply(frame=dyf_ai_study_step,
            #                        f=lambda x: x['tag'] == 'ait'
            #                                    and x['student_answer_details'] is not None
            #                                    and x['student_answer_details'] != 'null'
            #                                    and x['correct_answer'] is not None)

            df_ait = dyf_ait.toDF()

            # udf_parse_json = udf(lambda str: parse_json(str), json_schema)

            # age_list = df_ait["student_answer_details"].tolist()
            # print ('list', age_list)

            df_ait = df_ait.withColumn('len_answer', udf_get_length(df_ait["student_answer_details"]))
            # df_ait.printSchema()
            # df_ait.show()

            df_ait_max_step = df_ait.groupby('user_id', 'lesson_id', 'total_step').agg(
                f.max('current_step').alias("max_step"))
            df_ait_max_step = df_ait_max_step.where('total_step = max_step')
            df_ait_max_step = df_ait_max_step.withColumnRenamed('user_id', 'user_id1').withColumnRenamed('lesson_id',
                                                                                                         'lesson_id1').withColumnRenamed(
                'total_step', 'total_step1')
            # df_ait_max_step.printSchema()
            # df_ait_max_step.show()

            df_ait_received_point = df_ait.where(
                "student_answer_details IS NOT NULL AND max_point = received_point AND page_style like '%ait_practice%'")
            df_ait_received_point = df_ait_received_point.groupby('user_id', 'lesson_id').agg(
                f.count('received_point').alias("count_received_point"))
            df_ait_received_point = df_ait_received_point.withColumnRenamed('user_id', 'user_id2').withColumnRenamed(
                'lesson_id',
                'lesson_id2')
            # df_ait_received_point.printSchema()
            # df_ait_received_point.show()

            # ait_pronunciation
            df_ait = df_ait.where("max_point = received_point AND page_style like '%ait_pronunciation%'")
            df_ait = df_ait.join(df_ait_received_point, (
                    df_ait['user_id'] == df_ait_received_point['user_id2']) & (
                                         df_ait['lesson_id'] ==
                                         df_ait_received_point[
                                             'lesson_id2']))
            df_ait = df_ait.join(df_ait_max_step, (
                    df_ait['user_id'] == df_ait_max_step['user_id1']) & (
                                         df_ait['lesson_id'] ==
                                         df_ait_max_step[
                                             'lesson_id1']))
            # print('SCHEMA:::')
            # df_ait.printSchema()
            # df_ait.show()
            df_ait = df_ait.withColumn("knowledge",
                                       add_score_ait(df_ait.total_step, df_ait.max_step, df_ait.count_received_point,
                                                     df_ait.len_answer)) \
                .withColumn("comprehension",
                            add_score_ait(df_ait.total_step, df_ait.max_step, df_ait.count_received_point,
                                          df_ait.len_answer)) \
                .withColumn("application",
                            add_score_ait(df_ait.total_step, df_ait.max_step, df_ait.count_received_point,
                                          df_ait.len_answer)) \
                .withColumn("analysis", f.lit(0)) \
                .withColumn("synthesis", f.lit(0)) \
                .withColumn("evaluation", f.lit(0)) \
                .withColumn("lo_type", f.lit(1)) \
                .withColumn("date_id",
                            from_unixtime(unix_timestamp(df_ait.created_at, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
            # df_ait.printSchema()
            # df_ait.show()

            dyf_ait = DynamicFrame.fromDF(df_ait, glueContext, "dyf_ait")

            applymapping = ApplyMapping.apply(frame=dyf_ait,
                                              mappings=[("created_at", "string", "created_at", "timestamp"),
                                                        ("user_id", 'string', 'student_id', 'long'),
                                                        ("correct_answer", "string", "learning_object", "string"),
                                                        ("date_id", "string", "date_id", "int"),
                                                        ("knowledge", "int", "knowledge", "int"),
                                                        ("comprehension", "int", "comprehension", "int"),
                                                        ("application", "int", "application", "int"),
                                                        ("analysis", "int", "analysis", "int"),
                                                        ("synthesis", "int", "synthesis", "int"),
                                                        ("evaluation", "int", "evaluation", "int")])
            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
            dropnullfields.printSchema()
            dropnullfields.show()
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student_starter",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/ai_study_step/",
                                                                       transformation_ctx="datasink5")

        except Exception as e:
            print("###################### Exception ##########################")
            print(e)

        df_temp = dyf_ai_study_step.toDF()
        flag = df_temp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_ai_study_step.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
