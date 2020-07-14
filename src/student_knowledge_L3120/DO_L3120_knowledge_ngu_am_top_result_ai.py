#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, StringType, ArrayType, IntegerType
import unicodedata


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # thoi gian tu 01/10/2019
    timestamp = 1569888000

    ## Phonetic
    dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )
    dyf_phonemic = Filter.apply(frame=dyf_learning_object, f=lambda x: x["learning_object_type"] == 'phonetic')
    dyf_phonemic = dyf_phonemic.select_fields(['learning_object_id', 'learning_object_name'])
    # df_phonemic = dyf_phonemic.toDF()
    # df_phonemic = df_phonemic.withColumn('lo_name', convertedudf(df_phonemic.learning_object_name))
    # df_phonemic.show()
    # Lay ra ngu am
    df1 = dyf_phonemic.toDF()
    df1 = df1.select('learning_object_id', 'learning_object_name')
    # myArr = np.array(df1.select('phonemic').collect())
    arrPhonetic = [row.learning_object_name for row in df1.collect()]
    arrPhoneticId = [[row.learning_object_name, row.learning_object_id] for row in df1.collect()]
    # print(unicode(arrPhonetic[2]))
    # print('ARR:', arrPhonetic)
    # print('ARR:', arrPhonetic[2].encode('utf-8', 'replace'))
    # print('ARR1 :', (u'i:' in arrPhonetic))

    # ETL TBHV
    # Custom function


    def doAddScoreAll(plus, minus):
        if plus is None and minus is not None:
            return minus
        if minus is None and plus is not None:
            return plus
        if minus is not None and plus is not None:
            return plus + minus
        return 0

    addScoreAll = udf(doAddScoreAll, IntegerType())

    def do_get_phone_tic_id(phonetic):
        phonetic = phonetic.encode('utf-8', 'replace').strip()
        for x in arrPhoneticId:
            p = x[0].encode('utf-8', 'replace').strip()
            if p == phonetic:
                return x[1]

    get_phone_tic_id = udf(do_get_phone_tic_id, IntegerType())

    def do_check_null(val1, val2):
        if val1 is None and val2 is not None:
            return val2
        if val2 is None and val1 is not None:
            return val1
        if val1 is not None and val2 is not None:
            return val1
        return 0

    check_data_null = udf(do_check_null, StringType())

    def doSplitWord(word):
        rs = []
        if word is not None:
            i = 0
            size = len(word)
            while i < size:
                s = word[i:i + 2]
                i += 2
                if s in arrPhonetic:
                    rs.append(s)
                if s not in arrPhonetic:
                    i -= 2
                    s = word[i:i + 1]
                    i += 1
                    if s in arrPhonetic:
                        rs.append(s)

        return rs

    splitWord = udf(lambda x: doSplitWord(x))

    state_right = 'state_right'
    state_wrong = 'state_wrong'

    # mac dinh duoc cong knowledge
    # P1_D1; P1_D2; P1_D3; P2_D1; P2_D2; P2_D3; P3_D1; P3_D2; P4_D1; P4_D2
    # knowledge = []
    # cong diem comprehension:
    # Can list cac name duoc cong diem comprehension:
    # P1_D1; P1_D2; P1_D3; P2_D1; P2_D2; P2_D3; P3_D2; P4_D1; P4_D2
    comprehension = ['P1_D1', 'P1_D2', 'P1_D3', 'P2_D1', 'P2_D2', 'P2_D3', 'P3_D1', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem application:
    # Can list cac name duoc cong diem application:
    # P1_D3; P2_D1; P2_D2; P2_D3; P3_D2; P4_D1; P4_D2
    application = ['P1_D1', 'P1_D2', 'P1_D3', 'P2_D1', 'P2_D2', 'P2_D3', 'P3_D1', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem analysis:
    # Can list cac name duoc cong diem analysis
    # P2_D3; P3_D2; P4_D1; P4_D2
    analysis = ['P2_D3', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem synthesis:
    # Can list cac name duoc cong diem synthesis
    # P4_D1; P4_D2
    synthesis = []
    # cong diem evaluation:
    # Can list cac name duoc cong diem evaluation
    evaluation = []

    def doAddScore(name, state, type):
        arr = ['']
        score = 0
        if type == 'comprehension':
            arr = comprehension

        if type == 'application':
            arr = application

        if type == 'analysis':
            arr = analysis

        if type == 'synthesis':
            arr = synthesis

        name = name.lower()
        if state == state_right:
            score = 2
        if state == state_wrong:
            score = -1

        if name is not None:
            for x in arr:
                if x.lower() in name:
                    return score
        return 0

    addScore = udf(doAddScore, IntegerType())

    # chuoi ky tu can replace
    special_str = '["] ;'

    ########## top_quiz_attempts
    dyf_top_quiz_attempts = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_quiz_attempts"
    )
    dyf_top_quiz_attempts = dyf_top_quiz_attempts.select_fields(['_key', 'id', 'timestart', 'quiz'])

    dyf_top_quiz_attempts = dyf_top_quiz_attempts.resolveChoice(specs=[('_key', 'cast:long')])

    # print dyf_top_quiz_attempts.count()
    # dyf_top_quiz_attempts.show(2)

    dyf_top_quiz_attempts = Filter.apply(frame=dyf_top_quiz_attempts,
                                         f=lambda x: x["timestart"] >= timestamp)

    # print dyf_top_quiz_attempts.count()
    # dyf_top_quiz_attempts.show()

    # xu ly truong hop start_read is null
    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_knowledge_ngu_am_top_ai")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read from index: ', start_read)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_top_quiz_attempts = Filter.apply(frame=dyf_top_quiz_attempts, f=lambda x: x['_key'] > start_read)
    # except:
    #     print('read flag file error ')

    # print('the number of new contacts: ', dyf_top_quiz_attempts.count())

    if dyf_top_quiz_attempts.count() > 0:
        ########## dyf_top_user
        dyf_top_user = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="do_top_user"
        )
        dyf_top_user = dyf_top_user.select_fields(
            ['id', 'student_id']).rename_field('id', 'top_user_id')
        ######### top_question
        dyf_top_question = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question"
        )
        dyf_top_question = dyf_top_question.select_fields(
            ['id', 'name'])
        # dyf_top_result_ai = dyf_top_result_ai.resolveChoice(specs=[('_key', 'cast:long')])

        ######### top_result_ai
        dyf_top_result_ai = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_result_ai"
        )
        dyf_top_result_ai = dyf_top_result_ai.select_fields(
            ['question_id', 'attempt_id', 'user_id', 'ratio', 'right_word', 'wrong_word'])

        # JOIN va FILTER cac bang theo dieu kien
        dyf_join01 = Join.apply(dyf_top_result_ai, dyf_top_question, 'question_id', 'id')
        dyf_join02 = Join.apply(dyf_join01, dyf_top_quiz_attempts, 'attempt_id', 'id')
        dyf_join02 = Filter.apply(frame=dyf_join02, f=lambda x: x["quiz"] not in [7, 9, 918])
        dyf_join02 = Join.apply(dyf_join02, dyf_top_user, 'user_id', 'top_user_id')

        # dyf_join02 = Filter.apply(frame=dyf_join02, f=lambda x: x["student_id"] == 259442)

        # dyf_join02.show()
        df_study = dyf_join02.toDF()
        df_study.cache()
        if (df_study.count() > 0):
            try:

                # print("COUNT 1:", df_study.count())
                # Loc cac ky tu dac biet [ ] "
                # Hien data co dang nhu sau: ["house","her","to","how","get","long"] hoac "environmental", ...
                # df_study = df_study.select(
                #     'quiz', 'name', 'user_id', 'timestart', 'right_word', 'wrong_word', f.translate(df_study.right_word,
                #                                                                                     special_str, ''), f.translate(df_study.wrong_word,
                #                                        special_str, ''))
                df_study = df_study.select(
                    'quiz', 'name', 'student_id', 'timestart', 'right_word', 'wrong_word')
                df_study = df_study.withColumn("right_word_new", f.translate(df_study.right_word, special_str, '')) \
                    .withColumn("wrong_word_new", f.translate(df_study.wrong_word, special_str, ''))

                # Tach cau thanh array tu:
                # house, her => [house, her]
                # PHan tich tu dung
                df_study_right = df_study.withColumn("right_word_list", f.split(
                    df_study.right_word_new, ','))

                # Split column array => nhieu row
                # row: [house, her] =>
                # row1: house
                # row2: her
                df_study_right = df_study_right.withColumn("right", f.explode(df_study_right.right_word_list))
                # convert to lowercase
                df_study_right = df_study_right.withColumn("right", f.lower(f.col("right")))
                df_study_right = df_study_right.select('quiz', 'name', 'student_id', 'timestart', 'right')
                # print("COUNT 2:", df_study_right.count())
                # df_study_right.printSchema()
                # df_study_right.show()
                dyf_study_right = DynamicFrame.fromDF(df_study_right, glueContext, "dyf_study_right")
                ## Learning Object
                # dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
                #     database="nvn_knowledge",
                #     table_name="nvn_knowledge_learning_object"
                # )
                dyf_learning_object = Filter.apply(frame=dyf_learning_object,
                                            f=lambda x: x["learning_object_type"] == 'vocabulary')
                dyf_learning_object = dyf_learning_object.select_fields(
                    ['learning_object_id', 'learning_object_name', 'transcription'])
                df_learning_object = dyf_learning_object.toDF()
                # convert to lowercase
                df_learning_object = df_learning_object.withColumn("learning_object_name", f.lower(f.col("learning_object_name")))
                # replace cac ky tu
                df_learning_object = df_learning_object.withColumn("phone_tic_new",
                                                                   f.translate(df_learning_object.transcription, '\',', ''))

                df_learning_object = df_learning_object.withColumn("phone_tic_tmp",
                                                                   splitWord(df_learning_object.phone_tic_new))
                df_learning_object = df_learning_object.withColumn("phone_tic_tmp_01",
                                                                   f.translate(df_learning_object.phone_tic_tmp, '[]',
                                                                               ''))
                df_learning_object = df_learning_object.withColumn("phone_tic_arr",
                                                                   f.split(df_learning_object.phone_tic_tmp_01, ','))

                df_learning_object = df_learning_object.withColumn("split_phonetic",
                                                                   f.explode(df_learning_object.phone_tic_arr))

                df_learning_object = df_learning_object.select('learning_object_id', 'learning_object_name',
                                                               'split_phonetic')

                dyf_learning_object = DynamicFrame.fromDF(df_learning_object, glueContext, "dyf_learning_object")

                dyf_knowledge_right = Join.apply(dyf_study_right, dyf_learning_object, 'right', 'learning_object_name')


                # print("COUNT 3:", dyf_knowledge_right.count())
                # dyf_knowledge_right.printSchema()
                # 1
                df_knowledge_right = dyf_knowledge_right.toDF()
                # df_knowledge_right = df_knowledge_right.withColumn("right_phonetic",
                #                                                    f.explode(df_knowledge_right.phone_tic_arr))
                df_knowledge_right = df_knowledge_right.select('timestart', 'name', 'student_id', 'split_phonetic')
                df_knowledge_right = df_knowledge_right.withColumn("learning_object_id", get_phone_tic_id(df_knowledge_right.split_phonetic))
                # dyf_phonemic_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_phonemic_right")



                # dyf_phonemic_right = Join.apply(dyf_study_right, dyf_phonemic, 'split_phonetic', 'learning_object_name')
                #
                # dropnullfields = DropNullFields.apply(frame=dyf_phonemic_right, transformation_ctx="dropnullfields")
                # datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "mapping_lo_student_history_v06",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/top_question_attempt/",
                #                                                            transformation_ctx="datasink6")

                # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                # df_knowledge_right = dyf_phonemic_right.toDF()
                # print("COUNT 4:")
                # df_knowledge_right.printSchema()
                df_knowledge_right.cache()

                df_knowledge_right = df_knowledge_right.withColumn("knowledge", f.lit(2)) \
                    .withColumn("comprehension",
                                addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('comprehension'))) \
                    .withColumn("application",
                                addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('application'))) \
                    .withColumn("analysis", addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('analysis'))) \
                    .withColumn("synthesis",
                                addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('synthesis'))) \
                    .withColumn("evaluation", f.lit(0)) \
                    .withColumn("date_id", from_unixtime(df_knowledge_right['timestart'], 'yyyyMMdd')) \
                    .withColumn("lo_type", f.lit(2))

                dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                # dropnullfields = DropNullFields.apply(frame=dyf_knowledge_right, transformation_ctx="dropnullfields")
                # datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "mapping_lo_student_history_v02",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/top_question_attempt/",
                #                                                            transformation_ctx="datasink6")

                # print("COUNT 444444444444444:", df_knowledge_right.count())
                # df_knowledge_right.printSchema()
                # df_knowledge_right.show()
                #
                # dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                # # chon cac truong va kieu du lieu day vao db
                # applymapping = ApplyMapping.apply(frame=dyf_knowledge_right,
                #                                   mappings=[("timestart", "long", "timestart", "long"),
                #                                             ("student_id", 'int', 'student_id', 'long'),
                #                                             ("name", 'string', 'name', 'string'),
                #                                             ("learning_object_id", "long", "learning_object_id", "long"),
                #                                             ("date_id", "string", "date_id", "long"),
                #                                             ("knowledge", "int", "knowledge", "long"),
                #                                             ("comprehension", "int", "comprehension", "long"),
                #                                             ("application", "int", "application", "long"),
                #                                             ("analysis", "int", "analysis", "long"),
                #                                             ("synthesis", "int", "synthesis", "long"),
                #                                             ("evaluation", "int", "evaluation", "long"),
                #                                             ("lo_type", "int", "lo_type", "int")])
                # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                #                                     transformation_ctx="resolvechoice")
                # dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
                #
                # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "t_temp_right_learning_object_phonetic",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/",
                #                                                            transformation_ctx="datasink5")
                # END Cong diem cac tu dung

                ##################################################
                # Tru diem cac tu sai: Xu lu tuong tu tu dung.
                # rule tru diem la -1 diem neu sai
                df_study_wrong = df_study.withColumn("wrong_word_list", f.split(
                    df_study.wrong_word_new, ','))

                # Split column array => nhieu row
                # row: [house, her] =>
                # row1: house
                # row2: her
                df_study_wrong = df_study_wrong.withColumn("wrong", f.explode(df_study_wrong.wrong_word_list))
                #convert to lowercase
                df_study_wrong = df_study_wrong.withColumn("wrong",  f.lower(f.col("wrong")))
                df_study_wrong = df_study_wrong.select('quiz', 'name', 'student_id', 'timestart', 'wrong')
                # print("COUNT 2222:", df_study_wrong.count())
                # df_study_wrong.printSchema()
                # df_study_wrong.show()
                dyf_study_wrong = DynamicFrame.fromDF(df_study_wrong, glueContext, "dyf_study_wrong")
                ## Learning Object
                dyf_knowledge_wrong = Join.apply(dyf_study_wrong, dyf_learning_object, 'wrong', 'learning_object_name')

                df_knowledge_wrong = dyf_knowledge_wrong.toDF()
                # df_knowledge_wrong = df_knowledge_wrong.withColumn("wrong_phonetic",
                #                                                    f.explode(df_knowledge_wrong.phone_tic_arr))
                df_knowledge_wrong = df_knowledge_wrong.select('timestart', 'name', 'student_id', 'split_phonetic')

                df_knowledge_wrong = df_knowledge_wrong.withColumn("learning_object_id",
                                                                   get_phone_tic_id(df_knowledge_wrong.split_phonetic))

                # dyf_study_wrong = DynamicFrame.fromDF(df_knowledge_wrong, glueContext, "dyf_study_wrong")

                # dyf_phonemic_wrong = Join.apply(dyf_study_wrong, dyf_phonemic, 'split_phonetic', 'learning_object_name')

                # print("COUNT 3:", dyf_knowledge_wrong.count())
                # dyf_knowledge_wrong.printSchema()
                # print("COUNT 4:", dyf_knowledge_wrong.count())
                # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                # df_knowledge_wrong = dyf_phonemic_wrong.toDF()
                df_knowledge_wrong.cache()

                df_knowledge_wrong = df_knowledge_wrong.withColumn("knowledge", f.lit(-1)) \
                    .withColumn("comprehension",
                                addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('comprehension'))) \
                    .withColumn("application",
                                addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('application'))) \
                    .withColumn("analysis", addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('analysis'))) \
                    .withColumn("synthesis",
                                addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('synthesis'))) \
                    .withColumn("evaluation", f.lit(0)) \
                    .withColumn("date_id", from_unixtime(df_knowledge_wrong['timestart'], 'yyyyMMdd'))

                # df_knowledge_wrong.printSchema()
                # df_knowledge_wrong.show()
                #
                # dyf_knowledge_wrong = DynamicFrame.fromDF(df_knowledge_wrong, glueContext, "dyf_knowledge_wrong")
                #
                # # chon cac truong va kieu du lieu day vao db
                # applymapping1 = ApplyMapping.apply(frame=dyf_knowledge_wrong,
                #                                    mappings=[("timestart", "long", "timestart", "long"),
                #                                              ("name", 'string', 'name', 'string'),
                #                                              ("student_id", 'int', 'student_id', 'long'),
                #                                              ("id", "int", "learning_object_id", 'long'),
                #                                              ("date_id", "string", "date_id", "long"),
                #                                              ("knowledge", "int", "knowledge", "long"),
                #                                              ("comprehension", "int", "comprehension", "long"),
                #                                              ("application", "int", "application", "long"),
                #                                              ("analysis", "int", "analysis", "long"),
                #                                              ("synthesis", "int", "synthesis", "long"),
                #                                              ("evaluation", "int", "evaluation", "long")])
                # resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                #                                      transformation_ctx="resolvechoice1")
                # dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields1")
                #
                # datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "t_temp_right_learning_object_phonetic",
                #                                                                "database": "dts_odin",
                #                                                                "postactions": """ call proc_knowledge_ngu_am_top_result_ai () """
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/",
                #                                                            transformation_ctx="datasink5")



                ### Luu bang mapping_lo_student_history
                df_knowledge_right = df_knowledge_right.groupby('student_id', 'date_id',
                                                                'learning_object_id').agg(
                    f.count('knowledge').alias("count_plus"),
                    f.sum('knowledge').alias("knowledge_plus"),
                    f.sum('comprehension').alias("comprehension_plus"),
                    f.sum('application').alias("application_plus"),
                    f.sum('analysis').alias("analysis_plus"),
                    f.sum('synthesis').alias("synthesis_plus"),
                    f.sum('evaluation').alias("evaluation_plus"))
                df_knowledge_right = df_knowledge_right.where('student_id is not null')

                df_knowledge_wrong = df_knowledge_wrong.groupby('student_id', 'date_id',
                                                                'learning_object_id').agg(
                    f.count('knowledge').alias("count_minus"),
                    f.sum('knowledge').alias("knowledge_minus"),
                    f.sum('comprehension').alias("comprehension_minus"),
                    f.sum('application').alias("application_minus"),
                    f.sum('analysis').alias("analysis_minus"),
                    f.sum('synthesis').alias("synthesis_minus"),
                    f.sum('evaluation').alias("evaluation_minus")) \
                    .withColumnRenamed('student_id', 'student_id_wrong') \
                    .withColumnRenamed('date_id', 'date_id_wrong') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_wrong')
                df_knowledge_wrong = df_knowledge_wrong.where('student_id_wrong is not null')
                df_knowledge = df_knowledge_right.join(df_knowledge_wrong, (
                        df_knowledge_right['student_id'] == df_knowledge_wrong['student_id_wrong']) & (
                                                               df_knowledge_right['date_id'] ==
                                                               df_knowledge_wrong['date_id_wrong']) & (
                                                               df_knowledge_right['learning_object_id'] ==
                                                               df_knowledge_wrong['learning_object_id_wrong']), 'outer')
                df_knowledge = df_knowledge.withColumn("user_id",
                                check_data_null(df_knowledge.student_id, df_knowledge.student_id_wrong)) \
                    .withColumn("learning_object_id",
                                check_data_null(df_knowledge.learning_object_id, df_knowledge.learning_object_id_wrong)) \
                    .withColumn("created_date_id",
                                check_data_null(df_knowledge.date_id, df_knowledge.date_id_wrong)) \
                    .withColumn("source_system", f.lit('top_result_ai_phonetic')) \
                    .withColumn("lu_id", f.lit(0))

                dyf_knowledge = DynamicFrame.fromDF(df_knowledge, glueContext, "df_knowledge")

                # dyf_knowledge.printSchema()
                dyf_knowledge.printSchema()
                dyf_knowledge.show()

                # dyf_knowledge = DynamicFrame.fromDF(dyf_knowledge, glueContext, "dyf_knowledge")
                # chon cac truong va kieu du lieu day vao db
                applymapping = ApplyMapping.apply(frame=dyf_knowledge,
                                                  mappings=[("user_id", 'string', 'student_id', 'long'),
                                                             ("learning_object_id", "string", "learning_object_id", "long"),
                                                             # ("knowledge", "int", "knowledge", "long"),
                                                             # ("comprehension", "int", "comprehension", "long"),
                                                             # ("application", "int", "application", "long"),
                                                             # ("analysis", "int", "analysis", "long"),
                                                             # ("synthesis", "int", "synthesis", "long"),
                                                             # ("evaluation", "int", "evaluation", "long"),
                                                             ("knowledge_plus", "long", "knowledge_plus", "long"),
                                                             ("comprehension_plus", "long", "comprehension_plus", "long"),
                                                             ("application_plus", "long", "application_plus", "long"),
                                                             ("analysis_plus", "long", "analysis_plus", "long"),
                                                             ("synthesis_plus", "long", "synthesis_plus", "long"),
                                                             ("evaluation_plus", "long", "evaluation_plus", "long"),
                                                             ("knowledge_minus", "long", "knowledge_minus", "long"),
                                                             ("comprehension_minus", "long", "comprehension_minus", "long"),
                                                             ("application_minus", "long", "application_minus", "long"),
                                                             ("analysis_minus", "long", "analysis_minus", "long"),
                                                             ("synthesis_minus", "long", "synthesis_minus", "long"),
                                                             ("evaluation_minus", "long", "evaluation_minus", "long"),
                                                             ("count_plus", "long", "plus_number", "long"),
                                                             ("count_minus", "long", "minus_number", "long"),
                                                             # ("lo_type", "string", "lo_type", "long"),
                                                             ("source_system", "string", "source_system", "string"),
                                                             ("created_date_id", "string", "created_date_id", "long"),
                                                             ("lu_id", "int", "lu_type", "long")
                                                             # ("student_level", "string", "student_level", "string"),
                                                             # ("advisor_id", "string", "advisor_id", "long"),
                                                             # ("package_code", "string", "package_code", "string")
                                                             ])
                resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                    transformation_ctx="resolvechoice")
                dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

                print('START WRITE TO S3-------------------------')

                datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields, connection_type="s3",
                                                                         connection_options={
                                                                             "path": "s3://dtsodin/nvn_knowledge/mapping_lo_student_history_v2/",
                                                                             "partitionKeys": ["created_date_id", "source_system"]},
                                                                         format="parquet",
                                                                         transformation_ctx="datasink6")
                print('END WRITE TO S3-------------------------')

                # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "mapping_lo_student_history",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/top_question_attempt/",
                #                                                            transformation_ctx="datasink5")


                ### END Luu bang mapping_lo_student_history
                # END Tru diem cac tu sai
                # lay max _key tren datasource
                datasource = dyf_top_quiz_attempts.toDF()
                flag = datasource.agg({"_key": "max"}).collect()[0][0]
                flag_data = [flag]
                df = spark.createDataFrame(flag_data, "long").toDF('flag')

                # ghi de flag moi vao s3
                df.write.parquet("s3a://dtsodin/flag/flag_knowledge_ngu_am_top_ai", mode="overwrite")
                # xoa cache
                df_study.unpersist()
                df_knowledge_right.unpersist()
                # df_knowledge_right.unpersist()
            except Exception as e:
                print("###################### Exception ##########################")
                print(e)

        # # chon cac field
        # dyf_nvn_knowledge_vocabulary = dyf_nvn_knowledge_vocabulary.select_fields(
        #     ['_key', 'question_id', 'user_id', 'right_word', 'wrong_word', 'name', 'timestart'])
        # # convert kieu du lieu
        # dyf_nvn_knowledge_vocabulary = dyf_nvn_knowledge_vocabulary.resolveChoice(specs=[('_key', 'cast:long')])
        # print ("nvn_knowledge_vocabulary: ", dyf_nvn_knowledge_vocabulary.count())
        # df_nvn_knowledge_vocabulary = dyf_nvn_knowledge_vocabulary.toDF()
        #
        # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn('right_word_new',
        #                                                                      f.regexp_replace(
        #                                                                          df_nvn_knowledge_vocabulary.right_word,
        #                                                                          '["] ', ''))
        # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.select('right_word',
        #                                                                  f.translate(df_nvn_knowledge_vocabulary.right_word,
        #                                                                              '["] ', '').alias(
        #                                                                      'right_word_new'))
        # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col1", f.split(
        #     df_nvn_knowledge_vocabulary.right_word_new, ',').alias('col1'))
        # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col3",
        #                                                                      f.explode(df_nvn_knowledge_vocabulary.col1))
        #
        #
        #
        # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col4", splitWord(
        #     df_nvn_knowledge_vocabulary.col3))
        # # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col5",
        # #                                                                      f.explode(df_nvn_knowledge_vocabulary.col4))
        # print ("nvn_knowledge_vocabulary after: ", df_nvn_knowledge_vocabulary.count())
        # df_nvn_knowledge_vocabulary.printSchema()
        # df_nvn_knowledge_vocabulary.show()


if __name__ == "__main__":
    main()
