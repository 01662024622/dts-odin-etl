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
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # thoi gian tu 01/10/2019
    timestamp = 1569888000

    # ETL TBHV
    # Custom function
    def doSplitWord(word):
        size = len(word)
        rs = [word[i:i + 2] for i in range(0, size, 1)]
        rs1 = [word[i:i + 1] for i in range(0, size, 1)]
        rs.extend(rs1)
        return rs

    state_right = 'state_right'
    state_wrong = 'state_wrong'

    # mac dinh duoc cong knowledge
    # P1_D1; P1_D2; P1_D3; P2_D1; P2_D2; P2_D3; P3_D1; P3_D2; P4_D1; P4_D2
    knowledge = ''
    # cong diem comprehension:
    # Can list cac name duoc cong diem comprehension:
    # P1_D1; P1_D2; P1_D3; P2_D1; P2_D2; P2_D3; P3_D2; P4_D1; P4_D2
    comprehension = ['P1_D1', 'P1_D2', 'P1_D3', 'P2_D1', 'P2_D2', 'P2_D3', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem application:
    # Can list cac name duoc cong diem application:
    # P1_D3; P2_D1; P2_D2; P2_D3; P3_D2; P4_D1; P4_D2
    application = ['P1_D3', 'P2_D1', 'P2_D2', 'P2_D3', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem analysis:
    # Can list cac name duoc cong diem analysis
    # P2_D3; P3_D2; P4_D1; P4_D2
    analysis = ['P2_D3', 'P3_D2', 'P4_D1', 'P4_D2']
    # cong diem synthesis:
    # Can list cac name duoc cong diem synthesis
    # P4_D1; P4_D2
    synthesis = ['P4_D1', 'P4_D2']
    # cong diem evaluation:
    # Can list cac name duoc cong diem evaluation
    evaluation = ''

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
            score = 10
        if state == state_wrong:
            score = -5

        if name is not None:
            for x in arr:
                if x.lower() in name:
                    return score
        return 0

    addScore = udf(doAddScore, IntegerType())

    def doAddScoreAll(plus, minus):
        if plus is None and minus is not None:
            return minus
        if minus is None and plus is not None:
            return plus
        if minus is not None and plus is not None:
            return plus + minus
        return 0
    addScoreAll = udf(doAddScoreAll, IntegerType())

    def do_check_null(val1, val2):
        if val1 is None and val2 is not None:
            return val2
        if val2 is None and val1 is not None:
            return val1
        if val1 is not None and val2 is not None:
            return val1
        return 0
    check_data_null = udf(do_check_null, StringType())

    # chuoi ky tu can replace
    special_str = '["] ;'

    splitWord = udf(lambda x: doSplitWord(x))

    ########## top_quiz_attempts
    dyf_top_quiz_attempts = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_quiz_attempts"
    )
    dyf_top_quiz_attempts = dyf_top_quiz_attempts.select_fields(
        ['_key', 'id', 'timestart', 'quiz'])

    dyf_top_quiz_attempts = dyf_top_quiz_attempts.resolveChoice(specs=[('_key', 'cast:long')])

    print dyf_top_quiz_attempts.count()
    dyf_top_quiz_attempts.show(2)

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_tu_vung_result_ai.parquet")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read from index: ', start_read)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_top_quiz_attempts = Filter.apply(frame=dyf_top_quiz_attempts, f=lambda x: x['_key'] > start_read)
    # except:
    #     print('read flag file error ')

    dyf_top_quiz_attempts = Filter.apply(frame=dyf_top_quiz_attempts,
                     f=lambda x: x["timestart"] >= timestamp)

    print dyf_top_quiz_attempts.count()
    dyf_top_quiz_attempts.show()

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
            ['id', 'name']).rename_field('id', 'quest_id')
        # dyf_top_result_ai = dyf_top_result_ai.resolveChoice(specs=[('_key', 'cast:long')])

        ######### top_result_ai
        dyf_top_result_ai = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_result_ai"
        )
        dyf_top_result_ai = dyf_top_result_ai.select_fields(
            ['question_id', 'attempt_id', 'user_id', 'ratio', 'right_word', 'wrong_word'])

        # JOIN va FILTER cac bang theo dieu kien
        dyf_join01 = Join.apply(dyf_top_result_ai, dyf_top_question, 'question_id', 'quest_id')
        dyf_join02 = Join.apply(dyf_join01, dyf_top_quiz_attempts, 'attempt_id', 'id')

        dyf_join02 = Filter.apply(frame=dyf_join02, f=lambda x: x["quiz"] not in [7, 9, 918])
        dyf_join02 = Join.apply(dyf_join02, dyf_top_user, 'user_id', 'top_user_id')

        # dyf_join02.show()
        df_study = dyf_join02.toDF()
        df_study.cache()
        if (df_study.count() > 0):
            try:
                # print("COUNT 1:", df_study.count())
                # Loc cac ky tu dac biet [ ] ",
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
                df_study_right = df_study_right.select('quiz', 'name', 'student_id', 'timestart', 'right')
                df_study_right = df_study_right.withColumn("right", f.lower(f.col("right")))
                # print("COUNT 2:", df_study_right.count())
                # df_study_right.printSchema()
                # df_study_right.show()
                dyf_study_right = DynamicFrame.fromDF(df_study_right, glueContext, "dyf_study_right")
                ## Learning Object
                dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
                    database="nvn_knowledge",
                    table_name="learning_object"
                )
                dyf_learning_object = dyf_learning_object.select_fields(
                    ['learning_object_id', 'learning_object_name'])

                df_learning_object = dyf_learning_object.toDF()
                # convert to lowercase
                df_learning_object = df_learning_object.withColumn("learning_object_name",
                                                                   f.lower(f.col("learning_object_name")))
                dyf_learning_object = DynamicFrame.fromDF(df_learning_object, glueContext, "dyf_learning_object")

                dyf_knowledge_right = Join.apply(dyf_study_right, dyf_learning_object, 'right', 'learning_object_name')

                # print("COUNT 3:", dyf_knowledge_right.count())
                # dyf_knowledge_right.printSchema()
                # print("COUNT 4:", dyf_knowledge_wrong.count())
                # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                df_knowledge_right = dyf_knowledge_right.toDF()
                df_knowledge_right.cache()

                df_knowledge_right = df_knowledge_right.withColumn("knowledge", f.lit(10)) \
                        .withColumn("comprehension", addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('comprehension'))) \
                        .withColumn("application", addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('application'))) \
                        .withColumn("analysis", addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('analysis'))) \
                        .withColumn("synthesis", addScore(df_knowledge_right.name, f.lit('state_right'), f.lit('synthesis'))) \
                        .withColumn("evaluation", f.lit(0)) \
                        .withColumn("date_id", from_unixtime(df_knowledge_right['timestart'], 'yyyyMMdd'))


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
                # df_knowledge_right.printSchema()
                # df_knowledge_right.show()

                # dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                #
                # applymapping = ApplyMapping.apply(frame=dyf_knowledge_right,
                #                                   mappings=[("timestart", "long", "timestart", "long"),
                #                                             ("student_id", 'int', 'student_id', 'long'),
                #                                             ("learning_object_id", "int", "learning_object_id", "int"),
                #                                             ("date_id", "string", "date_id", "int"),
                #                                             ("knowledge", "int", "knowledge", "int"),
                #                                             ("comprehension", "int", "comprehension", "int"),
                #                                             ("application", "int", "application", "int"),
                #                                             ("analysis", "int", "analysis", "int"),
                #                                             ("synthesis", "int", "synthesis", "int"),
                #                                             ("evaluation", "int", "evaluation", "int")])
                # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                #                                     transformation_ctx="resolvechoice2")
                # dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
                #
                # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "temp_right_wrong_learning_object",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/",
                #                                                            transformation_ctx="datasink5")

                # END Cong diem cac tu dung

                #################################################
                # Tru diem cac tu sai: Xu lu tuong tu tu dung.
                # rule tru diem la -5 diem neu sai

                df_study_wrong = df_study.withColumn("wrong_word_list", f.split(
                    df_study.wrong_word_new, ','))

                # Split column array => nhieu row
                # row: [house, her] =>
                # row1: house
                # row2: her
                df_study_wrong = df_study_wrong.withColumn("wrong", f.explode(df_study_wrong.wrong_word_list))
                #convert to lowercase
                df_study_wrong = df_study_wrong.withColumn("wrong", f.lower(f.col("wrong")))

                df_study_wrong = df_study_wrong.select('quiz', 'name', 'student_id', 'timestart', 'wrong')
                # print("COUNT 2:", df_study_wrong.count())
                # df_study_wrong.printSchema()
                # df_study_wrong.show()

                dyf_study_wrong = DynamicFrame.fromDF(df_study_wrong, glueContext, "dyf_study_wrong")
                ## Learning Object
                dyf_knowledge_wrong = Join.apply(dyf_study_wrong, dyf_learning_object, 'wrong', 'learning_object_name')

                # print("COUNT 3:", dyf_knowledge_wrong.count())
                # dyf_knowledge_wrong.printSchema()
                # print("COUNT 4:", dyf_knowledge_wrong.count())
                # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                df_knowledge_wrong = dyf_knowledge_wrong.toDF()
                df_knowledge_wrong.cache()

                df_knowledge_wrong = df_knowledge_wrong.withColumn("knowledge", f.lit(-5)) \
                    .withColumn("comprehension",
                                addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('comprehension'))) \
                    .withColumn("application",
                                addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('application'))) \
                    .withColumn("analysis", addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('analysis'))) \
                    .withColumn("synthesis", addScore(df_knowledge_wrong.name, f.lit('state_wrong'), f.lit('synthesis'))) \
                    .withColumn("evaluation", f.lit(0)) \
                    .withColumn("date_id", from_unixtime(df_knowledge_wrong['timestart'], 'yyyyMMdd'))

                df_knowledge_wrong = df_knowledge_wrong.groupby('student_id', 'date_id',
                                                                'learning_object_id').agg(
                    f.count('knowledge').alias("count_minus"),
                    f.sum('knowledge').alias("knowledge_minus"),
                    f.sum('comprehension').alias("comprehension_minus"),
                    f.sum('application').alias("application_minus"),
                    f.sum('analysis').alias("analysis_minus"),
                    f.sum('synthesis').alias("synthesis_minus"),
                    f.sum('evaluation').alias("evaluation_minus"))\
                    .withColumnRenamed('student_id', 'student_id_wrong') \
                    .withColumnRenamed('date_id', 'date_id_wrong') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_wrong')

                df_knowledge_wrong = df_knowledge_wrong.where('student_id_wrong is not null')
                # df_study_all = df_study.select('student_id').withColumnRenamed('student_id', 'student_id_all')

                # df_knowledge_right.printSchema()
                # df_knowledge_right.show()
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
                    .withColumn("source_system", f.lit('top_result_ai')) \
                    .withColumn("lu_id", f.lit(0))

                dyf_knowledge = DynamicFrame.fromDF(df_knowledge, glueContext, "df_knowledge")

                applymapping2 = ApplyMapping.apply(frame=dyf_knowledge,
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

                applymapping2.printSchema()
                applymapping2.show(20)

                resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                                    transformation_ctx="resolvechoice3")
                dropnullfields2 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

                print('COUNT df_knowledge: ', dropnullfields2.count())
                dropnullfields2.printSchema()
                dropnullfields2.show(2)

                print('START WRITE TO S3-------------------------')

                datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields2, connection_type="s3",
                                                                         connection_options={
                                                                             "path": "s3://dtsodin/nvn_knowledge/mapping_lo_student_history_v2/",
                                                                             "partitionKeys": ["created_date_id", "source_system"]},
                                                                         format="parquet",
                                                                         transformation_ctx="datasink6")
                print('END WRITE TO S3-------------------------')
                # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields2,
                #                                                            catalog_connection="glue_redshift",
                #                                                            connection_options={
                #                                                                "dbtable": "mapping_lo_student_history",
                #                                                                "database": "dts_odin"
                #                                                            },
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/top_result_ai/",
                #                                                            transformation_ctx="datasink5")

                # END Tru diem cac tu sai

                # xoa cache
                df_study.unpersist()
                df_knowledge_right.unpersist()
                df_knowledge_wrong.unpersist()
                # df_knowledge_right.unpersist()
            except Exception as e:
                print("###################### Exception ##########################")
                print(e)

            # ghi flag
            # lay max key trong data source
            mdl_dyf_top_quiz_attempts = dyf_top_quiz_attempts.toDF()
            flag = mdl_dyf_top_quiz_attempts.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_tu_vung_result_ai.parquet", mode="overwrite")

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
    #     #                                                                          '["] ', ''))
    #     # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.select('right_word',
    #     #                                                                  f.translate(df_nvn_knowledge_vocabulary.right_word,
    #     #                                                                              '["] ', '').alias(
    #     #                                                                      'right_word_new'))
    #     # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col1", f.split(
    #     #     df_nvn_knowledge_vocabulary.right_word_new, ',').alias('col1'))
    #     # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col3",
    #     #                                                                      f.explode(df_nvn_knowledge_vocabulary.col1))
    #     #
    #     #
    #     #
    #     # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col4", splitWord(
    #     #     df_nvn_knowledge_vocabulary.col3))
    #     # # df_nvn_knowledge_vocabulary = df_nvn_knowledge_vocabulary.withColumn("col5",
    #     # #                                                                      f.explode(df_nvn_knowledge_vocabulary.col4))
    #     # print ("nvn_knowledge_vocabulary after: ", df_nvn_knowledge_vocabulary.count())
    #     # df_nvn_knowledge_vocabulary.printSchema()
    #     # df_nvn_knowledge_vocabulary.show()


if __name__ == "__main__":
    main()

