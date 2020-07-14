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
    # Them truong hop ['V05', 'SBISIC'] do data tren production dang luu sai.
    # Tu SBASIC dang luu sai thanh SBISIC
    comprehension = [['V04', 'INTER'], ['V05', 'SBASIC'], ['V05', 'BASIC'], ['V05', 'SBISIC']]

    application = [['V05', 'SBASIC'], ['V05', 'BASIC'], ['V05', 'SBISIC']]

    state_gradedright = 'gradedright'

    def doAddScore(name, parName, state, type):
        arr = []
        score = 0

        if type == 'comprehension':
            arr = comprehension

        if type == 'application':
            arr = application

        if state is not None and state == state_gradedright:
            score = 10
        else:
            score = -5

        if type == 'knowledge':
            return score
        else:
            for x in arr:
                if x[0] is None and x[1] == parName:
                    return score
                if x[0] == name and x[1] is None:
                    return score
                if x[0] == name and x[1] is not None and x[1].lower() in parName.lower():
                    return score
            return 0

    addScore = udf(doAddScore, IntegerType())

    # print('CHECK:', checkContains('ABCD EFHFF'))

    # chuoi ky tu can replace
    special_str = '["];.,'

    ########## top_question_attempts
    dyf_top_question_attempts = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_question_attempts"
    )
    dyf_top_question_attempts = dyf_top_question_attempts.select_fields(
        ['_key', 'id', 'rightanswer', 'questionid', 'questionusageid', 'timemodified'])
    dyf_top_question_attempts = dyf_top_question_attempts.resolveChoice(specs=[('_key', 'cast:long')])

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_question_attempts")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read from index: ', start_read)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_top_question_attempts = Filter.apply(frame=dyf_top_question_attempts, f=lambda x: x['_key'] > start_read)
    # except:
    #     print('read flag file error ')

    if dyf_top_question_attempts.count() > 0:
        ########## dyf_top_user
        dyf_top_user = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="do_top_user"
        )
        dyf_top_user = dyf_top_user.select_fields(
            ['id', 'student_id']).rename_field('id', 'top_user_id')

        ######### top_quiz_attempts
        dyf_top_quiz_attempts = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_quiz_attempts"
        )
        dyf_top_quiz_attempts = dyf_top_quiz_attempts.select_fields(
            ['userid', 'uniqueid'])

        ######### top_question_attempt_steps
        dyf_top_question_attempt_steps = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question_attempt_steps"
        )
        dyf_top_question_attempt_steps = dyf_top_question_attempt_steps.select_fields(
            ['id', 'questionattemptid', 'state']).rename_field('id', 'steps_id')

        print dyf_top_question_attempts.count()
        dyf_top_question_attempts.show(2)

        dyf_top_question_attempts = Filter.apply(frame=dyf_top_question_attempts,
                                             f=lambda x: x["timemodified"] >= timestamp)

        print dyf_top_question_attempts.count()
        dyf_top_question_attempts.show()

        ######### top_question
        dyf_top_question = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question"
        )
        dyf_top_question = dyf_top_question.select_fields(
            ['id', 'name', 'category']).rename_field('id', 'quest_id')
        # dyf_top_result_ai = dyf_top_result_ai.resolveChoice(specs=[('_key', 'cast:long')])

        ######### top_question_categories
        dyf_top_question_categories = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question_categories"
        )
        dyf_top_question_categories = dyf_top_question_categories.select_fields(
            ['id', 'name', 'parent']).rename_field('id', 'quest_cat_id')

        ######### dyf_top_question_categories_parent
        dyf_top_question_categories_parent = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question_categories"
        )
        dyf_top_question_categories_parent = dyf_top_question_categories_parent.select_fields(
            ['id', 'name']).rename_field('id', 'par_id').rename_field('name', 'par_name')

        # print("COUNT dyf_top_question_attempts:", dyf_top_question_attempts.count())
        # print("COUNT dyf_top_question:", dyf_top_question.count())
        # print("COUNT dyf_top_question_attempt_steps:", dyf_top_question_attempt_steps.count())
        # print("COUNT dyf_top_question_categories:", dyf_top_question_categories.count())
        # dyf_top_question_attempt_steps = Filter.apply(frame=dyf_top_question_attempt_steps, f=lambda x: x["steps_id"])

        # JOIN va FILTER cac bang theo dieu kien
        dyf_join = Join.apply(dyf_top_question_attempts, dyf_top_quiz_attempts, 'questionusageid', 'uniqueid')

        dyf_top_question_attempt_steps = Filter.apply(frame=dyf_top_question_attempt_steps,
                                                      f=lambda x: x["state"] == state_gradedright)
        df_top_question_attempt_steps = dyf_top_question_attempt_steps.toDF()
        df_join = dyf_join.toDF()
        df_join01 = df_join.join(df_top_question_attempt_steps,
                                                   (df_join['id'] == df_top_question_attempt_steps['questionattemptid']), 'left')

        dyf_join01 = DynamicFrame.fromDF(df_join01, glueContext, "dyf_join01")

        # dyf_join01 = Join.apply(dyf_top_question_attempt_steps, dyf_top_question_attempts, 'questionattemptid', 'id')
        print("COUNT 1:", dyf_join01.count())
        dyf_join01.printSchema()

        dyf_join02 = Join.apply(dyf_join01, dyf_top_question, 'questionid', 'quest_id')
        # print("COUNT 2:", dyf_join02.count())
        # dyf_join02.printSchema()
        dyf_join03 = Join.apply(dyf_join02, dyf_top_question_categories, 'category', 'quest_cat_id')
        dyf_join03 = Join.apply(dyf_join03, dyf_top_question_categories_parent, 'parent', 'par_id')
        dyf_join03 = Join.apply(dyf_join03, dyf_top_user, 'userid', 'top_user_id')
        # print("COUNT 3:", dyf_join03.count())
        dyf_join03.printSchema()
        # print "total dyf_join_steps: ", dyf_join_steps.count()
        # dyf_join_steps.show(10)
        # dyf_join03.show(5)

        arrName = ['V01', 'V02', 'V03', 'V04', 'V05']
        # arrParName = ['CONVERSATIONAL_EXPRESSION', 'VOCABULARY', 'READING']
        dyf_join03 = Filter.apply(frame=dyf_join03, f=lambda x: x["name"] in arrName)

        dyf_join03 = dyf_join03.select_fields(
            ['student_id', 'rightanswer', 'timemodified', 'state', 'name', 'parent', 'par_name'])
        # dyf_join03.printSchema()
        # dyf_join03.show()
        # dyf_right = Filter.apply(frame=dyf_join03, f=lambda x: x["state"] == state_gradedright)
        # dyf_wrong = Filter.apply(frame=dyf_join03, f=lambda x: x["state"] != state_gradedright)

        # dyf_join02.show()
        df_right = dyf_join03.toDF()
        df_right.cache()
        if (df_right.count() > 0):
            try:
                # print("COUNT 1:", df_right.count())
                # Loc cac ky tu dac biet [ ] ",

                # Tach cau thanh array tu:
                # house, her => [house, her]
                df_right = df_right.withColumn("right_str", f.translate(df_right.rightanswer, special_str, ''))
                df_right = df_right.withColumn("right_arr", f.split(df_right.right_str, ' '))

                # Split column array => nhieu row
                # row: [house, her] =>
                # row1: house
                # row2: her
                df_right = df_right.withColumn("right",
                                               f.explode(df_right.right_arr))
                df_right = df_right.withColumn("right", f.lower(f.col("right")))
                # print("COUNT 2:", df_right.count())
                # df_right.printSchema()
                dyf_right = DynamicFrame.fromDF(df_right, glueContext, "dyf_right")
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
                dyf_learning_object.printSchema()
                dyf_learning_object.show()

                dyf_knowledge_all = Join.apply(dyf_right, dyf_learning_object, 'right', 'learning_object_name')
                dyf_knowledge_all = dyf_knowledge_all.select_fields(
                    ['student_id', 'learning_object_id', 'name', 'parent', 'timemodified', 'par_name', 'state'])
                print("COUNT 3:", dyf_knowledge_all.count())
                dyf_knowledge_all.printSchema()
                dyf_knowledge_all.show(10)
                # # print("COUNT 4:", dyf_knowledge_wrong.count())
                # # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                df_knowledge_all = dyf_knowledge_all.toDF()
                df_knowledge_all.cache()

                # dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                # dyf_knowledge_right = dyf_knowledge_right.map(lambda x: doCheckContains(x, comprehension, 'comprehension'))

                df_knowledge_all = df_knowledge_all.withColumn("knowledge", addScore(df_knowledge_all['name'],df_knowledge_all['par_name'],df_knowledge_all['state'],f.lit('knowledge')))\
                    .withColumn("comprehension", addScore(df_knowledge_all['name'],df_knowledge_all['par_name'],df_knowledge_all['state'],f.lit('comprehension')))\
                    .withColumn("application", addScore(df_knowledge_all['name'], df_knowledge_all['par_name'],df_knowledge_all['state'], f.lit('application')))\
                    .withColumn("analysis", f.lit(0))\
                    .withColumn("synthesis", f.lit(0))\
                    .withColumn("evaluation", f.lit(0))\
                    .withColumn("date_id", from_unixtime(df_knowledge_all['timemodified'], 'yyyyMMdd'))

                print "df_knowledge_all ", df_knowledge_all.count()
                df_knowledge_right = df_knowledge_all.where('knowledge is not null and knowledge > 0')
                df_knowledge_wrong = df_knowledge_all.where('knowledge is not null and knowledge < 0')
                # df_knowledge_right.printSchema()
                print "df_knowledge_right ", df_knowledge_right.count()
                print "df_knowledge_wrong ", df_knowledge_wrong.count()

                df_knowledge_right = df_knowledge_right.groupby('student_id', 'date_id',
                                                                                  'learning_object_id').agg(
                    f.count('knowledge').alias("count_plus"),
                    f.sum('knowledge').alias("knowledge_plus"),
                    f.sum('comprehension').alias("comprehension_plus"),
                    f.sum('application').alias("application_plus"),
                    f.sum('analysis').alias("analysis_plus"),
                    f.sum('synthesis').alias("synthesis_plus"),
                    f.sum('evaluation').alias("evaluation_plus"))\
                    .withColumnRenamed('student_id', 'student_id_plus') \
                    .withColumnRenamed('date_id', 'date_id_plus') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_plus')
                df_knowledge_right = df_knowledge_right.where('student_id_plus is not null')

                df_knowledge_wrong = df_knowledge_wrong.groupby('student_id', 'date_id',
                                                                'learning_object_id').agg(
                    f.count('knowledge').alias("count_minus"),
                    f.sum('knowledge').alias("knowledge_minus"),
                    f.sum('comprehension').alias("comprehension_minus"),
                    f.sum('application').alias("application_minus"),
                    f.sum('analysis').alias("analysis_minus"),
                    f.sum('synthesis').alias("synthesis_minus"),
                    f.sum('evaluation').alias("evaluation_minus")) \
                    .withColumnRenamed('student_id', 'student_id_minus') \
                    .withColumnRenamed('date_id', 'date_id_minus') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_minus')
                df_knowledge_wrong = df_knowledge_wrong.where('student_id_minus is not null')

                df_knowledge = df_knowledge_right.join(df_knowledge_wrong, (
                        df_knowledge_right['student_id_plus'] == df_knowledge_wrong['student_id_minus']) & (
                                                               df_knowledge_right['date_id_plus'] ==
                                                               df_knowledge_wrong['date_id_minus']) & (
                                                               df_knowledge_right['learning_object_id_plus'] ==
                                                               df_knowledge_wrong['learning_object_id_minus']), 'outer')
                df_knowledge = df_knowledge.withColumn("user_id",
                                check_data_null(df_knowledge.student_id_plus, df_knowledge.student_id_minus)) \
                    .withColumn("learning_object_id",
                                check_data_null(df_knowledge.learning_object_id_plus, df_knowledge.learning_object_id_minus)) \
                    .withColumn("created_date_id",
                                check_data_null(df_knowledge.date_id_plus, df_knowledge.date_id_minus)) \
                    .withColumn("source_system", f.lit('top_question_attempt')) \
                    .withColumn("lu_id", f.lit(0))

                dyf_knowledge_all = DynamicFrame.fromDF(df_knowledge, glueContext, "dyf_knowledge_all")

                # dyf_knowledge = DynamicFrame.fromDF(df_knowledge, glueContext, "df_knowledge")

                print("total: ", dyf_knowledge_all.count())

                applymapping2 = ApplyMapping.apply(frame=dyf_knowledge_all,
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
                resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                                     transformation_ctx="resolvechoice3")
                dropnullfields2 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")

                # print('COUNT df_knowledge: ', dropnullfields2.count())
                # dropnullfields2.printSchema()
                # dropnullfields2.show()

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
                #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/top_question_attempt/",
                #                                                            transformation_ctx="datasink5")

                # Tru diem cac tu sai: Xu lu tuong tu tu dung.

                # xoa cache
                df_right.unpersist()
                df_knowledge_right.unpersist()
                # df_knowledge_right.unpersist()
            except Exception as e:
                print("###################### Exception ##########################")
                print(e)

            # ghi flag
            # lay max key trong data source
            df_temp = dyf_top_question_attempts.toDF()
            flag = df_temp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_question_attempts", mode="overwrite")

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
