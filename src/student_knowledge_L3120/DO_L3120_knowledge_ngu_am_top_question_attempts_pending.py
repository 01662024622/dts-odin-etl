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
    ## Phonetic
    dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )
    dyf_phonemic = Filter.apply(frame=dyf_learning_object, f=lambda x: x["learning_object_type"] == 'phonetic')
    dyf_phonemic = dyf_phonemic.select_fields(['learning_object_id', 'learning_object_name'])
    # Lay ra ngu am
    df1 = dyf_phonemic.toDF()
    df1 = df1.select('learning_object_id', 'learning_object_name')
    # myArr = np.array(df1.select('phonemic').collect())
    arrPhonetic = [row.learning_object_name for row in df1.collect()]
    arrPhoneticId = [[row.learning_object_name, row.learning_object_id] for row in df1.collect()]

    # print('ARR:', arrPhonetic)
    # print('ARR1 :', (u'i:' in arrPhonetic))

    lu_type = []

    # check value for lu_id: valid = 1, invalid = 0
    def doAddLuId(code):
        code = str(code)
        if code is None:
            return 0
        if code not in lu_type:
            return 0
        else:
            return 1

    add_lu_id = udf(doAddLuId, IntegerType())

    def doCheckLyType(plus, minus):
        if plus == 1:
            return plus
        if minus == 1:
            return minus
        return 0

    check_lu_type = udf(doCheckLyType, IntegerType())

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

    # print('test:', doSplitWord('abcacd'))
    splitWord = udf(lambda x: doSplitWord(x))

    knowledge = [['P01', 'sbasic'], ['P01', 'basic'], ['P02', 'sbasic'], ['P02', 'Basic'], ['P03', 'sbasic'],
                 ['P03', 'basic'], ['P04', 'sbasic'], ['P04', 'basic'], ['L01', None],
                 ['L02', None], ['L03', None], ['L04', None], ['L05', None]]
    comprehension = [['P01', 'sbasic'], ['P01', 'basic'], ['P02', 'sbasic'], ['P02', 'basic'], ['P03', None],
                     ['P03', 'basic'], ['P04', 'sbasic'], ['P04', 'basic'], ['L01', None],
                     ['L02', None], ['L03', None], ['L04', None], ['L05', None]]
    application = [['L04', None], ['L04', None], ['L05', None]]
    analysis = []
    synthesis = []
    evaluation = []

    state_gradedright = 'gradedright'

    def doAddScore(name, parName, state, type):

        arr = []
        score = 0
        if type == 'knowledge':
            arr = knowledge
        if type == 'comprehension':
            arr = comprehension
        if type == 'application':
            arr = application
        if type == 'analysis':
            arr = analysis
        if type == 'synthesis':
            arr = synthesis
        if type == 'evaluation':
            arr = evaluation

        if state is not None and state == state_gradedright:
            score = 2
        else:
            score = -1

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
    special_str = '["].'

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
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_knowledge_ngu_am_top_quest_attempts")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read from index: ', start_read)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_top_question_attempts = Filter.apply(frame=dyf_top_question_attempts, f=lambda x: x['_key'] > start_read)
    # except:
    #     print('read flag file error ')

    print('number of dyf_top_question_attempts: ', dyf_top_question_attempts.count())
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
        # print("COUNT 1:", dyf_join01.count())
        # dyf_join01.printSchema()
        dyf_join02 = Join.apply(dyf_join01, dyf_top_question, 'questionid', 'quest_id')
        # print("COUNT 2:", dyf_join02.count())
        # dyf_join02.printSchema()
        dyf_join03 = Join.apply(dyf_join02, dyf_top_question_categories, 'category', 'quest_cat_id')
        dyf_join03 = Join.apply(dyf_join03, dyf_top_question_categories_parent, 'parent', 'par_id')
        dyf_join03 = Join.apply(dyf_join03, dyf_top_user, 'userid', 'top_user_id')
        # print("COUNT 3:", dyf_join03.count())

        dyf_join03.printSchema()

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

                # print("COUNT 2:", df_right.count())
                # df_right.printSchema()
                dyf_right = DynamicFrame.fromDF(df_right, glueContext, "dyf_right")
                ## Learning Object: loc lay dang tu vung de doc ngu am
                dyf_learning_object = Filter.apply(frame=dyf_learning_object,
                                                   f=lambda x: x["learning_object_type"] == 'vocabulary')
                dyf_learning_object = dyf_learning_object.select_fields(
                    ['learning_object_id', 'learning_object_name', 'transcription'])
                df_learning_object = dyf_learning_object.toDF()
                # replace cac ky tu
                df_learning_object = df_learning_object.withColumn("phone_tic_new",
                                                                   f.translate(df_learning_object.transcription, '\',', ''))

                df_learning_object = df_learning_object.withColumn("phone_tic_tmp",
                                                                   splitWord(df_learning_object.phone_tic_new))
                df_learning_object = df_learning_object.withColumn("phone_tic_tmp_01",
                                                                   f.translate(df_learning_object.phone_tic_tmp, '[]', ''))
                df_learning_object = df_learning_object.withColumn("phone_tic_arr",
                                                                   f.split(df_learning_object.phone_tic_tmp_01, ','))
                df_learning_object = df_learning_object.select('learning_object_id', 'learning_object_name',
                                                               'phone_tic_arr')
                dyf_learning_object = DynamicFrame.fromDF(df_learning_object, glueContext, "dyf_learning_object")

                dyf_knowledge_right = Join.apply(dyf_right, dyf_learning_object, 'right', 'learning_object_name')
                dyf_knowledge_right = dyf_knowledge_right.select_fields(
                    ['student_id', 'learning_object_id', 'name', 'parent', 'timemodified', 'par_name', 'state',
                     'phone_tic_arr'])

                # print("COUNT 3:", dyf_knowledge_right.count())
                # dyf_knowledge_right.printSchema()
                # dyf_knowledge_right.show()
                # # print("COUNT 4:", dyf_knowledge_wrong.count())
                # # dyf_knowledge_wrong.printSchema()
                # Cong diem cac tu dung
                df_knowledge_right = dyf_knowledge_right.toDF()
                df_knowledge_right = df_knowledge_right.withColumn("right_phonetic",
                                                                   f.explode(df_knowledge_right.phone_tic_arr))
                df_knowledge_right = df_knowledge_right.select('student_id', 'name', 'timemodified', 'par_name', 'state',
                                                               'right_phonetic')
                df_knowledge_right = df_knowledge_right.withColumn("learning_object_id",
                                                                   get_phone_tic_id(df_knowledge_right.right_phonetic))

                # dyf_study_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_study_right")

                # dyf_phonemic_right = Join.apply(dyf_study_right, dyf_phonemic, 'right_phonetic', 'learning_object_name')

                # df_knowledge_right = dyf_phonemic_right.toDF()
                df_knowledge_right = df_knowledge_right.withColumn("knowledge", addScore(df_knowledge_right['name'],
                                                                                         df_knowledge_right['par_name'],
                                                                                         df_knowledge_right['state'],
                                                                                         f.lit("knowledge"))) \
                    .withColumn("comprehension", addScore(df_knowledge_right['name'], df_knowledge_right['par_name'],
                                                          df_knowledge_right['state'], f.lit('comprehension'))) \
                    .withColumn("application", addScore(df_knowledge_right['name'], df_knowledge_right['par_name'],
                                                        df_knowledge_right['state'], f.lit('application'))) \
                    .withColumn("analysis", addScore(df_knowledge_right['name'], df_knowledge_right['par_name'],
                                                     df_knowledge_right['state'], f.lit('analysis'))) \
                    .withColumn("synthesis", addScore(df_knowledge_right['name'], df_knowledge_right['par_name'],
                                                      df_knowledge_right['state'], f.lit('synthesis'))) \
                    .withColumn("evaluation", addScore(df_knowledge_right['name'], df_knowledge_right['par_name'],
                                                       df_knowledge_right['state'], f.lit('evaluation'))) \
                    .withColumn("date_id", from_unixtime(df_knowledge_right['timemodified'], 'yyyyMMdd')) \
                    .withColumn("lo_type", f.lit(2))

                # df_knowledge_right.printSchema()
                # df_knowledge_right.show()
                df_knowledge_right.cache()
                # History
                # dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                # dyf_knowledge_right = dyf_knowledge_right.resolveChoice(specs=[('lo_type', 'cast:int')])
                # df_knowledge_right = dyf_knowledge_right.toDF()
                # chon cac truong va kieu du lieu day vao db

                # dyf_ai_history_plus = Filter.apply(frame=dyf_knowledge_right, f=lambda x: x['knowledge'] > 0)
                #
                # dyf_ai_history_minus = Filter.apply(frame=dyf_knowledge_right, f=lambda x: x['knowledge'] < 0)

                df_ai_history_plus = df_knowledge_right.where('knowledge > 0')

                df_ai_history_plus = df_ai_history_plus.groupby('student_id', 'learning_object_id', 'date_id').agg(
                    f.count("student_id").alias("count_plus"), f.sum("knowledge").alias("knowledge_plus"),
                    f.sum("comprehension").alias("comprehension_plus"), f.sum("application").alias("application_plus"),
                    f.sum("analysis").alias("analysis_plus"), f.sum("synthesis").alias("synthesis_plus"),
                    f.sum("evaluation").alias("evaluation_plus"))
                df_ai_history_plus = df_ai_history_plus.withColumnRenamed('student_id', 'student_id_plus') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_plus') \
                    .withColumnRenamed('date_id', 'date_id_plus')
                    # .withColumnRenamed('lu_type', 'lu_type_plus')

                df_ai_history_plus = df_ai_history_plus.where('student_id_plus is not null')

                # dyf_ai_history_plus = DynamicFrame.fromDF(df_ai_history_plus, glueContext, "dyf_ai_history_plus")
                #
                # dyf_ai_history_plus = dyf_ai_history_plus.select_fields(
                #     ['date_id', 'student_id', 'learning_object_id', 'lo_type', 'knowledge_plus', 'comprehension_plus',
                #      'application_plus', 'analysis_plus',
                #      'synthesis_plus', 'evaluation_plus', 'count_plus']).rename_field('student_id',
                #                                                                       'student_id_plus').rename_field(
                #     'date_id', 'date_id_plus').rename_field('lo_type', 'lo_type_plus').rename_field('id',
                #                                                                                     'learning_object_id_plus')

                df_ai_history_minus = df_knowledge_right.where('knowledge < 0')
                df_ai_history_minus = df_ai_history_minus.groupby('student_id', 'learning_object_id', 'date_id').agg(
                    f.count("student_id").alias("count_minus"),
                    f.sum("knowledge").alias("knowledge_minus"),
                    f.sum("comprehension").alias("comprehension_minus"),
                    f.sum("application").alias("application_minus"),
                    f.sum("analysis").alias("analysis_minus"),
                    f.sum("synthesis").alias("synthesis_minus"),
                    f.sum("evaluation").alias("evaluation_minus"))
                df_ai_history_minus = df_ai_history_minus.withColumnRenamed('student_id', 'student_id_minus') \
                    .withColumnRenamed('learning_object_id', 'learning_object_id_minus') \
                    .withColumnRenamed('date_id', 'date_id_minus')
                    # .withColumnRenamed('lu_type', 'lu_type_minus')

                df_ai_history_minus = df_ai_history_minus.where('student_id_minus is not null')
                print("AAAAAAAAAAAAAAAAAAAAAAAA")

                # dyf_ai_history_minus = DynamicFrame.fromDF(df_ai_history_minus, glueContext, "dyf_ai_history_plus")
                # dyf_ai_history_minus = dyf_ai_history_minus.select_fields(
                #     ['date_id', 'student_id', 'id', 'lo_type', 'knowledge_minus', 'comprehension_minus',
                #      'application_minus', 'analysis_minus', 'synthesis_minus', 'evaluation_minus',
                #      'count_minus']).rename_field('student_id', 'user_id_minus').rename_field(
                #     'date_id', 'date_id_minus').rename_field('lo_type', 'lo_type_minus').rename_field('id',
                #                                                                                       'learning_object_id_minus')

                # dyf_ai_history_minus.printSchema()
                # dyf_ai_history_minus.show(2)
                # dyf_ai_history_plus.printSchema()
                # dyf_ai_history_plus.show(2)

                print ("###########################################")
                # df_ai_history_minus = dyf_ai_history_minus.toDF()
                # df_ai_history_plus = dyf_ai_history_plus.toDF()
                df_join_history = df_ai_history_plus.join(df_ai_history_minus, (
                            df_ai_history_plus['student_id_plus'] == df_ai_history_minus['student_id_minus']) &
                                                          (df_ai_history_plus['date_id_plus'] == df_ai_history_minus[
                                                              'date_id_minus']) &
                                                          (df_ai_history_plus['learning_object_id_plus'] ==
                                                           df_ai_history_minus['learning_object_id_minus']), 'outer')

                df_join_history = df_join_history.withColumn("created_date_id", check_data_null(df_join_history.date_id_plus, df_join_history.date_id_minus)) \
                    .withColumn("user_id",
                                check_data_null(df_join_history.student_id_plus, df_join_history.student_id_minus)) \
                    .withColumn("source_system", f.lit("top_question_attempt_phonetic")) \
                    .withColumn("learning_object_id", check_data_null(df_join_history.learning_object_id_plus,
                                                                      df_join_history.learning_object_id_minus)) \
                    .withColumn("lu_id", f.lit(0))
                    # .withColumn("lu_id", check_lu_type(df_join_history.lu_type_plus, df_join_history.lu_type_minus))
                join_history = DynamicFrame.fromDF(df_join_history, glueContext, 'join_history')
                # join_history.printSchema()
                # join_history.printSchema()
                ################
                applymapping1 = ApplyMapping.apply(frame=join_history,
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
                resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                     transformation_ctx="resolvechoice1")
                dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields")

                print(dropnullfields1.count())
                dropnullfields1.show(5)

                print('START WRITE TO S3-------------------------')
                datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
                                                                         connection_options={
                                                                             "path": "s3://dts-odin/nvn_knowledge/mapping_lo_student_history/"},
                                                                         format="parquet",
                                                                         transformation_ctx="datasink6")
                print('END WRITE TO S3-------------------------')

                datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                           catalog_connection="glue_redshift",
                                                                           connection_options={
                                                                               "dbtable": "mapping_lo_student_history",
                                                                               "database": "dts_odin"
                                                                           },
                                                                           redshift_tmp_dir="s3n://dts-odin/temp1/mapping_lo_student_history/",
                                                                           transformation_ctx="datasink5")


                # dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                # dyf_knowledge_right = dyf_knowledge_right.resolveChoice(specs=[('lo_type', 'cast:byte')])
                # # df_knowledge_right = dyf_knowledge_right.toDF()
                # # chon cac truong va kieu du lieu day vao db
                # applymapping = ApplyMapping.apply(frame=dyf_knowledge_right,
                #                                   mappings=[("timemodified", "long", "timestart", "long"),
                #                                             ("name", "string", "name", "string"),
                #                                             ("par_name", "string", "par_name", "string"),
                #                                             ("student_id", 'int', 'student_id', 'long'),
                #                                             ("learning_object_id", "long", "learning_object_id", "int"),
                #                                             ("date_id", "string", "date_id", "long"),
                #                                             ("knowledge", "int", "knowledge", "long"),
                #                                             ("comprehension", "int", "comprehension", "long"),
                #                                             ("application", "int", "application", "long"),
                #                                             ("analysis", "int", "analysis", "long"),
                #                                             ("synthesis", "int", "synthesis", "long"),
                #                                             ("evaluation", "int", "evaluation", "long"),
                #                                             ("phone_tic", "string", "phone_tic", "long"),
                #                                             ("lo_type", "byte", "lo_type", "int")])
                # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                #                                     transformation_ctx="resolvechoice2")
                # dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")



                # xoa cache
                df_right.unpersist()
                df_knowledge_right.unpersist()
                # df_knowledge_right.unpersist()

                # lay max _key tren datasource
                df_temp = dyf_top_question_attempts.toDF()
                flag = df_temp.agg({"_key": "max"}).collect()[0][0]
                flag_data = [flag]
                df = spark.createDataFrame(flag_data, "long").toDF('flag')

                # ghi de flag moi vao s3
                df.write.parquet("s3a://dts-odin/flag/flag_knowledge_ngu_am_top_quest_attempts", mode="overwrite")

            except Exception as e:
                print("###################### Exception ##########################")
                print(e)


if __name__ == "__main__":
    main()
