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
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # ETL TBHV
    ## Phonetic
    dyf_phonemic = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="phonemic"
    )
    dyf_phonemic = dyf_phonemic.select_fields(
        ['id', 'phonemic'])
    df1 = dyf_phonemic.toDF()
    df1.cache()
    df1 = df1.select('phonemic')
    # myArr = np.array(df1.select('phonemic').collect())
    arrPhonetic = [row.phonemic for row in df1.collect()]

    # print('ARR:', arrPhonetic)
    # print('ARR1 :', (u'i:' in arrPhonetic))

    # Custom function
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
                 ['L02', None], ['L03', None], ['L04', None], ['L05', None], [None, 'DICTATION'], [None, 'LISTENING']]
    comprehension = [['P01', 'sbasic'], ['P01', 'basic'], ['P02', 'sbasic'], ['P02', 'basic'], ['P03', None],
                     ['P03', 'basic'], ['P04', 'sbasic'], ['P04', 'basic'], ['L01', None],
                     ['L02', None], ['L03', None], ['L04', None], ['L05', None], [None, 'DICTATION'],
                     [None, 'LISTENING']]
    application = [['L04', None], ['L04', None], ['L05', None], [None, 'LISTENING']]
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

        if state == state_gradedright:
            score = 2
        if state != state_gradedright:
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

    ######### top_question_attempt_steps
    dyf_top_question_attempt_steps = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_question_attempt_steps_092019"
    )
    dyf_top_question_attempt_steps = dyf_top_question_attempt_steps.select_fields(
        ['_key', 'id', 'questionattemptid', 'state', 'userid']).rename_field('id', 'steps_id')

    try:
        # # doc moc flag tu s3
        # df_flag = spark.read.parquet("s3://dts-odin/flag/flag_knowledge_ngu_am_top_quest_attempts")
        # start_read = df_flag.collect()[0]['flag']
        # print('read from index: ', start_read)
        start_read = 22000000
        end_read = 24000000
        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_top_question_attempt_steps = Filter.apply(frame=dyf_top_question_attempt_steps, f=lambda x: x['_key'] >= start_read and x['_key'] < end_read)
    except:
        print('read flag file error ')
    df_temp = dyf_top_question_attempt_steps.toDF()
    df_temp.cache()
    print('COUNT df_temp:' , df_temp.count())
    dyf_top_question_attempt_steps = DynamicFrame.fromDF(df_temp, glueContext, "dyf_right")
    # print('number of dyf_top_question_attempt_steps: ', dyf_top_question_attempt_steps.count())
    if dyf_top_question_attempt_steps.count() > 0:
        ########## dyf_top_user
        dyf_top_user = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="do_top_user"
        )
        dyf_top_user = dyf_top_user.select_fields(
            ['id', 'student_id']).rename_field('id', 'top_user_id')

        ########## top_question_attempts
        dyf_top_question_attempts = glueContext.create_dynamic_frame.from_catalog(
            database="moodle",
            table_name="top_question_attempts_092019"
        )
        dyf_top_question_attempts = dyf_top_question_attempts.select_fields(
            ['id', 'rightanswer', 'questionid', 'timemodified'])
        # dyf_top_quiz_attempts = dyf_top_quiz_attempts.resolveChoice(specs=[('_key', 'cast:long')])
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
        dyf_join01 = Join.apply(dyf_top_question_attempt_steps, dyf_top_question_attempts, 'questionattemptid', 'id')
        # print("COUNT 1:", dyf_join01.count())
        # dyf_join01.printSchema()
        dyf_join02 = Join.apply(dyf_join01, dyf_top_question, 'questionid', 'quest_id')
        # print("COUNT 2:", dyf_join02.count())
        # dyf_join02.printSchema()
        dyf_join03 = Join.apply(dyf_join02, dyf_top_question_categories, 'category', 'quest_cat_id')
        dyf_join03 = Join.apply(dyf_join03, dyf_top_question_categories_parent, 'parent', 'par_id')

        # print("COUNT dyf_join03 1:", dyf_join03.count())
        # print("COUNT dyf_top_user:", dyf_top_user.count())
        # dyf_join03.printSchema()
        # dyf_top_user.printSchema()
        # dyf_join03.show(5)
        # dyf_top_user.show(5)
        dyf_join03 = Join.apply(dyf_join03, dyf_top_user, 'userid', 'top_user_id')

        # print("COUNT dyf_join03 2:", dyf_join03.count())
        # dyf_join03.printSchema()

        dyf_join03 = dyf_join03.select_fields(
            ['student_id', 'rightanswer', 'timemodified', 'state', 'name', 'parent', 'par_name'])
        arrName = ['V01', 'V02', 'V03', 'V04', 'V05', 'G01', 'G02', 'G03', 'G04', 'G05', 'P01', 'P02', 'P03', 'P04',
                   'P05']
        arrParName = ['CONVERSATIONAL_EXPRESSION', 'VOCABULARY', 'READING']
        dyf_join03 = Filter.apply(frame=dyf_join03, f=lambda x: x["name"] in arrName or x["par_name"] in arrParName)
        # dyf_join03.printSchema()
        # dyf_join03.show()
        # dyf_right = Filter.apply(frame=dyf_join03, f=lambda x: x["state"] == state_gradedright)
        # dyf_wrong = Filter.apply(frame=dyf_join03, f=lambda x: x["state"] != state_gradedright)

        # dyf_join02.show()
        df_right = dyf_join03.toDF()
        # df_right.cache()
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
                df_right.printSchema()
                dyf_right = DynamicFrame.fromDF(df_right, glueContext, "dyf_right")
                ## Learning Object
                dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
                    database="nvn_knowledge",
                    table_name="nvn_knowledge_learning_object"
                )
                dyf_learning_object = dyf_learning_object.select_fields(
                    ['learning_object_id', 'learning_object_name', 'phone_tic'])
                df_learning_object = dyf_learning_object.toDF()
                # replace cac ky tu
                df_learning_object = df_learning_object.withColumn("phone_tic_new",
                                                                   f.translate(df_learning_object.phone_tic, '\',', ''))

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
                df_knowledge_right.cache()
                df_knowledge_right = df_knowledge_right.withColumn("right_phonetic",
                                                                   f.explode(df_knowledge_right.phone_tic_arr))
                df_knowledge_right = df_knowledge_right.select('student_id', 'name', 'timemodified', 'par_name', 'state',
                                                               'right_phonetic')
                dyf_study_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_study_right")

                dyf_phonemic_right = Join.apply(dyf_study_right, dyf_phonemic, 'right_phonetic', 'phonemic')

                df_knowledge_right = dyf_phonemic_right.toDF()
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

                dyf_knowledge_right = DynamicFrame.fromDF(df_knowledge_right, glueContext, "dyf_knowledge_right")
                dyf_knowledge_right = dyf_knowledge_right.resolveChoice(specs=[('lo_type', 'cast:byte')])
                # df_knowledge_right = dyf_knowledge_right.toDF()
                # chon cac truong va kieu du lieu day vao db
                applymapping = ApplyMapping.apply(frame=dyf_knowledge_right,
                                                  mappings=[("timemodified", "long", "timestart", "long"),
                                                            ("name", "string", "name", "string"),
                                                            ("par_name", "string", "par_name", "string"),
                                                            ("student_id", 'int', 'student_id', 'long'),
                                                            ("id", "int", "learning_object_id", "int"),
                                                            ("date_id", "string", "date_id", "long"),
                                                            ("knowledge", "int", "knowledge", "long"),
                                                            ("comprehension", "int", "comprehension", "long"),
                                                            ("application", "int", "application", "long"),
                                                            ("analysis", "int", "analysis", "long"),
                                                            ("synthesis", "int", "synthesis", "long"),
                                                            ("evaluation", "int", "evaluation", "long"),
                                                            ("phone_tic", "string", "phone_tic", "long"),
                                                            ("lo_type", "byte", "lo_type", "int")])
                resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                    transformation_ctx="resolvechoice2")
                dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

                datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                           catalog_connection="glue_redshift",
                                                                           connection_options={
                                                                               "dbtable": "t_temp_right_learning_object_02",
                                                                               "database": "dts_odin",
                                                                               "postactions": """ call proc_knowledge_ngu_am_top_question_attempts () """
                                                                           },
                                                                           redshift_tmp_dir="s3n://dts-odin/temp1/",
                                                                           transformation_ctx="datasink5")

                # xoa cache
                # df_right.unpersist()
                # df_knowledge_right.unpersist()
                # df_knowledge_right.unpersist()

                # lay max _key tren datasource
                df_temp = dyf_top_question_attempt_steps.toDF()
                flag = df_temp.agg({"_key": "max"}).collect()[0][0]
                flag_data = [flag]
                df = spark.createDataFrame(flag_data, "long").toDF('flag')

                # ghi de flag moi vao s3
                df.write.parquet("s3a://dts-odin/flag/flag_knowledge_ngu_am_top_quest_attempts", mode="overwrite")

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
