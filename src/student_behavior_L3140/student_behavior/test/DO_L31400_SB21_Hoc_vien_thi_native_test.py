import sys
from pyspark import StorageLevel
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, LongType, MapType, FloatType
from pyspark.sql.functions import when
from datetime import date, datetime, timedelta
import pytz
from pyspark.sql.window import Window
import json

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = True
is_just_monthly_exam = False
is_limit_test = False
START_LOAD_DATE = 1564592400L

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'

def get_df_student_level(glueContext):
    dyf_student_level = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_level",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )

    dyf_student_level = dyf_student_level.select_fields(
        ['contact_id', 'level_code', 'start_date', 'end_date']) \
        .rename_field('contact_id', 'contact_id_level') \
        .rename_field('level_code', 'student_level_code')

    if is_dev:
        print('dyf_student_level__original')
        dyf_student_level.printSchema()
        dyf_student_level.show(3)

    return dyf_student_level.toDF()


def get_df_student_package(glueContext):
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_package",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )

    dyf_student_package = dyf_student_package.select_fields(
        ['contact_id', 'package_code', 'package_status_code',
         'package_start_time', 'package_end_time']) \
        .rename_field('contact_id', 'contact_id_package')

    if is_dev:
        print('dyf_student_package__original')
        dyf_student_package.printSchema()
        dyf_student_package.show(3)

    return dyf_student_package.toDF()


def get_df_student_advisor(glueContext):
    dyf_student_advisor = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_advisor",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )

    # | -- student_advisor_id: long
    # | -- user_id: long
    # | -- contact_id: string
    # | -- advisor_id: long
    # | -- start_date: long
    # | -- end_date: long
    # | -- created_at: long
    # | -- updated_at: long

    dyf_student_advisor = dyf_student_advisor.select_fields(
        ['contact_id', 'advisor_id', 'start_date',
         'end_date']) \
        .rename_field('contact_id', 'contact_id_advisor')

    if is_dev:
        print('dyf_student_advisor__original')
        dyf_student_advisor.printSchema()
        dyf_student_advisor.show(3)

    return dyf_student_advisor.toDF()


def concaText(student_behavior_date, behavior_id, student_id, contact_id,
              package_code, package_status_code, student_level_code,
              transformed_at):
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
    # if package_endtime is  not None:
    #     text_concat += str(package_endtime)
    # if package_starttime is not None:
    #     text_concat += str(package_starttime)
    if student_level_code is not None:
        text_concat += str(student_level_code)
    if package_status_code is not None:
        text_concat += str(package_status_code)
    if transformed_at is not None:
        text_concat += str(transformed_at)
    return text_concat

concaText = f.udf(concaText, StringType())



BEHAVIOR_ID_TEST_TUAN = 22L
BEHAVIOR_ID_TEST_THANG = 23L

def getBehaviorIdByQuiz(quiz):
    if quiz in [6L, 7L, 9L, 918L]:
        return BEHAVIOR_ID_TEST_THANG
    return BEHAVIOR_ID_TEST_TUAN

getBehaviorIdByQuiz = f.udf(getBehaviorIdByQuiz, LongType())

def main():

    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second =  long(today.strftime("%s"))
    print('today_id: ', today_second)


    #------------------------------------------------------------------------------------------------------------------#


    # ------------------------------------------------------------------------------------------------------------------#
    # diem thu tuan
    dyf_top_topica_question_mark_week = glueContext.create_dynamic_frame.from_catalog(database="moodle",
                                                                        table_name="top_topica_question_mark_week")

    dyf_top_topica_question_mark_week = dyf_top_topica_question_mark_week\
        .select_fields(['attemptid', 'grade', 'quiz_name'])\
        .rename_field('attemptid', 'attemptid_mark_week') \
        .rename_field('grade', 'grade_mark_week')\
        .rename_field('quiz_name', 'quiz_name_week')

    dyf_top_topica_question_mark_week = dyf_top_topica_question_mark_week\
        .resolveChoice(specs=[('attemptid_mark_week', 'cast:long'),
                              ('grade_mark_week', 'cast:float')])

    df_top_topica_question_mark_week = dyf_top_topica_question_mark_week.toDF()
    df_top_topica_question_mark_week = df_top_topica_question_mark_week.dropDuplicates(['attemptid_mark_week'])

    if is_dev:
        print ('df_top_topica_question_mark_week')
        df_top_topica_question_mark_week.printSchema()

    # diem thi thang
    dyf_top_topica_question_marks = glueContext.create_dynamic_frame.from_catalog(database="moodle",
                                                                        table_name="top_topica_question_marks")

    dyf_top_topica_question_marks = dyf_top_topica_question_marks \
        .select_fields(['attemptid', 'marks']) \
        .rename_field('attemptid', 'attemptid_mark')\
        .rename_field('marks', 'marks_month')

    dyf_top_topica_question_marks = dyf_top_topica_question_marks \
        .resolveChoice(specs=[('attemptid_mark', 'cast:long')])

    df_top_topica_question_marks = dyf_top_topica_question_marks.toDF()
    df_top_topica_question_marks = df_top_topica_question_marks.dropDuplicates(['attemptid_mark'])

    if is_dev:
        print ('df_top_topica_question_marks')
        df_top_topica_question_marks.printSchema()

    # ------------------------------------------------------------------------------------------------------------------#
    # dyf_student_package = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
    #                                                                     table_name="student_package")
    # 
    # print('dyf_student_package__0')
    # dyf_student_package.printSchema()
    # 
    # dyf_student_package = dyf_student_package \
    #     .select_fields(['student_id', 'package_code', 'start_time', 'end_time']) \
    #     .rename_field('student_id', 'student_id_pk')
    # 
    # dyf_student_package = dyf_student_package.resolveChoice(
    #     specs=[('start_time', 'cast:long'), ('end_time', 'cast:long')])
    # 
    # df_student_package = dyf_student_package.toDF()
    # df_student_package = df_student_package.drop_duplicates()

    # ------------------------------------------------------------------------------------------------------------------#
    # dyf_student_package_status = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
    #                                                                            table_name="student_status")
    #
    # dyf_student_package_status = dyf_student_package_status \
    #     .select_fields(['contact_id', 'status_code', 'start_date', 'end_date']) \
    #     .rename_field('contact_id', 'contact_id_ps')
    #
    # print('dyf_student_package_status::drop_duplicates')
    #
    # df_student_package_status = dyf_student_package_status.toDF()
    # df_student_package_status = df_student_package_status.drop_duplicates()
    # ------------------------------------------------------------------------------------------------------------------#
    dyf_result_ai = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_result_ai"
    )
    dyf_result_ai = dyf_result_ai.select_fields(
        ['id', 'answer', '.speech_result', 'right_word', 'wrong_word', 'result', 'attempt_id'])\
        .rename_field('attempt_id', 'attempt_id_result_ai')

    dyf_result_ai = dyf_result_ai.resolveChoice(specs=[('attempt_id_result_ai', 'cast:long')])
    df_result_ai = dyf_result_ai.toDF()
    df_result_ai = df_result_ai.drop_duplicates(['attempt_id_result_ai'])


    # ------------------------------------------------------------------------------------------------------------------#
    dyf_moodle_top_user = glueContext.create_dynamic_frame.from_catalog(database="moodle",
                                                                        table_name="top_user")

    # Chon cac truong can thiet
    dyf_moodle_top_user = dyf_moodle_top_user.select_fields(
        ['id', 'username', 'levelstudy'])

    df_moodle_top_user = dyf_moodle_top_user.toDF()
    #------------------------------------------------------------------------------------------------------------------#
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                        table_name="student_contact")

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id', 'user_name'])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                      f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                  and x["student_id"] is not None and x["student_id"] != ''
                                                  and x["user_name"] is not None and x["user_name"] != '')


    df_student_contact = dyf_student_contact.toDF()

    # -------------------------------------------------------------------------------------------------------------------#

    dyf_moodle_question_attempts = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_question_attempts"
    )

    dyf_moodle_question_attempts = dyf_moodle_question_attempts.select_fields(
        ['id', 'rightanswer', 'responsesummary', 'timemodified', 'maxmark', 'questionusageid',
         'questionid']).rename_field('id', 'question_attempt_id')

    dyf_moodle_question_attempts = Filter.apply(frame=dyf_moodle_question_attempts,
                                                f=lambda x: x["questionusageid"] is not None and x["questionusageid"] != '')

    df_moodle_question_attempts = dyf_moodle_question_attempts.toDF()
    df_moodle_question_attempts = df_moodle_question_attempts.dropDuplicates(['questionusageid'])
    # -------------------------------------------------------------------------------------------------------------------#

    dyf_top_quiz = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_quiz"
    )
    dyf_top_quiz = dyf_top_quiz.select_fields(['name', 'id']).rename_field('id', 'quiz_id')

    df_top_quiz = dyf_top_quiz.toDF()
    # -------------------------------------------------------------------------------------------------------------------#
    dyf_moodle_question_steps = glueContext.create_dynamic_frame.from_catalog(
        database="moodle",
        table_name="top_question_attempt_steps"
    )
    dyf_moodle_question_steps = dyf_moodle_question_steps\
        .select_fields(['id', 'state', 'questionattemptid', 'timecreated'])

    df_moodle_question_steps = dyf_moodle_question_steps.toDF()
    df_moodle_question_steps = df_moodle_question_steps.dropDuplicates(['id'])

    # get latest question_steps state
    w2 = Window.partitionBy("questionattemptid").orderBy(f.col("timecreated").desc())
    df_moodle_question_steps = df_moodle_question_steps.withColumn("row", f.row_number().over(w2)) \
        .where(f.col('row') <= 1)

    df_moodle_question_steps.cache()
    if is_dev:
        print('df_moodle_question_steps after getting latest question_steps state')
        df_moodle_question_steps.show(2)

    df_moodle_question_steps = df_moodle_question_steps.drop('row', 'timecreated')

    df_moodle_question_steps.cache()

    # -------------------------------------------------------------------------------------------------------------------#
    # dyf_mapping_grammar_lo = glueContext.create_dynamic_frame.from_catalog(
    #     database="moodle",
    #     table_name="mapping_grammar_lo"
    # )

    dyf_mapping_grammar_lo = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "mapping_grammar_lo",
            "redshiftTmpDir": "s3://dtsodin/temp/mapping_grammar_lo/v9"
        }
    )



    dyf_mapping_grammar_lo = dyf_mapping_grammar_lo \
        .select_fields(['question', 'lo', 'lc'])\
        .rename_field('question', 'question_grammar_id')

    df_mapping_grammar_lo = dyf_mapping_grammar_lo.toDF()
    # -------------------------------------------------------------------------------------------------------------------#

    dyf_moodle_quiz_attempts = glueContext.create_dynamic_frame.from_catalog(database="moodle",
                                                                        table_name="top_quiz_attempts")

    # Chon cac truong can thiet
    if is_dev:
        print('dyf_moodle_quiz_attempts::original')
        dyf_moodle_quiz_attempts.printSchema()

    # try:
    #     df_flag = spark.read.parquet("s3a://toxd-olap/transaction_log/flag/sb_native_test/sb_native_test.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     dyf_moodle_quiz_attempts = Filter.apply(frame=dyf_moodle_quiz_attempts,
    #                                            f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')

    dyf_moodle_quiz_attempts = dyf_moodle_quiz_attempts.select_fields(
        ['id', '_key', 'quiz', 'userid', 'sumgrades', 'uniqueid', 'timestart']) \
        .rename_field('id', 'attempt_id')\
        .rename_field('timestart', 'testing_time')




    # -------------------------------------------------------------------------------------------------------------------#
    if is_dev:
        print('df_moodle_question_attempts')
        df_moodle_question_attempts.printSchema()

        print('df_moodle_question_steps')
        df_moodle_question_steps.printSchema()

        print('df_top_quiz')
        df_top_quiz.printSchema()

        print('dyf_moodle_quiz_attempts')
        dyf_moodle_quiz_attempts.printSchema()

        print('df_moodle_top_user')
        df_moodle_top_user.printSchema()

        print('df_student_contact')
        df_student_contact.printSchema()

        # print ('df_student_package_status')
        # df_student_package_status.printSchema()


    #-------------------------------------------------------------------------------------------------------------------#
    dyf_moodle_quiz_attempts = Filter.apply(frame=dyf_moodle_quiz_attempts, f=lambda x: x['userid'] is not None
                                                                and x['quiz'] is not None
                                                                and x['testing_time'] > START_LOAD_DATE
                                            )

    if is_just_monthly_exam:
        dyf_moodle_quiz_attempts = Filter.apply(frame=dyf_moodle_quiz_attempts,
                                                f=lambda x: x['userid'] is not None
                                                and x['quiz'] in [6L, 7L, 9L, 918L])
    else:
        dyf_moodle_quiz_attempts = Filter.apply(frame=dyf_moodle_quiz_attempts,
                                                f=lambda x: x['userid'] is not None)

    df_moodle_quiz_attempts = dyf_moodle_quiz_attempts.toDF()
    df_moodle_quiz_attempts = df_moodle_quiz_attempts.dropDuplicates(['attempt_id'])
    df_moodle_quiz_attempts.cache()

    moodle_quiz_attempts_number = df_moodle_quiz_attempts.count()

    if is_dev:
        print ('moodle_quiz_attempts_number: ', moodle_quiz_attempts_number)

    if moodle_quiz_attempts_number < 1:
        return

    df_student_level = get_df_student_level(glueContext)
    df_student_level.cache()
    df_student_package = get_df_student_package(glueContext)
    df_student_package.cache()
    df_student_advisor = get_df_student_advisor(glueContext)
    df_student_advisor.cache()

    # Step 1: get user info, package_code, level, status
    df_quiz_student = df_moodle_quiz_attempts\
        .join(df_moodle_top_user, df_moodle_quiz_attempts.userid == df_moodle_top_user.id)\
        .join(df_student_contact, df_moodle_top_user.username == df_student_contact.user_name)

    package_endtime_unavailable = 99999999999L
    package_starttime_unavailable = 0L
    package_code_unavailable = 'UNAVAILABLE'
    student_level_code_unavailable = 'UNAVAILABLE'
    package_status_code_unavailable = 'UNAVAILABLE'

    df_quiz_student_original = df_quiz_student.select(

        'id',
        'uniqueid',
        'quiz',
        'levelstudy',
        'attempt_id',

        df_quiz_student.testing_time.alias('student_behavior_date'),
        getBehaviorIdByQuiz(df_quiz_student.quiz).alias('behavior_id'),
        df_quiz_student.student_id.cast('long').alias('student_id'),
        'contact_id',

        # f.lit(package_code_unavailable).alias('package_code'),
        # f.lit(package_endtime_unavailable).alias('package_endtime'),
        # f.lit(package_starttime_unavailable).alias('package_starttime'),

        # f.lit(student_level_code_unavailable).alias('student_level_code'),
        # f.lit(package_status_code_unavailable).alias('package_status_code'),

        f.lit(today_second).alias('transformed_at'),

        f.from_unixtime('testing_time', format="yyyyMM").alias('year_month_id')
    )
    df_quiz_student_original =df_quiz_student_original\
        .join(df_student_advisor,
          (df_quiz_student_original.contact_id == df_student_advisor.contact_id_advisor)
          & (df_quiz_student_original.student_behavior_date >= df_student_advisor.start_date)
          & (df_quiz_student_original.student_behavior_date < df_student_advisor.end_date),
          'left'
          ) \
        .join(df_student_package,
              (df_quiz_student_original.contact_id == df_student_package.contact_id_package)
              & (df_quiz_student_original.student_behavior_date >= df_student_package.package_start_time)
              & (df_quiz_student_original.student_behavior_date < df_student_package.package_end_time),
              'left'
              ) \
        .join(df_student_level,
              (df_quiz_student_original.contact_id == df_student_level.contact_id_level)
              & (df_quiz_student_original.student_behavior_date >= df_student_level.start_date)
              & (df_quiz_student_original.student_behavior_date < df_student_level.end_date),
              'left'
              )

    df_quiz_student_original = df_quiz_student_original \
        .withColumn('student_behavior_id',
                 f.md5(concaText(
                     df_quiz_student_original.student_behavior_date,
                     df_quiz_student_original.behavior_id,
                     df_quiz_student_original.student_id,
                     df_quiz_student_original.contact_id,
                     df_quiz_student_original.package_code,
                     df_quiz_student_original.package_status_code,
                     df_quiz_student_original.student_level_code,
                     df_quiz_student_original.transformed_at)))

    df_quiz_student_original.persist(StorageLevel.DISK_ONLY_2)

    # | -- id: long(nullable=true)
    # | -- uniqueid: long(nullable=true)
    # | -- quiz: long(nullable=true)
    # | -- levelstudy: string(nullable=true)
    # | -- attempt_id: long(nullable=true)
    # | -- student_behavior_date: long(nullable=true)
    # | -- behavior_id: long(nullable=true)
    # | -- student_id: long(nullable=true)
    # | -- contact_id: string(nullable=true)
    # | -- package_code: string(nullable=false)
    # | -- package_endtime: long(nullable=false)
    # | -- package_starttime: long(nullable=false)
    # | -- student_level_code: string(nullable=false)
    # | -- package_status_code: string(nullable=false)
    # | -- transformed_at: long(nullable=false)
    # | -- student_behavior_id: string(nullable=true)

    if is_dev:
        print('df_quiz_student_original')
        df_quiz_student_original.printSchema()
        df_quiz_student_original.show(1)


    # get data for getting testing detail
    df_quiz_student = df_quiz_student_original

    #1. save weekly native test for AI (speeking)

    # Step 2: Seperate result AI(Speaking) and question attempt
    # Step 2.1 Get result AI
    df_quiz_student_ai = df_quiz_student\
        .join(df_result_ai, df_quiz_student.attempt_id == df_result_ai.attempt_id_result_ai, 'inner')\
        .join(df_top_quiz, df_quiz_student.quiz == df_top_quiz.quiz_id, 'left')

    if is_limit_test:
        df_quiz_student_ai = df_quiz_student_ai.limit(100)

    if is_dev:
        print('df_quiz_student_ai')
        df_quiz_student_ai.printSchema()
        print('df_quiz_student_ai::after:separate:: ', df_quiz_student_ai.count())

    source_system_native_test_ai = 'NATIVE_TEST_AI'
    source_system_native_test_simple = 'NATIVE_TEST_SIMPLE'
    source_system_native_test_grammar = 'NATIVE_TEST_GRAMMAR'

    current_step_unavailable = -1L
    total_step_unavailable = -1L
    learning_category_id_unavailable = -1L
    learning_unit_code_unavailable = 'UNAVAILABLE'
    learning_object_type_code_unavailable = 'UNAVAILABLE'
    learning_object_id_unavailable = -1L
    learning_object_unavailable = 'UNAVAILABLE'
    learning_category_code_unavailable = 'UNAVAILABLE'

    student_answer_detail_unavailable = 'UNAVAILABLE'

    duration_unavailable = -1L
    max_point_unavailable = -1L
    received_point_unavailable = -2L

    test_type_unavailable = 'UNAVAILABLE'

    right_answer_unavailable = 'UNAVAILABLE'
    wrong_answer_unavailable = 'UNAVAILABLE'
    #

    #
    #
    # #------------------------------------------------------------------------------------------------------------------#
    #
    #
    #
    # #------------------------------------------------------------------------------------------------------------------#
    #
    if is_dev:
        print('df_quiz_student_ai')
        df_quiz_student_ai.printSchema()
        df_quiz_student_ai.show(1)

    # Step 2.2 Get data for result AI
    df_quiz_student_ai_full = df_quiz_student_ai.select(
        'student_behavior_id',
        'student_behavior_date',
        'behavior_id',
        'student_id',
        'contact_id',

        # 'package_code',
        # df_quiz_student_ai.end_time.cast('long').alias('package_endtime'),
        # df_quiz_student_ai.start_time.cast('long').alias('package_starttime'),
        #
        # df_quiz_student_ai.levelstudy.alias('student_level_code'),
        # df_quiz_student_ai.status_code.alias('package_status_code'),

        'package_code',
        # 'package_endtime',
        # 'package_starttime',

        'student_level_code',
        'package_status_code',


        'transformed_at',

        'attempt_id',

        #for student_test_detail
        f.lit(source_system_native_test_ai).alias('source_system'),
        df_quiz_student_ai.name.alias('test_type'),
        df_quiz_student_ai.attempt_id_result_ai.cast('long').alias('attempt_step_id'),

        f.lit(current_step_unavailable).cast('long').alias('current_step'),
        f.lit(total_step_unavailable).cast('long').alias('total_step'),

        f.lit(learning_category_id_unavailable).cast('long').alias('learning_category_id'),
        f.lit(learning_category_code_unavailable).alias('learning_category_code'),

        f.lit(learning_unit_code_unavailable).cast('string').alias('learning_unit_code'),
        f.lit(learning_object_type_code_unavailable).cast('string').alias('learning_object_type_code'),
        f.lit(learning_object_id_unavailable).cast('long').alias('learning_object_id'),
        f.lit(learning_object_unavailable).cast('string').alias('learning_object'),

        df_quiz_student_ai.answer.cast('string').alias('correct_answer'),
        df_quiz_student_ai.speech_result.cast('string').alias('student_answer'),
        f.lit(student_answer_detail_unavailable).cast('string').alias('student_answer_detail'),

        'result',

        df_quiz_student_ai.right_word.cast('string').alias('right_answer'),
        df_quiz_student_ai.wrong_word.cast('string').alias('wrong_answer'),

        f.lit(duration_unavailable).cast('long').alias('duration'),
        f.lit(max_point_unavailable).cast('long').alias('max_point'),
        f.lit(received_point_unavailable).cast('long').alias('received_point'),

        'year_month_id'
    )

    if is_dev:
        print('df_quiz_student_ai_full')
        df_quiz_student_ai_full.printSchema()


    # # Step 3.1 Get data for question_attempts
    df_quiz_student_question = df_quiz_student\
        .join(df_moodle_question_attempts, df_quiz_student.uniqueid == df_moodle_question_attempts.questionusageid, 'inner')\
        .join(df_moodle_question_steps, df_moodle_question_attempts.question_attempt_id == df_moodle_question_steps.questionattemptid, 'left')\
        .join(df_mapping_grammar_lo,
              df_moodle_question_attempts.question_attempt_id == df_mapping_grammar_lo.question_grammar_id, 'left')

    if is_limit_test:
        df_quiz_student_question = df_quiz_student_question.limit(100)

    if is_dev:
        print('df_quiz_student_question')
        df_quiz_student_question.printSchema()
        print('df_quiz_student_question: ', df_quiz_student_question.count())


    def getSourceSystemByLC(lc):
        if lc in ['G01', 'G02', 'G03', 'G04', 'G05']:
            return source_system_native_test_grammar
        return source_system_native_test_simple

    getSourceSystemByLC = f.udf(getSourceSystemByLC, StringType())

    if is_dev:
        print('df_quiz_student_question')
        df_quiz_student_question.printSchema()
        df_quiz_student_question.show(1)

    #Step 3.2 Get data for question_attempts
    df_quiz_student_question_full = df_quiz_student_question.select(
        'student_behavior_id',
        'student_behavior_date',

        'behavior_id',
        'student_id',
        'contact_id',

        # 'package_code',
        # df_quiz_student_question.end_time.cast('long').alias('package_endtime'),
        # df_quiz_student_question.start_time.cast('long').alias('package_starttime'),
        #
        # df_quiz_student_question.levelstudy.alias('student_level_code'),
        # df_quiz_student_question.status_code.alias('package_status_code'),

        'package_code',
        # 'package_endtime',
        # 'package_starttime',

        'student_level_code',
        'package_status_code',

        'transformed_at',

        'attempt_id',

        # for student_test_detail
        getSourceSystemByLC(df_quiz_student_question.lc).alias('source_system'),

        f.lit(test_type_unavailable).alias('test_type'),
        df_quiz_student_question.question_attempt_id.cast('long').alias('attempt_step_id'),

        f.lit(current_step_unavailable).cast('long').alias('current_step'),
        f.lit(total_step_unavailable).cast('long').alias('total_step'),

        f.lit(learning_category_id_unavailable).cast('long').alias('learning_category_id'),
        df_quiz_student_question.lc.cast('string').alias('learning_category_code'),

        f.lit(learning_unit_code_unavailable).cast('string').alias('learning_unit_code'),

        f.lit(learning_object_type_code_unavailable).cast('string').alias('learning_object_type_code'),
        f.lit(learning_object_id_unavailable).cast('long').alias('learning_object_id'),
        df_quiz_student_question.lo.cast('string').alias('learning_object'),

        df_quiz_student_question.rightanswer.cast('string').alias('correct_answer'),
        df_quiz_student_question.responsesummary.cast('string').alias('student_answer'),
        f.lit(student_answer_detail_unavailable).cast('string').alias('student_answer_detail'),

        df_quiz_student_question.state.alias('result'),

        f.lit(right_answer_unavailable).alias('right_answer'),
        f.lit(wrong_answer_unavailable).alias('wrong_answer'),

        f.lit(duration_unavailable).cast('long').alias('duration'),
        f.lit(max_point_unavailable).cast('long').alias('max_point'),
        df_quiz_student_question.maxmark.cast('long').alias('received_point'),

        'year_month_id'
    )

    if is_dev:
        print ('df_quiz_student_question_full')
        df_quiz_student_question_full.printSchema()
        print('df_quiz_student_ai_full::before::union')
        print('df_quiz_student_ai_full::number: ', df_quiz_student_ai_full.count())
        print('df_quiz_student_question_full::number: ', df_quiz_student_question_full.count())
    #
    df_quiz_full = df_quiz_student_ai_full.union(df_quiz_student_question_full)
    if is_dev:
        print('df_quiz_full')
        df_quiz_full.printSchema()
        df_quiz_full.show(3)
    #
    #
    #
    #
    # #save to student behavior
    #
    dyf_quiz_full = DynamicFrame.fromDF(df_quiz_full, glueContext, 'dyf_quiz_full')
    #
    # #Save to
    apply_ouput_test_detail = ApplyMapping.apply(frame=dyf_quiz_full,
                                     mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),

                                               ("attempt_id", "long", "attempt_id", "long"),
                                               ("source_system", "string", "source_system", "string"),
                                               ("test_type", "string", "test_type", "string"),
                                               ("attempt_step_id", "long", "attempt_step_id", "long"),

                                               ("current_step", "long", "current_step", "long"),
                                               ("total_step", "long", "total_step", "long"),

                                               ("learning_category_id", "long", "learning_category_id", "long"),
                                               ("learning_category_code", "string", "learning_category_code", "string"),

                                               ("learning_unit_code", "string", "learning_unit_code", "string"),

                                               ("learning_object_type_code", "string", "learning_object_type_code", "string"),
                                               ("learning_object_id", "long", "learning_object_id", "long"),
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

                                               ("student_behavior_date", "long", "created_at", "long"),

                                               ("behavior_id", "long", "behavior_id", "long"),

                                               ("year_month_id", "string", "year_month_id", "long")

                                               ])

    dfy_output_test = ResolveChoice.apply(frame=apply_ouput_test_detail, choice="make_cols", transformation_ctx="resolvechoice2")
    # # save to s3
    glueContext.write_dynamic_frame.from_options(
        frame=dfy_output_test,
        connection_type="s3",
        connection_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_test_detail",
                            "partitionKeys": ["behavior_id", "year_month_id"]},
        format="parquet")

    # save to redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output_test,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "sb_student_test_detail",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/student_behavior/sb_student_test_detail",
                                                   transformation_ctx="datasink4")



    # #-------------------------------------------------------------------------------------------------------------------#
    # # for save behavior
    df_quiz_full = df_quiz_full.dropDuplicates(['student_behavior_id'])
    dyf_quiz_student_behavior = DynamicFrame.fromDF(df_quiz_student_original, glueContext, 'dyf_quiz_student_behavior')
    apply_ouput_hehavior = ApplyMapping.apply(frame=dyf_quiz_student_behavior,
                                              mappings=[
                                                  ("student_behavior_id", "string", "student_behavior_id", "string"),
                                                  ("student_behavior_date", "long", "student_behavior_date", "long"),
                                                  ("behavior_id", "long", "behavior_id", "int"),
                                                  ("student_id", "long", "student_id", "long"),
                                                  ("contact_id", "string", "contact_id", "string"),

                                                  ("package_code", "string", "package_code", "string"),

                                                  ("student_level_code", "string", "student_level_code", "string"),
                                                  ("package_status_code", "string", "package_status_code", "string"),

                                                  ("advisor_id", "long", "advisor_id", "long"),

                                                  ("transformed_at", "long", "transformed_at", "long"),

                                                  ("year_month_id", "string", "year_month_id", "long")
                                                  ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput_hehavior, choice="make_cols",
                                     transformation_ctx="resolvechoice2")


    # save to s3
    glueContext.write_dynamic_frame.from_options(
        frame=dfy_output,
        connection_type="s3",
        connection_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior",
                            "partitionKeys": ["behavior_id", "year_month_id"]},
        format="parquet")

    # save to redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "sb_student_behavior",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/student_behavior/sb_student_behavior",
                                                   transformation_ctx="datasink4")

    # # -------------------------------------------------------------------------------------------------------------------#
    #
    # # Step 5 - get marks
    if is_dev:
        print('Check before get marks')
        print ('df_top_topica_question_marks')
        df_top_topica_question_marks.printSchema()

        print ('df_top_topica_question_marks')
        df_top_topica_question_mark_week.printSchema()

    df_quiz_mark = df_quiz_student_original.select('student_behavior_id', 'behavior_id', 'attempt_id', 'year_month_id')

    dyf_quiz_mark = DynamicFrame.fromDF(df_quiz_mark, glueContext, 'dyf_quiz_mark')

    #check du lieu df_quiz_mark
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_quiz_mark,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "dyf_quiz_mark",
                                                                   "database": "dts_odin_checking"
                                                               },
                                                               redshift_tmp_dir="s3://dts-odin/dts_odin_checking/temp/dyf_quiz_mark",
                                                               transformation_ctx="datasink4")

    df_quiz_week = df_quiz_mark.where(df_quiz_mark.behavior_id == BEHAVIOR_ID_TEST_TUAN)
    df_quiz_month = df_quiz_mark.where(df_quiz_mark.behavior_id == BEHAVIOR_ID_TEST_THANG)
    #
    # # ------------------------------------------------------------------------------------------------------------------#
    df_quiz_week_marks = df_quiz_week.join(df_top_topica_question_mark_week,
                                     df_quiz_week.attempt_id == df_top_topica_question_mark_week.attemptid_mark_week,
                                     'left'
                                     )
    #
    if is_dev:
        print('df_quiz_week_marks::after_join_df_top_topica_question_mark_week')
        df_quiz_week_marks.printSchema()
        df_quiz_week_marks.show(10)
    #
    df_quiz_week_marks = df_quiz_week_marks.na.fill({'grade_mark_week': 0})
    #
    def convertIntergerToFloat(grade_mark_week):
        if grade_mark_week is None:
            return float(0.0)
        return float(grade_mark_week)

    convertIntergerToFloat = f.udf(convertIntergerToFloat, FloatType())
    #
    #
    df_quiz_week_marks = df_quiz_week_marks.select(
        'behavior_id',
        'attempt_id',
        'student_behavior_id',
        df_quiz_week_marks.quiz_name_week.alias('question_category'),
        df_quiz_week_marks.grade_mark_week.alias('grade_t'),
        'year_month_id'
    )
    #
    if is_dev:
        print('df_quiz_week_marks')
        df_quiz_week_marks.printSchema()
        df_quiz_week_marks.show(2)
    #
    dyf_quiz_week_marks = DynamicFrame.fromDF(df_quiz_week_marks, glueContext, 'dyf_quiz_week_marks')

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_quiz_week_marks,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "dyf_quiz_week_marks",
                                                                   "database": "dts_odin_checking"
                                                               },
                                                               redshift_tmp_dir="s3://dts-odin/dts_odin_checking/temp/dyf_quiz_week_marks",
                                                               transformation_ctx="datasink4")

    #
    # #------------------------------------------------------------------------------------------------------------------#
    MARK_UNAVAILABLE = '-1'
    def getMapFromStringJson(str_value):
        if str_value is None:
            return {
                {"VOCABULARY": MARK_UNAVAILABLE,
                 "CONVERSATIONAL_EXPRESSION": MARK_UNAVAILABLE,
                 "LISTENING": MARK_UNAVAILABLE,
                 "DICTATION": MARK_UNAVAILABLE,
                 "GRAMMAR": MARK_UNAVAILABLE,
                 "READING": MARK_UNAVAILABLE
                 }
            }
        str_value = str(str_value)
        json_value = json.loads(str_value)
        return json_value
    #
    getMapFromStringJson = f.udf(getMapFromStringJson, MapType(StringType(), StringType()))
    #
    df_quiz_month_marks = df_quiz_month.join(df_top_topica_question_marks,
                                  df_quiz_month.attempt_id == df_top_topica_question_marks.attemptid_mark,
                                  'inner')

    if is_dev:
        print('df_quiz_month_marks after join question marks')
        df_quiz_month_marks.printSchema()
        df_quiz_month_marks.show(5)

    df_quiz_month_marks = df_quiz_month_marks.select(
        'behavior_id',
        'attempt_id',
        'student_behavior_id',
        getMapFromStringJson(df_quiz_month_marks.marks_month).alias('marks_month_dict'),

        'year_month_id'
    )



    #
    if is_dev:
        print('df_quiz_month_marks after join question marks::after convert marks_month_dict')
        df_quiz_month_marks.printSchema()
        df_quiz_month_marks.show(5)

    df_quiz_month_marks = df_quiz_month_marks.select(
        'behavior_id',
        'attempt_id',
        'student_behavior_id',
        f.explode(df_quiz_month_marks.marks_month_dict),

        'year_month_id'
    )
    #
    if is_dev:
        print('df_quiz_month_marks after join question marks::after explode')
        df_quiz_month_marks.printSchema()
        df_quiz_month_marks.show(5)
    #
    df_quiz_month_marks = df_quiz_month_marks.select(
        'behavior_id',
        'attempt_id',
        'student_behavior_id',
        df_quiz_month_marks.key.alias('question_category'),
        df_quiz_month_marks.value.cast('float').alias('grade_t'),

        'year_month_id'
    )
    #
    if is_dev:
        print('df_quiz_month_marks::complete')
        df_quiz_month_marks.printSchema()
        df_quiz_month_marks.show(3)

    dyf_quiz_month_marks = DynamicFrame.fromDF(df_quiz_month_marks, glueContext, 'dyf_quiz_month_marks')

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_quiz_month_marks,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "dyf_quiz_month_marks",
                                                                   "database": "dts_odin_checking"
                                                               },
                                                               redshift_tmp_dir="s3://dts-odin/dts_odin_checking/temp/dyf_quiz_month_marks",
                                                               transformation_ctx="datasink4")
    # # ------------------------------------------------------------------------------------------------------------------#
    #
    df_quiz_month_marks_full = df_quiz_week_marks.union(df_quiz_month_marks)

    df_quiz_month_marks_full = df_quiz_month_marks_full.dropDuplicates(['attempt_id', 'question_category'])

    dyf_quiz_month_marks_full = DynamicFrame.fromDF(df_quiz_month_marks_full, glueContext, 'dyf_quiz_month_marks_full')

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_quiz_month_marks_full,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "dyf_quiz_month_marks_full",
                                                                   "database": "dts_odin_checking"
                                                               },
                                                               redshift_tmp_dir="s3://dts-odin/dts_odin_checking/temp/dyf_quiz_month_marks_full",
                                                               transformation_ctx="datasink4")
    #
    QUESTION_CATEGORY__UNAVAILABLE = 'UNAVAILABLE'
    df_quiz_month_marks_full = df_quiz_month_marks_full.na.fill(
        {'question_category': QUESTION_CATEGORY__UNAVAILABLE,
         'grade_t': MARK_UNAVAILABLE}
    )

    if is_dev:
        print ('df_quiz_month_marks_full')
        df_quiz_month_marks_full.printSchema()
        df_quiz_month_marks_full.show(3)
    # #
    dyf_quiz_month_marks_full = DynamicFrame.fromDF(df_quiz_month_marks_full, glueContext, 'dyf_quiz_month_marks_full')
    # #
    apply_dyf_quiz_month_marks_full = ApplyMapping.apply(frame=dyf_quiz_month_marks_full,
                                                mappings=[("behavior_id", "long", "behavior_id", "long"),
                                                          ("attempt_id", "long", "attempt_id", "long"),
                                                          ("student_behavior_id", "string", "student_behavior_id", "string"),
                                                          ("question_category", "string", "question_category", "string"),
                                                          ("grade_t", "float", "grade", "float"),

                                                          ("year_month_id", "string", "year_month_id", "long")
                                                          ])
    #
    dyf_quiz_month_marks_full_output = ResolveChoice.apply(frame=apply_dyf_quiz_month_marks_full,
                                                           choice="make_cols", transformation_ctx="resolvechoice2")
    #
    # save to s3
    glueContext.write_dynamic_frame.from_options(
        frame=dyf_quiz_month_marks_full_output,
        connection_type="s3",
        connection_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_test_mark",
                            "partitionKeys": ["behavior_id", "year_month_id"]},
        format="parquet")


    #save to redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_quiz_month_marks_full_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "sb_student_test_mark",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/student_behavior/sb_student_test_mark",
                                                   transformation_ctx="datasink4")


    #
    # df_quiz_full.unpersist()
    # df_moodle_quiz_attempts.unpersist()
    # df_moodle_question_steps.unpersist()

    df_mdl_logsservice_in_out = dyf_moodle_quiz_attempts.toDF()
    flag = df_mdl_logsservice_in_out.agg({"_key": "max"}).collect()[0][0]
    flag_data = [flag]
    df = spark.createDataFrame(flag_data, "long").toDF('flag')
    df.write.parquet("s3a://toxd-olap/transaction_log/flag/sb_native_test/sb_native_test.parquet",
                     mode="overwrite")

    df_quiz_student_original.unpersist()

if __name__ == "__main__":
    main()