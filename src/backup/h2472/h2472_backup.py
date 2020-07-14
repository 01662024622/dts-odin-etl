import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import concat_ws

from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
from pyspark.sql.functions import udf
from datetime import date, datetime, timedelta
import pytz
import json
import random


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session


####
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.now(ho_chi_minh_timezone)
print('today: ', today)
yesterday = today - timedelta(1)
print('yesterday: ', yesterday)
today_id = long(today.strftime("%Y%m%d"))
yesterday_id = long(yesterday.strftime("%Y%m%d"))
print('today_id: ', today_id)
print('yesterday_id: ', yesterday_id)

is_dev = True

def back_kup_h2472_assignee():
    dyf_jh2472_assignee = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="assignee")

    if is_dev:
        print ('dyf_jh2472_assignee')
        dyf_jh2472_assignee.printSchema()
        dyf_jh2472_assignee.show(3)

    dyf_jh2472_assignee = dyf_jh2472_assignee.resolveChoice(specs=[('id', 'cast:long')])

    dyf_jh2472_assignee = Filter.apply(frame=dyf_jh2472_assignee,
                                          f=lambda x: x["id"] > 38268
                                          )

    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_assignee,
                                       mappings=[("id", 'long', 'id', 'long'),
                                                 ("created_date", "string", "created_date", "timestamp"),
                                                 ("question_id", "string", "question_id", "long"),
                                                 ("user_name", "string", "user_name", "string")
                                                 ])

    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")


    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)


    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "assignee",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/assignee",
                                                               transformation_ctx="datasink5")


def back_kup_h2472_jhi_user():
    dyf_jh2472_jhi_user = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="jhi_user")

    if is_dev:
        print ('dyf_jh2472_jhi_user')
        dyf_jh2472_jhi_user.printSchema()
        dyf_jh2472_jhi_user.show(3)

    # root
    # | -- id: string
    # | -- login: string
    # | -- password_hash: string
    # | -- first_name: string
    # | -- last_name: string
    # | -- email: string
    # | -- image_url: string
    # | -- activated: boolean
    # | -- lang_key: string
    # | -- activation_key: string
    # | -- reset_key: string
    # | -- created_by: string
    # | -- created_date: string
    # | -- reset_date: string
    # | -- last_modified_by: string
    # | -- last_modified_date: string
    # | -- user_extra_id: string
    # | -- _key: long
    # | -- _table: string
    # | -- _schema: string

    dyf_jh2472_jhi_user = dyf_jh2472_jhi_user.resolveChoice(specs=[('id', 'cast:long')])

    dyf_jh2472_jhi_user = Filter.apply(frame=dyf_jh2472_jhi_user,
                                          f=lambda x: x["id"] > 4148
                                                    and x['login'] != 'anhntt15@topica.edu.vn'
                                          )

    df_jh2472_jhi_user = dyf_jh2472_jhi_user.toDF()
    df_jh2472_jhi_user = df_jh2472_jhi_user.dropDuplicates(['id'])
    dyf_jh2472_jhi_user = DynamicFrame.fromDF(df_jh2472_jhi_user, glueContext, 'dyf_jh2472_jhi_user')
    #
    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_jhi_user,
                                       mappings=[("id", 'long', 'id', 'long'),
                                                 ("login", "string", "login", "string"),
                                                 ("password_hash", "string", "password_hash", "string"),

                                                 ("first_name", "string", "first_name", "string"),
                                                 ("last_name", "string", "last_name", "string"),
                                                 ("email", "string", "email", "string"),
                                                 ("image_url", "string", "image_url", "string"),
                                                 ("activated", "boolean", "activated", "boolean"),

                                                 ("lang_key", "string", "lang_key", "string"),
                                                 ("activation_key", "string", "activation_key", "string"),
                                                 ("reset_key", "string", "reset_key", "string"),
                                                 ("created_by", "string", "created_by", "string"),
                                                 ("created_date", "string", "created_date", "timestamp"),
                                                 ("reset_date", "string", "reset_date", "timestamp"),

                                                  ("last_modified_by", "string", "last_modified_by", "string"),
                                                  ("last_modified_date", "string", "last_modified_date", "timestamp")
                                                 ])
    #
    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")


    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)
    #
    #
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "jhi_user",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/jhi_user",
                                                               transformation_ctx="datasink5")

def back_kup_h2472_question():
    dyf_jh2472_question = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="question")

    if is_dev:
        print ('dyf_jh2472_question')
        dyf_jh2472_question.printSchema()
        # dyf_jh2472_question.show(3)

    # root
    # | -- id: string
    # | -- assigned: boolean
    # | -- average_question_rating: float
    # | -- close_thread: boolean
    # | -- content: string
    # | -- created_by: string
    # | -- created_date: string
    # | -- is_private: boolean
    # | -- last_answer_time: string
    # | -- name: string
    # | -- status: string
    # | -- teacher_group_id: string
    # | -- teacher_group_name: string
    # | -- gift_id: string
    # | -- type_id: string
    # | -- on_time: boolean
    # | -- first_time_answer: string
    # | -- last_reject_time: string
    # | -- last_reject_reason: string
    # | -- assign_on_time: boolean
    # | -- new_reply: boolean
    # | -- last_receive_time: string
    # | -- _key: string
    # | -- _table: string
    # | -- _schema: string

    dyf_jh2472_question = dyf_jh2472_question.resolveChoice(specs=[('id', 'cast:long'),
                                                                   ('average_question_rating', 'cast:double')])
    #
    dyf_jh2472_question = Filter.apply(frame=dyf_jh2472_question,
                                          f=lambda x: x["id"] > 28956
                                          )
    #
    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_question,
                                       mappings=[("id", 'long', 'id', 'long'),
                                                 ("assigned", "boolean", "assigned", "boolean"),
                                                 ("average_question_rating", "double", "average_question_rating", "double"),
                                                 ("close_thread", "boolean", "close_thread", "boolean"),

                                                 ("content", 'string', 'content', 'string'),
                                                 ("created_by", "string", "created_by", "string"),
                                                 ("created_date", "string", "created_date", "timestamp"),
                                                 ("is_private", "boolean", "is_private", "boolean"),

                                                 ("last_answer_time", 'string', 'last_answer_time', 'timestamp'),
                                                 ("name", "string", "name", "string"),
                                                 ("status", "string", "status", "string"),
                                                 ("teacher_group_id", "string", "teacher_group_id", "long"),

                                                 ("teacher_group_name", 'string', 'teacher_group_name', 'string'),
                                                 ("gift_id", "string", "gift_id", "long"),
                                                 ("type_id", "string", "type_id", "long"),
                                                 ("on_time", "boolean", "on_time", "boolean"),

                                                 ("first_time_answer", 'string', 'first_time_answer', 'timestamp'),
                                                 ("last_reject_time", "string", "last_reject_time", "timestamp"),
                                                 ("last_reject_reason", "string", "last_reject_reason", "string"),
                                                 ("assign_on_time", "boolean", "assign_on_time", "boolean"),

                                                 ("new_reply", 'boolean', 'new_reply', 'boolean'),
                                                 ("last_receive_time", "string", "last_receive_time", "timestamp")
                                                 ])
    #
    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")


    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)
    #
    #
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "question",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/question",
                                                               transformation_ctx="datasink5")



def back_kup_h2472_question_type():
    dyf_jh2472_question_type = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="question_type")

    if is_dev:
        print ('dyf_jh2472_question_type')
        dyf_jh2472_question_type.printSchema()
        dyf_jh2472_question_type.show(3)

    # root
    # | -- id: string
    # | -- created_date: string
    # | -- description: string
    # | -- group_type: string

    # | -- modified_date: string
    # | -- name: string
    # | -- active: boolean
    # | -- parent_id: string
    # | -- _key: long
    # | -- _table: string
    # | -- _schema: string


    dyf_jh2472_question_type = dyf_jh2472_question_type.resolveChoice(specs=[('id', 'cast:long')])
    #
    dyf_jh2472_question_type = Filter.apply(frame=dyf_jh2472_question_type,
                                          f=lambda x: x["id"] > 54
                                          )

    df_jh2472_question_type = dyf_jh2472_question_type.toDF()
    df_jh2472_question_type = df_jh2472_question_type.dropDuplicates(['id'])
    df_jh2472_question_type = df_jh2472_question_type.withColumn('name', f.concat('name', f.lit('_'), 'id'))
    dyf_jh2472_question_type = DynamicFrame.fromDF(df_jh2472_question_type, glueContext, 'dyf_jh2472_question_type')

    # #
    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_question_type,
                                       mappings=[("id", 'long', 'id', 'long'),
                                                 ("created_date", "string", "created_date", "timestamp"),
                                                 ("description", "string", "description", "string"),
                                                 ("group_type", "string", "group_type", "string"),

                                                 ("modified_date", 'string', 'modified_date', 'timestamp'),
                                                 ("name", "string", "name", "string"),
                                                 ("active", "boolean", "active", "boolean")


                                                 ])
    # #
    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")


    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)
    # #
    # #
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "question_type",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/question_type",
                                                               transformation_ctx="datasink5")



def back_kup_h2472_question_answer():
    dyf_jh2472_question_answer = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="question_answer")

    if is_dev:
        print ('dyf_jh2472_question_answer')
        dyf_jh2472_question_answer.printSchema()
        dyf_jh2472_question_answer.show(3)

    # | -- id: string
    # | -- average_answer_rating: float
    # | -- content: string
    # | -- created_date: string

    # | -- user_answer_name: string
    # | -- question_id: string
    # | -- question_answer_id: string
    # | -- active_reply: boolean
    # | -- _key: string
    # | -- _table: string
    # | -- _schema: string

    dyf_jh2472_question_answer = dyf_jh2472_question_answer.resolveChoice(specs=[('id', 'cast:long'),
                                                                                 ('average_answer_rating', 'cast:double')])
    #
    dyf_jh2472_question_answer = Filter.apply(frame=dyf_jh2472_question_answer,
                                          f=lambda x: x["id"] > 42655
                                          )

    df_jh2472_question_answer = dyf_jh2472_question_answer.toDF()
    df_jh2472_question_answer = df_jh2472_question_answer.dropDuplicates(['id'])
    dyf_jh2472_question_answer = DynamicFrame.fromDF(df_jh2472_question_answer, glueContext, 'dyf_jh2472_question_answer')

    # #
    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_question_answer,
                                       mappings=[("id", 'string', 'id', 'long'),
                                                 ("average_answer_rating", "double", "average_answer_rating", "double"),
                                                 ("content", "string", "content", "string"),
                                                 ("created_date", "string", "created_date", "timestamp"),

                                                 ("user_answer_name", 'string', 'user_answer_name', 'string'),
                                                 ("question_id", "string", "question_id", "long"),
                                                 ("question_answer_id", "string", "question_answer_id", "long"),
                                                 ("active_reply", "boolean", "active_reply", "boolean")


                                                 ])
    # # #
    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")
    #
    #
    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)
    # #
    # #
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "question_answer",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/question_answer",
                                                               transformation_ctx="datasink5")


def back_kup_h2472_rating_answer():
    dyf_jh2472_rating_answer = glueContext \
        .create_dynamic_frame.from_catalog(database="do_h2472",
                                           table_name="rating_answer")

    if is_dev:
        print ('dyf_jh2472_rating_answer')
        dyf_jh2472_rating_answer.printSchema()
        dyf_jh2472_rating_answer.show(3)


    # return

    # root
    # | -- id: string
    # | -- rating: float
    # | -- rating_date: string
    # | -- rating_user: string
    # | -- answer_id: string
    # | -- _key: string
    # | -- _table: string
    # | -- _schema: string

    dyf_jh2472_rating_answer = dyf_jh2472_rating_answer.resolveChoice(specs=[('id', 'cast:long'), ('rating', 'cast:double')])
    #
    dyf_jh2472_rating_answer = Filter.apply(frame=dyf_jh2472_rating_answer,
                                          f=lambda x: x["id"] > 26139
                                          )

    df_jh2472_rating_answer = dyf_jh2472_rating_answer.toDF()
    df_jh2472_rating_answer = df_jh2472_rating_answer.dropDuplicates(['id'])
    dyf_jh2472_rating_answer = DynamicFrame.fromDF(df_jh2472_rating_answer, glueContext, 'dyf_jh2472_rating_answer')

    # #
    applymapping1 = ApplyMapping.apply(frame=dyf_jh2472_rating_answer,
                                       mappings=[("id", 'long', 'id', 'long'),
                                                 ("rating", "double", "rating", "double"),
                                                 ("rating_date", "string", "rating_date", "timestamp"),
                                                 ("rating_user", "string", "rating_user", "string"),

                                                 ("answer_id", 'string', 'answer_id', 'long')
                                                 ])
    # # #
    resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                         transformation_ctx="resolvechoice1")
    #
    #
    if is_dev:
        print ('resolvechoice1')
        resolvechoice1.printSchema()
        resolvechoice1.show(3)


    # #
    # #
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice1,
                                                               catalog_connection="h2474_backup",
                                                               connection_options={
                                                                   "dbtable": "rating_answer",
                                                                   "database": "topicaH2472"
                                                               },
                                                               redshift_tmp_dir="s3a://dts-odin/topicaH2472/rating_answer",
                                                               transformation_ctx="datasink5")


def main():
    print('hello')
    #ok gift

    #ok

    # back_kup_h2472_assignee()

    #ok
    # back_kup_h2472_jhi_user()

    # ok
    # back_kup_h2472_question()

    # ok
    # back_kup_h2472_question_type()

    # back_kup_h2472_question_answer()

    #okok
    # back_kup_h2472_rating_answer()

def main2():



    # back_kup_h2472_question_type()

    # back_kup_h2472_question()

    back_kup_h2472_question_answer()



    # back_kup_h2472_assignee()

    # back_kup_h2472_jhi_user()








if __name__ == "__main__":
    main2()
