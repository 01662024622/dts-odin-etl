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
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    is_python_version = 3

    # thoi gian tu 01/10/2019
    timestamp = 1569888000

    # TBHV E ngay
    knowledge = ['62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '95']
    comprehension = ['63', '65', '68', '70', '72', '75', '77', '93', '94']
    application = []
    analysis = []
    synthesis = []
    evaluation = []

    # arr_micro_tu_vung = ['62', '63', '64', '65', '66', '67', '68']
    # arr_micro_ngu_phap = ['69', '70', '71', '72', '73', '74', '75']
    # arr_micro_ngu_am = ['76', '77']
    # arr_micro_nghe = []
    # cong 10-5
    loai_1 = ['62', '63', '64', '65', '66', '68', '69', '70', '71', '72', '73', '75', '77', '93', '94', '95']
    # cong 10 -0
    loai_2 = ['67', '74', '76']

    lu_type = ['66']

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

    # LO_TYPE: 1: Tu vung; 2: Ngu am; 3: Nghe; 4: Ngu phap
    # def do_add_lo_type(code):
    #     lo_type = -1
    #     code = str(code)
    #     for x in arr_micro_tu_vung:
    #         if x == code:
    #             lo_type = 1
    #     for x in arr_micro_ngu_phap:
    #         if x == code:
    #             lo_type = 4
    #     for x in arr_micro_ngu_am:
    #         if x == code:
    #             lo_type = 2
    #     for x in arr_micro_nghe:
    #         if x == code:
    #             lo_type = 3
    #     return lo_type
    #
    # add_lo_type = udf(do_add_lo_type, IntegerType())

    # total_step
    # max

    def do_add_score_micro(code, received_point, max_point, type):
        score = 0
        code = str(code)
        arr = []
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
        for x in arr:
            if x == code:
                for y in loai_1:
                    if y == code:
                        if received_point == max_point:
                            score = 10
                        else:
                            score = -5
                for y in loai_2:
                    if y == code:
                        if received_point == max_point:
                            score = 10
                        else:
                            score = 0
                return score
        return None

    add_score_micro = udf(do_add_score_micro, IntegerType())

    ########## dyf_ai_study_step
    dyf_ai_study_step = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study_step"
    )
    dyf_ai_study_step = dyf_ai_study_step.select_fields(
        ['_key', 'user_id', 'tag', 'learning_object', 'lc', 'lo', 'started_at',
         'max_point', 'received_point', 'created_at'])

    dyf_ai_study_step = dyf_ai_study_step.resolveChoice(
        specs=[('_key', 'cast:long')])
    # dyf_ai_study_step.printSchema()
    # dyf_ai_study_step  = Filter.apply(frame=dyf_ai_study_step,
    #                            f=lambda x: x['tag'] == 'micro'
    #                                        and x['user_id'] == '2019090101')
    # print "filter by user_id "
    dyf_ai_study_step.printSchema()
    dyf_ai_study_step.show()
    dyf_ai_learning_obj = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )
    dyf_ai_learning_obj.printSchema()
    dyf_ai_learning_obj = dyf_ai_learning_obj.select_fields(['learning_object_code', 'learning_object_id'])

    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_ai_study_step_micro.parquet")
        max_key = df_flag.collect()[0]['flag']
        if is_python_version == 2:
            print('read from index: ', max_key)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_ai_study_step = Filter.apply(frame=dyf_ai_study_step, f=lambda x: x['_key'] > max_key)
    except:
        if is_python_version == 2:
            print('read flag error ')

    if is_python_version == 2:
        print('the number of new contacts: ', dyf_ai_study_step.count())
    # chon cac truong can thiet
    if (dyf_ai_study_step.count() > 0):
        ## Xu ly tag la: micro
        dyf_ai_study_step.show(5)
        dyf_micro = Filter.apply(frame=dyf_ai_study_step,
                                 f=lambda x: x['tag'] == 'micro')

        # tao column timestemp va filter > timestamp ngay 01/09
        df_micro = dyf_micro.toDF()
        df_micro = df_micro.withColumn('timestemp',
                                       (unix_timestamp(df_micro["created_at"])))

        if is_python_version == 2:
            print df_micro.count()

        df_micro.printSchema()
        df_micro.show(5)

        dyf_micro = DynamicFrame.fromDF(df_micro, glueContext, "dyf_micro")

        dyf_micro = Filter.apply(frame=dyf_micro,
                                 f=lambda x: x["timestemp"] >= timestamp)

        if is_python_version == 2:
            print dyf_micro.count()
        dyf_micro.printSchema()
        dyf_micro.show(5)

        dyf_micro = Filter.apply(frame=dyf_micro,
                                 f=lambda x: x["lo"] is not None
                                             and x["lc"] is not None)

        try:
            df_micro = dyf_micro.toDF()

            df_micro = df_micro.withColumn("code", df_micro['lc'])

            df_micro = df_micro.withColumn("knowledge",
                                           add_score_micro(df_micro.code, df_micro.max_point, df_micro.received_point,
                                                           f.lit('knowledge'))) \
                .withColumn("comprehension",
                            add_score_micro(df_micro.code, df_micro.max_point, df_micro.received_point,
                                            f.lit('comprehension'))) \
                .withColumn("application",
                            add_score_micro(df_micro.code, df_micro.max_point, df_micro.received_point,
                                            f.lit('application'))) \
                .withColumn("analysis", f.lit(0)) \
                .withColumn("synthesis", f.lit(0)) \
                .withColumn("evaluation", f.lit(0)) \
                .withColumn("lu_type", add_lu_id(df_micro.code)) \
                .withColumn("date_id", from_unixtime(unix_timestamp(df_micro["created_at"]), "yyyyMMdd")) \
                .withColumn("learning_object_", f.lit('train'))
            dyf_micro = DynamicFrame.fromDF(df_micro, glueContext, "dyf_micro")

            join_dyf = Join.apply(dyf_micro, dyf_ai_learning_obj, 'lo', 'learning_object_code')

            dyf_ai_history_plus = Filter.apply(frame=join_dyf,
                                               f=lambda x: x['knowledge'] > 0 and x['knowledge'] is not None)

            dyf_ai_history_minus = Filter.apply(frame=join_dyf,
                                                f=lambda x: x['knowledge'] <= 0 and x['knowledge'] is not None)

            df_ai_history_plus = dyf_ai_history_plus.toDF()
            df_ai_history_plus = df_ai_history_plus.groupby('date_id', 'user_id', 'learning_object_id', 'lu_type').agg(
                f.count("user_id").alias("count_plus"), f.sum("knowledge").alias("knowledge_plus"),
                f.sum("comprehension").alias("comprehension_plus"), f.sum("application").alias("application_plus"),
                f.sum("analysis").alias("analysis_plus"), f.sum("synthesis").alias("synthesis_plus"),
                f.sum("evaluation").alias("evaluation_plus"))

            dyf_ai_history_plus = DynamicFrame.fromDF(df_ai_history_plus, glueContext, "dyf_ai_history_plus")

            dyf_ai_history_plus = dyf_ai_history_plus.select_fields(
                ['date_id', 'user_id', 'learning_object_id', 'lu_type', 'knowledge_plus', 'comprehension_plus',
                 'application_plus', 'analysis_plus', 'synthesis_plus', 'evaluation_plus', 'count_plus']) \
                .rename_field('user_id', 'user_id_plus') \
                .rename_field('date_id', 'date_id_plus') \
                .rename_field('lu_type', 'lu_type_plus') \
                .rename_field('learning_object_id', 'learning_object_id_plus')

            df_ai_history_minus = dyf_ai_history_minus.toDF()
            df_ai_history_minus = df_ai_history_minus.groupby('date_id', 'user_id', 'learning_object_id',
                                                              'lu_type').agg(
                f.count("user_id").alias("count_minus"), f.sum("knowledge").alias("knowledge_minus")
                , f.sum("comprehension").alias("comprehension_minus"), f.sum("application").alias("application_minus"),
                f.sum("analysis").alias("analysis_minus"), f.sum("synthesis").alias("synthesis_minus"),
                f.sum("evaluation").alias("evaluation_minus"))
            dyf_ai_history_minus = DynamicFrame.fromDF(df_ai_history_minus, glueContext, "dyf_ai_history_plus")
            dyf_ai_history_minus = dyf_ai_history_minus.select_fields(
                ['date_id', 'user_id', 'learning_object_id', 'lu_type', 'knowledge_minus', 'comprehension_minus',
                 'application_minus', 'analysis_minus', 'synthesis_minus', 'evaluation_minus', 'count_minus']) \
                .rename_field('user_id', 'user_id_minus') \
                .rename_field('date_id', 'date_id_minus') \
                .rename_field('lu_type', 'lu_type_minus') \
                .rename_field('learning_object_id', 'learning_object_id_minus')
            if is_python_version == 2:
                print("AAAAAAAAAAAAAAAAAAAAAAAA")
            dyf_ai_history_minus.printSchema()
            dyf_ai_history_minus.show(2)
            dyf_ai_history_plus.printSchema()
            dyf_ai_history_plus.show(2)

            if is_python_version == 2:
                print ("###########################################")
            df_ai_history_minus = dyf_ai_history_minus.toDF()
            df_ai_history_plus = dyf_ai_history_plus.toDF()
            df_join_history = df_ai_history_plus.join(df_ai_history_minus, (
                    df_ai_history_plus['user_id_plus'] == df_ai_history_minus['user_id_minus'])
                                                      & (df_ai_history_plus['date_id_plus'] == df_ai_history_minus[
                'date_id_minus'])
                                                      & (df_ai_history_plus['learning_object_id_plus'] ==
                                                         df_ai_history_minus['learning_object_id_minus'])
                                                      & (df_ai_history_plus['lu_type_plus'] == df_ai_history_minus[
                'lu_type_minus']), 'outer')

            df_join_history.printSchema()
            df_join_history = df_join_history \
                .withColumn("created_date_id",
                            check_data_null(df_join_history.date_id_plus, df_join_history.date_id_minus)) \
                .withColumn("user_id", check_data_null(df_join_history.user_id_plus, df_join_history.user_id_minus)) \
                .withColumn("source_system", f.lit("starter_micro")) \
                .withColumn("learning_object_id", check_data_null(df_join_history.learning_object_id_plus,
                                                                  df_join_history.learning_object_id_minus)) \
                .withColumn("lu_id", check_lu_type(df_join_history.lu_type_plus, df_join_history.lu_type_minus))

            print "check lo_type"
            df_join_history.printSchema()
            df_join_history.show(5)

            join_history = DynamicFrame.fromDF(df_join_history, glueContext, 'join_history')
            join_history.show(10)

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
            dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields1")
            if is_python_version == 2:
                print('START WRITE TO S3-------------------------')
                print resolvechoice1.count()
            resolvechoice1.printSchema()
            # resolvechoice1.show(100)

            datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3://dtsodin/nvn_knowledge/mapping_lo_student_history/starter_micro/"},
                                                                     format="parquet",
                                                                     transformation_ctx="datasink6")
            if is_python_version == 2:
                print('END WRITE TO S3-------------------------')

            datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student_history",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3a://dtsodin/temp/ai_study_step_history/",
                                                                       transformation_ctx="datasink1")

            df_temp = dyf_ai_study_step.toDF()
            flag = df_temp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_ai_study_step_micro.parquet", mode="overwrite")
        except Exception as e:
            if is_python_version == 2:
                print "something was wrong: ", e

if __name__ == "__main__":
    main()
