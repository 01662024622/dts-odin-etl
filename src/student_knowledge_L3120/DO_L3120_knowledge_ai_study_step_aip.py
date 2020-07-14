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

    # thoi gian tu 01/10/2019
    timestamp = 1569888000

    # TBHV E ngay

    # Custom function

    # len(student_answer_deltails)
    def get_length(array_str):
        json_obj = json.loads(array_str)
        length = 0
        if json_obj is not None:
            length = len(json_obj)
        return length

    udf_get_length = udf(get_length, IntegerType())


    knowledge = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15',
              '16', '17', '18', '19', '20', '21', '22','23', '24', '25', '26', '27',
              '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39']

    comprehension = ['8', '9', '14', '17', '18', '19', '20', '21', '22', '23', '24','25', '26',
                     '27', '28', '29', '30', '31', '32', '33','34', '35', '36', '37', '38' '39']

    application = ['20', '21', '22','25', '26', '28', '29', '33', '35', '36', '37', '38' '39']

    analysis = ['25', '28',  '29', '33', '35', '36', '38']
    synthesis = ['25', '29', '35', '38']
    evaluation = ['25', '29']


    arr_aip_tu_vung = ['3','4', '5', '17', '22', '23', '24', '33']
    arr_aip_nghe = ['10', '11', '12', '13', '14', '15', '20', '21', '27', '28', '29', '30', '37', '38', '39']
    arr_aip_ngu_phap = ['6', '7', '8', '9', '18', '19', '25', '26', '34', '35', '36']
    arr_aip_ngu_am = ['16', '31', '32']

    # dung +10 sai -5
    loai_1 = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14',
              '15', '17', '18', '19', '20', '21', '22','23', '24', '25', '26',
              '27', '28', '29', '30', '33', '34', '35', '36', '37', '38', '39']
    # dung +2 sai -1
    loai_2 = ['16', '31', '32']

    lu_type = []

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
    #total_step
    #max

    def do_add_score_aip(code, received_point, max_point, type):
        score=0
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
                        if received_point == max_point :
                            score = 10
                        else:
                            score =-5
                for y in loai_2:
                    if y == code:
                        if received_point == max_point:
                            score = 2
                        else:
                            score = -1
                return score

        return None


    add_score_aip = udf(do_add_score_aip, IntegerType())


    ########## dyf_ai_study_step
    dyf_ai_study_step = glueContext.create_dynamic_frame.from_catalog(
        database="moodlestarter",
        table_name="ai_study_step"
    )
    dyf_ai_study_step = dyf_ai_study_step.select_fields(
        ['_key', 'user_id', 'tag', 'lo', 'lc', 'started_at', 'max_point', 'received_point', 'created_at'])

    dyf_ai_study_step = dyf_ai_study_step.resolveChoice(
        specs=[('_key', 'cast:long')])
    dyf_ai_study_step.printSchema()
    dyf_ai_learning_obj = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )
    dyf_ai_learning_obj.printSchema()
    dyf_ai_learning_obj = dyf_ai_learning_obj.select_fields(['learning_object_code','learning_object_id'])

    # dyf_ai_advisor = glueContext.create_dynamic_frame.from_catalog(
    #     database="do_tig_advisor",
    #     table_name="log_student_advisor"
    # )
    #
    # dyf_ai_level = glueContext.create_dynamic_frame.from_catalog(
    #     database="do_tig_advisor",
    #     table_name="log_student_level"
    # )
    # dyf_ai_package = glueContext.create_dynamic_frame.from_catalog(
    #     database="do_tig_advisor",
    #     table_name="log_student_package"
    # )

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_ai_study_step_aip.parquet")
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
        ## Xu ly tag la: aip
        # dyf_ai_study_step.show(5)

        dyf_aip = Filter.apply(frame=dyf_ai_study_step,
                                 f=lambda x: x['tag'] == 'aip')
        df_aip = dyf_aip.toDF()
        df_aip = df_aip.withColumn('timestemp',
                                   (unix_timestamp(df_aip["created_at"])))

        print df_aip.count()
        df_aip.printSchema()
        df_aip.show(5)

        dyf_aip = DynamicFrame.fromDF(df_aip, glueContext, "dyf_aip")

        dyf_aip = Filter.apply(frame=dyf_aip,
                               f=lambda x: x["timestemp"] >= timestamp)

        print dyf_aip.count()
        dyf_aip.printSchema()
        dyf_aip.show(5)

        dyf_ai_study_step.printSchema()
        dyf_aip = Filter.apply(frame=dyf_aip,
                                 f=lambda x: x["lo"] is not None
                                             and x["lc"] is not None)
        try:
            df_aip = dyf_aip.toDF()

            df_aip = df_aip.withColumn("code", df_aip['lc'])

            # df_aip.printSchema()
            # df_aip.show(2)

            # print('SCHEMA:::')
            # df_aip.printSchema()
            # df_aip.show()  code,received_point,max_point,length_answer
            # page_style, max_point, received_point, length_answer
            df_aip = df_aip.withColumn("knowledge", add_score_aip(df_aip.code, df_aip.max_point, df_aip.received_point,
                                                                  f.lit('knowledge'))) \
                .withColumn("comprehension",
                            add_score_aip(df_aip.code, df_aip.max_point, df_aip.received_point, f.lit('comprehension'))) \
                .withColumn("application",
                            add_score_aip(df_aip.code, df_aip.max_point, df_aip.received_point, f.lit('application'))) \
                .withColumn("analysis", f.lit(0)) \
                .withColumn("synthesis", f.lit(0)) \
                .withColumn("evaluation", f.lit(0)) \
                .withColumn("lu_type", add_lu_id(df_aip['code'])) \
                .withColumn("date_id", from_unixtime(unix_timestamp(df_aip["created_at"]), "yyyyMMdd"))
                # .withColumn("learning_object_", f.lit('train'))
            dyf_aip = DynamicFrame.fromDF(df_aip, glueContext, "dyf_aip")
            print "show dynamicframe dyf_aip"
            dyf_aip.show(5)
            join_dyf = Join.apply(dyf_aip, dyf_ai_learning_obj, 'lo', 'learning_object_code')
            join_dyf.printSchema()
            print "show dynamicframe join_dyf"
            join_dyf.show(5)
            # join_adv = Join.apply(join_dyf, dyf_ai_advisor, 'user_id', 'student_id').drop_fields(['student_id'])
            # join_adv = Filter.apply(frame=join_adv,
            #                        f=lambda x: x['started_at'] >= x['start_time'] and x['started_at'] <= x['end_time'] )
            # join_level = Join.apply(join_adv, dyf_ai_level, 'user_id', 'student_id')
            # join_level = Filter.apply(frame=join_level,
            #                        f=lambda x: x['started_at'] >= x['start_time'] and x['started_at'] <= x['end_time'])
            # join_package = Join.apply(join_level, dyf_ai_package, 'user_id', 'student_id')
            # join_package = Filter.apply(frame=join_package,
            #                        f=lambda x: x['started_at'] >= x['start_time'] and x['started_at'] <= x['end_time'])
            # join_package.show(2)
            # join_package.printSchema()

            # join_package=join_dyf.toDF()
            # join_package = join_package.groupby('date_id','user_id','learning_object_id','lo_type').agg(f.sum("knowledge").alias("knowledge_"),f.sum("comprehension").alias("comprehension_"),f.sum("application").alias("application_"),f.sum("analysis").alias("analysis"),f.sum("synthesis").alias("synthesis"),f.sum("evaluation").alias("evaluation"))
            # tmp_join=DynamicFrame.fromDF(join_package,glueContext,"tmp_join")
            # tmp_join.printSchema()
            # tmp_join.show(2)

            dyf_ai_history_plus = Filter.apply(frame=join_dyf, f=lambda x: x['knowledge'] > 0 )

            dyf_ai_history_minus = Filter.apply(frame=join_dyf, f=lambda x: x['knowledge'] <= 0 )

            df_ai_history_plus = dyf_ai_history_plus.toDF()
            print("EEEEEEEEEEEE")

            df_ai_history_plus.printSchema()
            df_ai_history_plus = df_ai_history_plus.groupby('date_id', 'user_id', 'learning_object_id', 'lu_type').agg(
                f.count("user_id").alias("count_plus"), f.sum("knowledge").alias("knowledge_plus"),
                f.sum("comprehension").alias("comprehension_plus"), f.sum("application").alias("application_plus"),
                f.sum("analysis").alias("analysis_plus"), f.sum("synthesis").alias("synthesis_plus"),
                f.sum("evaluation").alias("evaluation_plus"))

            dyf_ai_history_plus = DynamicFrame.fromDF(df_ai_history_plus, glueContext, "dyf_ai_history_plus")

            dyf_ai_history_plus = dyf_ai_history_plus.select_fields(
                ['date_id', 'user_id', 'learning_object_id', 'lu_type', 'knowledge_plus', 'comprehension_plus',
                 'application_plus', 'analysis_plus','synthesis_plus', 'evaluation_plus', 'count_plus'])\
                .rename_field('user_id','user_id_plus')\
                .rename_field('date_id', 'date_id_plus')\
                .rename_field('lu_type', 'lu_type_plus')\
                .rename_field('learning_object_id','learning_object_id_plus')

            df_ai_history_minus = dyf_ai_history_minus.toDF()
            df_ai_history_minus = df_ai_history_minus.groupby('date_id', 'user_id', 'learning_object_id', 'lu_type')\
                .agg(f.count("user_id").alias("count_minus"),
                     f.sum("knowledge").alias("knowledge_minus"),
                     f.sum("comprehension").alias("comprehension_minus"),
                     f.sum("application").alias("application_minus"),
                     f.sum("analysis").alias("analysis_minus"),
                     f.sum("synthesis").alias("synthesis_minus"),
                     f.sum("evaluation").alias("evaluation_minus"))

            dyf_ai_history_minus = DynamicFrame.fromDF(df_ai_history_minus, glueContext, "dyf_ai_history_minus")
            dyf_ai_history_minus = dyf_ai_history_minus.select_fields(
                ['date_id', 'user_id', 'learning_object_id', 'lu_type', 'knowledge_minus', 'comprehension_minus',
                 'application_minus', 'analysis_minus', 'synthesis_minus', 'evaluation_minus','count_minus'])\
                .rename_field('user_id', 'user_id_minus')\
                .rename_field('date_id', 'date_id_minus')\
                .rename_field('lu_type', 'lu_type_minus')\
                .rename_field('learning_object_id','learning_object_id_minus')
            print("AAAAAAAAAAAAAAAAAAAAAAAA")
            dyf_ai_history_minus.printSchema()
            dyf_ai_history_minus.show(2)
            dyf_ai_history_plus.printSchema()
            dyf_ai_history_plus.show(2)

            print ("###########################################3")
            df_ai_history_minus = dyf_ai_history_minus.toDF()
            df_ai_history_plus = dyf_ai_history_plus.toDF()
            df_join_history = df_ai_history_plus.join(df_ai_history_minus, (
                    df_ai_history_plus['user_id_plus'] == df_ai_history_minus['user_id_minus']) & (
                                                              df_ai_history_plus['date_id_plus'] == df_ai_history_minus['date_id_minus']) & (
                                                              df_ai_history_plus['learning_object_id_plus'] ==
                                                              df_ai_history_minus['learning_object_id_minus']) & (
                                                              df_ai_history_plus['lu_type_plus'] == df_ai_history_minus['lu_type_minus']), 'outer')

            df_join_history = df_join_history\
                .withColumn("lu_id", check_lu_type(df_join_history.lu_type_plus, df_join_history.lu_type_minus)) \
                .withColumn("created_date_id", check_data_null(df_join_history.date_id_plus, df_join_history.date_id_minus)) \
                .withColumn("user_id", check_data_null(df_join_history.user_id_plus, df_join_history.user_id_minus)) \
                .withColumn("source_system", f.lit("starter_aip")) \
                .withColumn("learning_object_id", check_data_null(df_join_history.learning_object_id_plus,
                                                                  df_join_history.learning_object_id_minus))
            join_history = DynamicFrame.fromDF(df_join_history, glueContext, 'join_history')
            #
            # applymapping = ApplyMapping.apply(frame=join_history,
            #                                   mappings=[("user_id", 'string', 'student_id', 'long'),
            #                                             ("learning_object_id", "string", "learning_object_id", "int"),
            #                                             ("date_id", "string", "date_id", "int"),
            #                                             ("knowledge", "int", "knowledge", "int"),
            #                                             ("comprehension", "int", "comprehension", "int"),
            #                                             ("application", "int", "application", "int"),
            #                                             ("analysis", "int", "analysis", "int"),
            #                                             ("synthesis", "int", "synthesis", "int"),
            #                                             ("evaluation", "int", "evaluation", "int"),
            #                                             ("lo_type", "string", "lo_type", "int"),
            #                                             ("source_system", "string", "source_system", "string")
            #                                             ])
            # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
            #                                     transformation_ctx="resolvechoice")
            # dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
            # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "hn_mapping_lo_student_starter",
            #                                                                "database": "dts_odin"
            #                                                            },
            #                                                            redshift_tmp_dir="s3a://dts-odin/ai_study_step/",
            #                                                            transformation_ctx="datasink5")

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
            dropnullfields1.printSchema()
            dropnullfields1.show()

            # datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "mapping_lo_student_history",
            #                                                                "database": "dts_odin"
            #                                                            },
            #                                                            redshift_tmp_dir="s3a://dts-odin/ai_study_step_history/",
            #                                                            transformation_ctx="datasink1")

            print('START WRITE TO S3-------------------------')
            datasink2 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3://dtsodin/nvn_knowledge/mapping_lo_student_history_v2/",
                                                                         "partitionKeys": ["created_date_id", "source_system"]},
                                                                     format="parquet",
                                                                     transformation_ctx="datasink2")
            print('END WRITE TO S3-------------------------')

            df_temp = dyf_ai_study_step.toDF()
            flag = df_temp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_ai_study_step_aip.parquet", mode="overwrite")
        except Exception as e:
            print "something was wrong ", e
    else:
        print("da cap nhat")


if __name__ == "__main__":
    main()
