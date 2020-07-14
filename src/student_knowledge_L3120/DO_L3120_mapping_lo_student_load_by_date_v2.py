import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType, IntegerType, LongType
from datetime import date, datetime, timedelta
import boto3

def main():

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    score_pass = 50
    date_read_data = 20191005
    print('date_read_date: ', date_read_data)

    def doAddScoreAll(plus, minus):
        if plus is None:
            plus = 0
        if minus is None:
            minus = 0
        return plus + minus

    addScoreAll = udf(doAddScoreAll, IntegerType())

    def getPreviousDate(date_input):
        current_date = datetime.strptime(str(date_input), "%Y%m%d")
        previous_date = current_date - timedelta(1)
        previous_date_id = previous_date.strftime("%Y%m%d")
        return previous_date_id

    def addMoreSore(current_value, new_value):
        if current_value >= score_pass:
            return score_pass
        if current_value is None:
            current_value = 0
        if new_value is None:
            new_value = 0
        a = current_value + new_value
        if a < 0:
            return 0
        if a >= score_pass:
            return score_pass
        return a

    addMoreSore = udf(addMoreSore, IntegerType())

    def getNewPassDate(current_pass_date, score_value_c, score_value_n):
        if current_pass_date != None:
            return current_pass_date
        if score_value_c is None:
            score_value_c = 0
        if score_value_n is None:
            score_value_n = 0
        if score_value_c + score_value_n >= score_pass:
            return date_read_data
        return None

    getNewPassDate = udf(getNewPassDate, IntegerType())


    # def getCurrentDate():
    #     return d4
    #
    # getCurrentDate = udf(getCurrentDate, IntegerType())

    def getModifyDate(modify_old, student_id_new):
        if student_id_new is not None:
            return date_read_data
        return modify_old

    getModifyDate = udf(getModifyDate, IntegerType())


    def getnewStudentId(student_id, student_id_new):
        if student_id is None:
            return student_id_new
        return student_id

    getnewStudentId = udf(getnewStudentId, LongType())


    def getnewStudentLearningObjectId(lo_id, lo_id_new):
        if lo_id is None:
            return lo_id_new
        return lo_id

    getnewStudentLearningObjectId = udf(getnewStudentLearningObjectId, LongType())

    def caculateScore(plus, minus):
        if plus is None:
            plus = 0
        if minus is None:
            minus = 0
        return plus + minus

    caculateScore = udf(caculateScore, LongType())

    def getModifiedDateId(student_id_new, learning_object_id_new, modified_date_id_current):
        if student_id_new is not None and learning_object_id_new is not None:
            return long(date_read_data)
        return long(modified_date_id_current)

    udfGetModifiedDateId = udf(getModifiedDateId, LongType())

    def getCreatedDateId(student_id_new, learning_object_id_new, created_date_id_current):
        if created_date_id_current is not None:
            return created_date_id_current
        if student_id_new is not None and learning_object_id_new is not None:
            return date_read_data
        return created_date_id_current

    getCreatedDateId = udf(getCreatedDateId, LongType())


    def getFirstLearningDate(student_id_new, learning_object_id_new, created_date_id_current):
        if created_date_id_current is not None:
            return created_date_id_current
        if student_id_new is not None and learning_object_id_new is not None:
            return date_read_data
        return created_date_id_current

    getFirstLearningDate = udf(getFirstLearningDate, LongType())

    dyf_mapping_lo_student_history = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_student_history",
        additional_options={"path": "s3://dts-odin/nvn_knowledge/mapping_lo_student_history/*/*"}
    )

    #get start read for read

    start_read = 0

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3a://dts-odin/flag/nvn_knowledge/mapping_lo_student_end_read.parquet")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read start date from: ', start_read)
    # except:
    #     print('read flag file error ')
    #     start_read = None

    dyf_mapping_lo_student_history = Filter.apply(frame=dyf_mapping_lo_student_history,
                                                  f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                              and x["learning_object_id"] is not None)
    #
    print ('dyf_mapping_lo_student_history')
    # print(dyf_mapping_lo_student_history.count())
    # dyf_mapping_lo_student_history.show(3)
    # dyf_mapping_lo_student_history.printSchema()

    df_mapping_lo_student_history_cache = dyf_mapping_lo_student_history.toDF()
    df_mapping_lo_student_history_cache.dropDuplicates(['student_id', 'learning_object_id',
                                                        'source_system', 'created_date_id'])
    df_mapping_lo_student_history_cache.cache()
    df_group_source_system = df_mapping_lo_student_history_cache.groupby('source_system').agg(
        f.max('created_date_id').alias('max_date')
    )

    max_allowing_date = df_group_source_system.agg({"max_date": "min"}).collect()[0][0]

    print('check date_read_data')
    print ('max_allowing_date: ', max_allowing_date)
    print ('date_read_data: ', date_read_data)
    if max_allowing_date <= date_read_data:
        print ('stop in here::max_allowing_date <= date_read_data')
        return

    df_mapping_lo_student_history_cache = df_mapping_lo_student_history_cache.filter(
        df_mapping_lo_student_history_cache['created_date_id'] == date_read_data)

    print('df_mapping_lo_student_history_cache')
    df_mapping_lo_student_history_cache.printSchema()
    df_mapping_lo_student_history_cache.show(3)
    print('df_mapping_lo_student_history_cache::number: ', df_mapping_lo_student_history_cache.count())
    if df_mapping_lo_student_history_cache.count() > 0:
        df_mapping_lo_student_new = df_mapping_lo_student_history_cache\
            .groupby('student_id', 'learning_object_id', ).agg(
                addScoreAll(f.sum('knowledge_plus'), f.sum('knowledge_minus')).alias('knowledge_new'),
                addScoreAll(f.sum('comprehension_plus'), f.sum('comprehension_minus')).alias('comprehension_new'),
                addScoreAll(f.sum('application_plus'), f.sum('application_minus')).alias('application_new'),
                addScoreAll(f.sum('analysis_plus'), f.sum('analysis_minus')).alias('analysis_new'),
                addScoreAll(f.sum('synthesis_plus'), f.sum('synthesis_minus')).alias('synthesis_new'),
                addScoreAll(f.sum('evaluation_plus'), f.sum('evaluation_minus')).alias('evaluation_new'))
        #
        df_mapping_lo_student_new = df_mapping_lo_student_new.withColumnRenamed('student_id', 'student_id_new')\
            .withColumnRenamed('learning_object_id', 'learning_object_id_new')

        # dyf_df_mapping_lo_student_new = DynamicFrame.fromDF(df_mapping_lo_student_new, glueContext, 'dyf_df_mapping_lo_student_new')
        #
        # print('dyf_df_mapping_lo_student_new')
        # dyf_df_mapping_lo_student_new.printSchema()
        # dyf_df_mapping_lo_student_new.show(3)
        # print('dyf_df_mapping_lo_student_new number: ', dyf_df_mapping_lo_student_new.count())

        # dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog(
        #         #     database="nvn_knowledge",
        #         #     table_name="mapping_lo_student"
        #         # )

        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "temp_v1_mapping_lo_student",
                "redshiftTmpDir": "s3n://dts-odin/temp1/thanhtv3/temp_v1_mapping_lo_student/v8"}
        )

        print('mapping_lo_student_current')
        dyf_mapping_lo_student_current.printSchema()
        dyf_mapping_lo_student_current.show(3)
        # #
        # # # Filter all
        dyf_mapping_lo_student_current = Filter.apply(frame=dyf_mapping_lo_student_current,
                                                           f=lambda x: x["student_id"] is not None and x["student_id"] != 0

                                                                       # and x["knowledge_pass_date_id"] is None
                                                                       # and x["comprehension_pass_date_id"] is None
                                                                       # and x["application_pass_date_id"] is None
                                                                       # and x["analysis_pass_date_id"] is not None
                                                                       # and x["synthesis_pass_date_id"] is None
                                                                       # and x["evaluation_pass_date_id"] is None
                                                      )
        #
        dy_mapping_lo_student_current = dyf_mapping_lo_student_current.toDF()
        # dy_mapping_lo_student_current = dy_mapping_lo_student_current.drop('user_id')
        dy_mapping_lo_student_current.cache()
        #
        #

        join_mapping = df_mapping_lo_student_new.join(dy_mapping_lo_student_current,
                                                    (dy_mapping_lo_student_current['student_id'] == df_mapping_lo_student_new['student_id_new'])
                                                    & (dy_mapping_lo_student_current['learning_object_id'] == df_mapping_lo_student_new['learning_object_id_new']),
                                                    'left')

        print ('join_new_knowledge_left')
        join_mapping.printSchema()

        #
        print('thanhtv3-------------------------------')

        join_mapping = join_mapping \
                .withColumn('knowledge_t', addMoreSore(join_mapping.knowledge, join_mapping.knowledge_new))\
                .withColumn('comprehension_t', addMoreSore(join_mapping.comprehension, join_mapping.comprehension_new))\
                .withColumn('application_t', addMoreSore(join_mapping.application, join_mapping.application_new))\
                .withColumn('analysis_t', addMoreSore(join_mapping.analysis, join_mapping.analysis_new))\
                .withColumn('synthesis_t', addMoreSore(join_mapping.synthesis, join_mapping.synthesis_new))\
                .withColumn('evaluation_t', addMoreSore(join_mapping.evaluation, join_mapping.evaluation_new)) \
                .withColumn('student_id_t', getnewStudentId(join_mapping.student_id, join_mapping.student_id_new)) \
                .withColumn('learning_object_id_t',
                            getnewStudentLearningObjectId(join_mapping.learning_object_id, join_mapping.learning_object_id_new))\
                .withColumn('knowledge_pass_date_id',
                            getNewPassDate(join_mapping.knowledge_pass_date_id, join_mapping.knowledge, join_mapping.knowledge_new))\
                .withColumn('comprehension_pass_date_id',
                            getNewPassDate(join_mapping.comprehension_pass_date_id, join_mapping.comprehension, join_mapping.comprehension_new))\
                .withColumn('application_pass_date_id',
                           getNewPassDate(join_mapping.application_pass_date_id, join_mapping.application, join_mapping.application_new))\
                .withColumn('analysis_pass_date_id',
                            getNewPassDate(join_mapping.analysis_pass_date_id, join_mapping.analysis, join_mapping.analysis_new))\
                .withColumn('synthesis_pass_date_id',
                            getNewPassDate(join_mapping.synthesis_pass_date_id, join_mapping.synthesis, join_mapping.synthesis_new))\
                .withColumn('evaluation_pass_date_id',
                            getNewPassDate(join_mapping.evaluation_pass_date_id, join_mapping.evaluation, join_mapping.evaluation_new))\
                .withColumn('modified_date_id_t',
                            udfGetModifiedDateId(join_mapping.student_id_new, join_mapping.learning_object_id_new, join_mapping.modified_date_id))\
                .withColumn('created_date_id_t',
                            getCreatedDateId(join_mapping.student_id_new, join_mapping.learning_object_id_new, join_mapping.created_date_id))\
                .withColumn('first_learning_date_id_t',
                            getFirstLearningDate(join_mapping.student_id_new, join_mapping.learning_object_id_new, join_mapping.first_learning_date_id))
        # #s
        # #
        # join_mapping = join_mapping.drop('knowledge_new', 'comprehension_new', 'synthesis_new',
        #                                                    'application_new', 'evaluation_new', 'analysis_new',
        #                                  'knowledge', 'comprehension', 'synthesis',
        #                                  'application', 'evaluation', 'analysis',
        #                                  'student_id_new', 'learning_object_id_new', 'created_date_id')
        # join_mapping.cache()



        join_mapping = join_mapping.select('id', 'user_id', 'student_id_t', 'learning_object_id_t',

                                           'knowledge_t', 'knowledge_pass_date_id',
                                           'comprehension_t', 'comprehension_pass_date_id',
                                           'application_t', 'application_pass_date_id',
                                           'analysis_t', 'analysis_pass_date_id',
                                           'synthesis_t', 'synthesis_pass_date_id',
                                           'evaluation_t', 'evaluation_pass_date_id',

                                           'modified_date_id_t',
                                           'created_date_id_t',
                                           'first_learning_date_id_t'
                                           )

        print('join_mapping')
        join_mapping.printSchema()
        join_mapping.show(1)
        dyf_join_mapping = DynamicFrame.fromDF(join_mapping, glueContext, 'dyf_join_mapping')

        dyf_join_mapping = dyf_join_mapping.resolveChoice(specs=[('user_id', 'cast:long')])


        apply_ouput = ApplyMapping.apply(frame=dyf_join_mapping,
                                       mappings=[("user_id", "long", "user_id", "long"),
                                                 ("student_id_t", "long", "student_id", "long"),
                                                 ("learning_object_id_t", "long", "learning_object_id", "long"),

                                                 ("knowledge_t", "int", "knowledge", "long"),
                                                 ("comprehension_t", "int", "comprehension","long"),
                                                 ("application_t", "int", "application","long"),
                                                 ("analysis_t", "int", "analysis", "long"),
                                                 ("synthesis_t", "int", "synthesis", "long"),
                                                 ("evaluation_t", "int", "evaluation", "long"),

                                                 ("knowledge_pass_date_id", "int", "knowledge_pass_date_id", "long"),
                                                 ("comprehension_pass_date_id", "int", "comprehension_pass_date_id", "long"),
                                                 ("application_pass_date_id", "int", "application_pass_date_id", "long"),
                                                 ("analysis_pass_date_id", "int", "analysis_pass_date_id", "long"),
                                                 ("synthesis_pass_date_id", "int", "synthesis_pass_date_id", "long"),
                                                 ("evaluation_pass_date_id", "int", "evaluation_pass_date_id", "long"),

                                                 ("modified_date_id_t", "long", "modified_date_id", "long"),
                                                 ("created_date_id_t", "long", "created_date_id", "long"),
                                                 ("first_learning_date_id_t", "long", "first_learning_date_id", "long")

                                                 ])
        #
        dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "temp_thanhtv3_dyf_join_mapping",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/nvn/knowledge/mapping_lo_student/v3",
                                                                   transformation_ctx="datasink4")
        # #
        # # #save flag for next read
        # next_day = getPreviousDate(date_read_data)
        # flag_data = [next_day]
        # df = spark.createDataFrame(flag_data, "int").toDF('flag')
        # # # ghi de _key vao s3
        # df.write.parquet("s3a://dts-odin/flag/nvn_knowledge/mapping_lo_student_end_read.parquet", mode="overwrite")
        # unpersit all cache
        df_mapping_lo_student_history_cache.unpersist()
        dy_mapping_lo_student_current.unpersist()
        # join_mapping.unpersist()


if __name__ == "__main__":
    main()
