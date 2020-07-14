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
from pyspark.sql.functions import udf, collect_list
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField
from datetime import date, datetime, timedelta
from pyspark.sql import Row
import pytz
import boto3

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
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

    yesterday = date.today() - timedelta(1)
    yesterday_id = long(yesterday.strftime("%Y%m%d"))
    print('yesterday_id: ', yesterday_id)

    date_start_read = 0
    date_end_read = 0

    score_pass = 50


    def doAddScoreAll(plus, minus):
        if plus is None:
            plus = 0
        if minus is None:
            minus = 0
        return plus + minus

    addScoreAll = udf(doAddScoreAll, IntegerType())

    ScoreDate = StructType([
        StructField("value", IntegerType(), False),
        StructField("pass_date", IntegerType(), False)])

    def aggregateScore(pass_date_c, current_value, history_learning_pairs):
        value = 0
        pass_date = None
        if current_value is None:
            current_value = 0
        if current_value >= score_pass:
            value = current_value
            pass_date = pass_date_c
        elif history_learning_pairs is None:
            value = 0
            pass_date = pass_date_c
        else:
            for pass_date_pair in history_learning_pairs:
                print ('pass_date_pair')
                print (pass_date_pair)
                append_value = pass_date_pair[1]
                if append_value is None:
                    append_value = 0
                current_value = current_value + append_value
                if current_value >= score_pass:
                    current_value = score_pass
                    pass_date = pass_date_pair[0]
                    break
                if current_value < 0:
                    current_value = 0
            value = current_value

        return Row('value', 'pass_date')(value, pass_date)

    aggregateScore = udf(aggregateScore, ScoreDate)

    def getCreatedDateId(student_id_new, learning_object_id_new, created_date_id_current, min_history_date):
        if created_date_id_current is not None:
            return created_date_id_current
        if student_id_new is not None and learning_object_id_new is not None:
            return min_history_date
        return created_date_id_current

    getCreatedDateId = udf(getCreatedDateId, LongType())

    def getModifiedDateId(student_id_new, learning_object_id_new, modified_date_id_current, max_history_date):
        if student_id_new is not None and learning_object_id_new is not None:
            return max_history_date
        return modified_date_id_current

    getModifiedDateId = udf(getModifiedDateId, LongType())

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

    dyf_mapping_lo_student_history = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_student_history",
        additional_options={"path": "s3://dtsodin/nvn_knowledge/mapping_lo_student_history/*/*"}
    )

    print("count mapping_lo_student_history after loading: ", dyf_mapping_lo_student_history.count())

    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dts-odin/flag/nvn_knowledge/mapping_lo_student_end_read.parquet")
        date_start_read = df_flag.collect()[0]['flag']
    except:
        print('read flag file error ')
        date_start_read = 0

    dyf_mapping_lo_student_history = Filter.apply(frame=dyf_mapping_lo_student_history,
                                                  f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                              and x["learning_object_id"] is not None
                                                              and x["created_date_id"] is not None)
    #
    print ('dyf_mapping_lo_student_history')
    print(dyf_mapping_lo_student_history.count())
    dyf_mapping_lo_student_history.show(3)
    # dyf_mapping_lo_student_history.printSchema()

    df_mapping_lo_student_history_cache = dyf_mapping_lo_student_history.toDF()
    df_mapping_lo_student_history_cache.dropDuplicates(['student_id', 'learning_object_id',
                                                        'source_system', 'created_date_id'])
    df_mapping_lo_student_history_cache.cache()
    df_group_source_system = df_mapping_lo_student_history_cache.groupby('source_system').agg(
        f.max('created_date_id').alias('max_date')
    )

    # print('df_group_source_system')
    # df_group_source_system.show()

    date_end_read = df_group_source_system.agg({"max_date": "min"}).collect()[0][0]
    print('date_start_read: ', date_start_read)
    print('date_end_read: ', date_end_read)

    print('df_mapping_lo_student_history_cache---------------------')
    df_mapping_lo_student_history_cache.printSchema()
    df_mapping_lo_student_history_cache.show(3)

    if date_end_read >= yesterday_id:
        date_end_read = today_id

    df_mapping_lo_student_history_cache = df_mapping_lo_student_history_cache.filter(
        (df_mapping_lo_student_history_cache['created_date_id'] >= date_start_read)
     & (df_mapping_lo_student_history_cache['created_date_id'] < date_end_read))
    print('df_mapping_lo_student_history_cache::after::filter: ', df_mapping_lo_student_history_cache.count())


    #load dyf_mapping_lo_student_current
    # push_down_predicate = "(backup_date == " + str(yesterday_id) + ")"
    # dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog(
    #     database="nvn_knowledge",
    #     table_name="backup_mapping_lo_student",
    #     push_down_predicate=push_down_predicate
    # )

    dyf_mapping_lo_student_current = None
    try:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog(
            database="nvn_knowledge",
            table_name="mapping_lo_student"
        )
    except:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "mapping_lo_student",
                "redshiftTmpDir": "s3://dts-odin/temp1/dyf_mapping_lo_student_current/v9"}
        )

    print('mapping_lo_student_current')
    dyf_mapping_lo_student_current.printSchema()
    dyf_mapping_lo_student_current.show(3)

    dy_mapping_lo_student_current = dyf_mapping_lo_student_current.toDF()
    dy_mapping_lo_student_current.cache()
    # df_bk_mapping_lo_student_current = dyf_mapping_lo_student_current

    if df_mapping_lo_student_history_cache.count() > 0:
        print('dont run in here-----------------------')
        # print('df_mapping_lo_student_history_cache')
        # df_mapping_lo_student_history_cache.printSchema()
        # df_mapping_lo_student_history_cache.show(3)
        # print('df_mapping_lo_student_history_cache::number: ', df_mapping_lo_student_history_cache.count())

        df_mapping_lo_student_history_cache = df_mapping_lo_student_history_cache\
            .na.fill({'knowledge_plus': 0, 'knowledge_minus': 0,
                      'comprehension_plus': 0, 'comprehension_minus': 0,
                      'application_plus': 0, 'application_minus': 0,
                      'analysis_plus': 0, 'analysis_minus': 0,
                      'synthesis_plus': 0, 'synthesis_minus': 0,
                      'evaluation_plus': 0, 'evaluation_minus': 0})

        df_mapping_lo_student_new = df_mapping_lo_student_history_cache.groupby('student_id', 'learning_object_id', 'created_date_id').agg(
            addScoreAll(f.sum('knowledge_plus'), f.sum('knowledge_minus')).alias('knowledge_new'),
            addScoreAll(f.sum('comprehension_plus'), f.sum('comprehension_minus')).alias('comprehension_new'),
            addScoreAll(f.sum('application_plus'), f.sum('application_minus')).alias('application_new'),
            addScoreAll(f.sum('analysis_plus'), f.sum('analysis_minus')).alias('analysis_new'),
            addScoreAll(f.sum('synthesis_plus'), f.sum('synthesis_minus')).alias('synthesis_new'),
            addScoreAll(f.sum('evaluation_plus'), f.sum('evaluation_minus')).alias('evaluation_new'))\
            .sort("student_id", "learning_object_id", "created_date_id")

        #group theo ngay

        df_mapping_lo_student_new = df_mapping_lo_student_new.filter(
            (df_mapping_lo_student_new.knowledge_new != 0)
            | (df_mapping_lo_student_new.comprehension_new != 0)
            | (df_mapping_lo_student_new.application_new != 0)
            | (df_mapping_lo_student_new.analysis_new != 0)
            | (df_mapping_lo_student_new.synthesis_new != 0)
            | (df_mapping_lo_student_new.evaluation_new != 0)
        )

        # df_mapping_lo_student_new = Filter.apply(frame=df_mapping_lo_student_new,
        #                                             f=lambda x: x["knowledge_new"] != 0
        #                                                         or x["comprehension_new"] != 0
        #                                                         or x["application_new"] != 0
        #                                                         or x["analysis_new"] != 0
        #                                                         or x["synthesis_new"] != 0
        #                                                         or x["evaluation_new"] != 0)

        df_mapping_lo_student_struct = df_mapping_lo_student_new.select('student_id', 'learning_object_id',
                                                                        'created_date_id',
                                                                        f.struct('created_date_id', 'knowledge_new').alias(
                                                                            'date_knowledge_new'),
                                                                        f.struct('created_date_id', 'comprehension_new').alias(
                                                                            'date_comprehension_new'),
                                                                        f.struct('created_date_id', 'application_new').alias(
                                                                            'date_application_new'),
                                                                        f.struct('created_date_id', 'analysis_new').alias(
                                                                            'date_analysis_new'),
                                                                        f.struct('created_date_id', 'synthesis_new').alias(
                                                                            'date_synthesis_new'),
                                                                        f.struct('created_date_id', 'evaluation_new').alias(
                                                                            'date_evaluation_new'))




        print ('df_mapping_lo_student_struct')
        df_mapping_lo_student_struct.printSchema()
        df_mapping_lo_student_struct.show(3)

        #group by student and learning_ob
        df_group_student_lo = df_mapping_lo_student_struct.groupby('student_id', 'learning_object_id').agg(
            collect_list("date_knowledge_new").alias('d_knowledge_new_l'),
            collect_list("date_comprehension_new").alias('d_comprehension_new_l'),
            collect_list("date_application_new").alias('d_application_new_l'),
            collect_list("date_analysis_new").alias('d_analysis_new_l'),
            collect_list("date_synthesis_new").alias('d_synthesis_new_l'),
            collect_list("date_evaluation_new").alias('d_evaluation_new_l'),
            f.min('created_date_id').alias('min_created_date_id'),
            f.max('created_date_id').alias('max_created_date_id'),
        )

        df_group_student_lo = df_group_student_lo.withColumnRenamed('student_id', 'student_id_new')\
            .withColumnRenamed('learning_object_id', 'learning_object_id_new')

        print ('df_group_student_lo')
        df_group_student_lo.printSchema()
        df_group_student_lo.show(3)

        join_mapping = dy_mapping_lo_student_current.join(df_group_student_lo,
                                                  (dy_mapping_lo_student_current['student_id'] == df_group_student_lo['student_id_new'])
                                                    & (dy_mapping_lo_student_current['learning_object_id'] == df_group_student_lo['learning_object_id_new']),
                                                    'outer')

        print ('join_mapping')
        join_mapping.printSchema()
        join_mapping.show(3)

        ### tinh ngay qua
        join_mapping = join_mapping \
            .withColumn('student_id_t', getnewStudentId(join_mapping.student_id, join_mapping.student_id_new)) \
            .withColumn('learning_object_id_t',
                        getnewStudentLearningObjectId(join_mapping.learning_object_id, join_mapping.learning_object_id_new))\
            .withColumn('knowledge_vs', aggregateScore(join_mapping.knowledge_pass_date_id, join_mapping.knowledge,
                                                      join_mapping.d_knowledge_new_l))\
            .withColumn('comprehension_vs', aggregateScore(join_mapping.comprehension_pass_date_id, join_mapping.comprehension,
                                                       join_mapping.d_comprehension_new_l))\
            .withColumn('application_vs', aggregateScore(join_mapping.application_pass_date_id, join_mapping.application,
                                                       join_mapping.d_application_new_l))\
            .withColumn('analysis_vs', aggregateScore(join_mapping.analysis_pass_date_id, join_mapping.analysis,
                                                       join_mapping.d_analysis_new_l))\
            .withColumn('synthesis_vs', aggregateScore(join_mapping.synthesis_pass_date_id, join_mapping.synthesis,
                                                       join_mapping.d_synthesis_new_l))\
            .withColumn('evaluation_vs', aggregateScore(join_mapping.evaluation_pass_date_id, join_mapping.evaluation,
                                                       join_mapping.d_evaluation_new_l))\
            .withColumn('created_date_id', getCreatedDateId(join_mapping.student_id_new, join_mapping.learning_object_id_new,
                                                            join_mapping.created_date_id, join_mapping.min_created_date_id))\
            .withColumn('modified_date_id', getModifiedDateId(join_mapping.student_id_new, join_mapping.learning_object_id_new,
                                                            join_mapping.modified_date_id, join_mapping.max_created_date_id))



        join_mapping = join_mapping.select('student_id_t', 'learning_object_id_t', 'user_id',
                                           'modified_date_id',
                                           'created_date_id',
                                           f.col("knowledge_vs").getItem("value").alias("knowledge_t"),
                                           f.col("knowledge_vs").getItem("pass_date").alias("knowledge_pass_date_id_t"),

                                           f.col("comprehension_vs").getItem("value").alias("comprehension_t"),
                                           f.col("comprehension_vs").getItem("pass_date").alias("comprehension_pass_date_id_t"),

                                           f.col("application_vs").getItem("value").alias("application_t"),
                                           f.col("application_vs").getItem("pass_date").alias("application_pass_date_id_t"),

                                           f.col("analysis_vs").getItem("value").alias("analysis_t"),
                                           f.col("analysis_vs").getItem("pass_date").alias("analysis_pass_date_id_t"),

                                           f.col("synthesis_vs").getItem("value").alias("synthesis_t"),
                                           f.col("synthesis_vs").getItem("pass_date").alias("synthesis_pass_date_id_t"),

                                           f.col("evaluation_vs").getItem("value").alias("evaluation_t"),
                                           f.col("evaluation_vs").getItem("pass_date").alias("evaluation_pass_date_id_t"))

        print('join_mapping::after caculating new score')
        join_mapping.printSchema()
        join_mapping.show(3)

        dyf_join_join_mapping_total = DynamicFrame.fromDF(join_mapping, glueContext, 'dyf_join_join_mapping_old_total')

        apply_ouput = ApplyMapping.apply(frame=dyf_join_join_mapping_total,
                                       mappings=[("user_id", "long", "user_id", "long"),
                                                 ("student_id_t", "long", "student_id", "long"),
                                                 ("learning_object_id_t", "long", "learning_object_id", "long"),

                                                 ("knowledge_t", "int", "knowledge", "long"),
                                                 ("comprehension_t", "int", "comprehension","long"),
                                                 ("application_t", "int", "application","long"),
                                                 ("analysis_t", "int", "analysis", "long"),
                                                 ("synthesis_t", "int", "synthesis", "long"),
                                                 ("evaluation_t", "int", "evaluation", "long"),

                                                 ("knowledge_pass_date_id_t", "int", "knowledge_pass_date_id", "long"),
                                                 ("comprehension_pass_date_id_t", "int", "comprehension_pass_date_id", "long"),
                                                 ("application_pass_date_id_t", "int", "application_pass_date_id", "long"),
                                                 ("analysis_pass_date_id_t", "int", "analysis_pass_date_id", "long"),
                                                 ("synthesis_pass_date_id_t", "int", "synthesis_pass_date_id", "long"),
                                                 ("evaluation_pass_date_id_t", "int", "evaluation_pass_date_id", "long"),

                                                 ("modified_date_id", "long", "modified_date_id", "long"),
                                                 ("created_date_id", "long", "created_date_id", "long")

                                                 ])

        dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "preactions": """TRUNCATE TABLE mapping_lo_student""",
                                                                       "dbtable": "mapping_lo_student",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/mapping_lo_student/v4",
                                                                   transformation_ctx="datasink4")


        #Lu vao S3
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dts-odin')
        bucket.objects.filter(Prefix="nvn_knowledge/mapping_lo_student/").delete()
        #

        # # #save flag for next read
        flag_data = [date_end_read]
        df = spark.createDataFrame(flag_data, "int").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/nvn_knowledge/mapping_lo_student_end_read.parquet", mode="overwrite")
        # unpersit all cache
        df_mapping_lo_student_history_cache.unpersist()
        dy_mapping_lo_student_current.unpersist()
        # # join_mapping.unpersist()

        df_bk_mapping_lo_student_current = dfy_output
        print('Save to S3')
        #Overide to s3
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dts-odin')
        bucket.objects.filter(Prefix="nvn_knowledge/mapping_lo_student/").delete()

        s3 = boto3.client('s3')
        bucket_name = "dts-odin"
        directory_name = "nvn_knowledge/mapping_lo_student/"
        s3.put_object(Bucket=bucket_name, Key=directory_name)

        datasink2 = glueContext.write_dynamic_frame.from_options(frame=df_bk_mapping_lo_student_current, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dtsodin/nvn_knowledge/mapping_lo_student/"},
                                                                 format="parquet",
                                                                 transformation_ctx="datasink2")

        print('Save to S3  completed')


if __name__ == "__main__":
    main()
