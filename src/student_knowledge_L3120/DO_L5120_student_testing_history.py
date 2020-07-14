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
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType
from datetime import date, datetime, timedelta
import pytz
import boto3

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    first_day_of_month = today.replace()
    print('today: ', today)
    yesterday = today - timedelta(1)
    print('yesterday: ', yesterday)
    today_id = long(today.strftime("%Y%m%d"))
    yesterday_id = long(yesterday.strftime("%Y%m%d"))
    today_id_0h00 = long(today.strftime("%s"))
    print('today_id: ', today_id)
    print('yesterday_id: ', yesterday_id)
    print ('today_id_0h00: ', today_id_0h00)

    date_end = 1573232400L
    General = 'General'
    Vocabulary = 'Vocabulary'
    Grammar = 'Grammar'
    Speaking = 'Speaking'
    Listening = 'Listening'
    Phrasal_Verb = 'Phrasal'
    Pronunciation = 'Pronunciation'

    # Phrasal
    # Verb

    # Speaking
    # 2
    # General
    # 3
    # Phrasal Verb
    # 4
    # Grammar
    # 5
    # Vocabulary
    # 6
    # Pronunciation
    # 7
    # Listening


    is_dev = True
    is_just_monthly_exam = False
    is_limit_test = False

    start_load_date = 0L

    BEHAVIOR_ID_TEST_TUAN = 22L
    BEHAVIOR_ID_TEST_THANG = 23L

    PERIOD_DAYLY = 1L
    PERIOD_WEEKLY = 2L
    PERIOD_MONTHLY = 3L


    def doCheckClassID(code):
        if code is None:
            return None
        code = str(code)
        if code == General:
            return 61L
        if code == Vocabulary:
            return 62L
        if code == Grammar:
            return 63L
        if code == Speaking:
            return 64L
        if code == Listening:
            return 65L
        if code == Pronunciation:
            return 66L
        if Phrasal_Verb in code:
            return 67L
        return None

    check_class_id = udf(doCheckClassID, LongType())


    # ------------------------------------------------------------------------------------------------------------------#
    my_partition_predicate = "(behavior_id=='22' or behavior_id=='23')"
    dyf_student_behavior = glueContext.create_dynamic_frame.from_catalog(
        database="od_student_behavior",
        table_name="student_behavior",
        push_down_predicate=my_partition_predicate
    )

    dyf_student_behaviors = dyf_student_behavior.resolveChoice(
        specs=[('behavior_id', 'cast:long'), ('transformed_at', 'cast:long')])

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_student_testing_history.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print('read from index: ', max_key)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_student_behaviors = Filter.apply(frame=dyf_student_behaviors, f=lambda x: x['transformed_at'] > max_key)
    # except:
    #     print('read flag error ')

    if dyf_student_behaviors.count() > 0:

        dyf_student_behaviors = Filter.apply(frame=dyf_student_behaviors,
                                            f=lambda x: x["student_behavior_id"] is not None
                                                        and x["student_id"] is not None
                                                        # and x["behavior_id"] in [BEHAVIOR_ID_TEST_TUAN,
                                                        #                          BEHAVIOR_ID_TEST_THANG
                                                        #                          ]
                                                        and start_load_date <= x["student_behavior_date"] < today_id_0h00
                                            )

        number_dyf_student_behavior = dyf_student_behaviors.count()
        print('number_dyf_student_behavior after filtering: ', number_dyf_student_behavior)
        if number_dyf_student_behavior == 0:
            return

        dyf_student_behavior = dyf_student_behaviors \
            .select_fields(['student_behavior_id',
                            'student_behavior_date',
                            'student_id',
                            'behavior_id'])

        df_student_behavior = dyf_student_behavior.toDF()
        df_student_behavior = df_student_behavior.drop_duplicates(['student_behavior_id'])
        if is_limit_test:
            df_student_behavior = df_student_behavior.limit(1000)

        df_student_behavior = df_student_behavior.repartition('behavior_id')
        df_student_behavior.cache()

        student_behavior_number = df_student_behavior.count()

        if is_dev:
            print('dy_student_behavior')
            print ('student_behavior_number: ', student_behavior_number)
            df_student_behavior.printSchema()
            df_student_behavior.show(3)

        if student_behavior_number == 0:
            return

        # ------------------------------------------------------------------------------------------------------------------#
        dyf_student_test_mark = glueContext.create_dynamic_frame.from_catalog(
            database="od_student_behavior",
            table_name="student_test_mark",
            push_down_predicate=my_partition_predicate
        )

        dyf_student_test_mark = dyf_student_test_mark.select_fields(['student_behavior_id',
                                                                   'question_category',
                                                                   'grade'
                                                                   ])

        # dyf_student_test_mark = Filter.apply(frame=dyf_student_test_mark,
        #                                     f=lambda x: x["behavior_id"] in [BEHAVIOR_ID_TEST_TUAN,
        #                                                                      BEHAVIOR_ID_TEST_THANG
        #                                                                      ]
        #                                     )

        df_student_test_mark = dyf_student_test_mark.toDF()

        number_student_test_mark = df_student_test_mark.count()

        if is_dev:
            print('df_student_test_mark')
            print ('df_student_test_mark: ', number_student_test_mark)
            df_student_test_mark.printSchema()
            df_student_test_mark.show(3)

        if number_student_test_mark == 0:
            return

        df_student_behavior_mark = df_student_behavior\
            .join(df_student_test_mark,
                    on='student_behavior_id',
                    how='left')

        if is_dev:
            print('df_student_behavior_mark')
            print ('df_student_behavior_mark: ', df_student_behavior_mark)
            df_student_behavior_mark.printSchema()
            df_student_behavior_mark.show(3)

        df_student_behavior_mark = df_student_behavior_mark.dropDuplicates(['student_behavior_id', 'student_id',
                                                                            'behavior_id', 'question_category'])

        df_student_behavior_mark_week = df_student_behavior_mark\
            .filter(df_student_behavior_mark.behavior_id == BEHAVIOR_ID_TEST_TUAN)
        df_student_behavior_mark_month = df_student_behavior_mark.filter(
            df_student_behavior_mark.behavior_id == BEHAVIOR_ID_TEST_THANG)


        df_student_behavior_mark_week = df_student_behavior_mark_week\
            .withColumn('agg_week_id',  from_unixtime(df_student_behavior_mark_week.student_behavior_date, "yyyyww"))

        df_student_behavior_mark_month = df_student_behavior_mark_month \
            .withColumn('agg_month_id',
                        from_unixtime(df_student_behavior_mark_month.student_behavior_date, "yyyyMM"))

        if is_dev:
            print('df_student_behavior_mark_week')
            df_student_behavior_mark_week.printSchema()
            df_student_behavior_mark_week.show(3)

            print('df_student_behavior_mark_month')
            df_student_behavior_mark_month.printSchema()
            df_student_behavior_mark_month.show(3)

        df_student_behavior_mark_week = df_student_behavior_mark_week \
            .withColumn("class_id", check_class_id(df_student_behavior_mark_week.question_category))

        df_student_behavior_mark_week_agg = df_student_behavior_mark_week.groupby('student_id', 'agg_week_id', 'class_id').agg(
            f.round(f.max(df_student_behavior_mark_week.grade)).cast('long').alias('grade_total'),
            f.lit(PERIOD_WEEKLY).alias('period_type_id'),
            f.lit(None).cast('string').alias('agg_date_id'),
            f.lit(None).cast('string').alias('agg_month_id')
        )


        df_student_behavior_mark_month = df_student_behavior_mark_month.na.fill({
            'grade': 0
        })

        df_student_behavior_mark_month = df_student_behavior_mark_month.groupby('student_behavior_id').agg(
            f.first('student_id').alias('student_id'),
            f.first('agg_month_id').alias('agg_month_id'),
            f.round(f.sum('grade')).cast('long').alias('grade_total_attempt'),
        )

        df_student_behavior_mark_month_agg = df_student_behavior_mark_month.groupby('student_id', 'agg_month_id').agg(
            f.max(df_student_behavior_mark_month.grade_total_attempt).alias('grade_total'),
            f.lit(PERIOD_MONTHLY).alias('period_type_id'),
            f.lit(None).cast('string').alias('agg_date_id'),
            f.lit(None).cast('string').alias('agg_week_id'),
            f.lit(68L).cast('long').alias('class_id')
        )

        df_student_behavior_mark_month_agg = df_student_behavior_mark_month_agg.select(
            'student_id',
            'agg_week_id',
            'class_id',
            'grade_total',
            'period_type_id',
            'agg_date_id',
            'agg_month_id'
        )


        if is_dev:
            print('df_student_behavior_mark_week_agg')
            df_student_behavior_mark_week_agg.printSchema()
            df_student_behavior_mark_week_agg.show(3)

            print('df_student_behavior_mark_month_agg')
            df_student_behavior_mark_month_agg.printSchema()
            df_student_behavior_mark_month_agg.show(3)

        df_student_behavior_mark_agg = df_student_behavior_mark_week_agg.union(df_student_behavior_mark_month_agg)

        if is_dev:
            print('df_student_behavior_mark_agg')
            df_student_behavior_mark_agg.printSchema()
            df_student_behavior_mark_agg.show(3)

        dyf_student_behavior_mark_agg = DynamicFrame.fromDF(df_student_behavior_mark_agg, glueContext,
                                                              'dyf_student_behavior_mark_agg')
        dyf_student_behavior_mark_agg = Filter.apply(frame=dyf_student_behavior_mark_agg,
                                                     f=lambda x: x["class_id"] is not None)
        dyf_student_behavior_mark_agg.show(3)
        apply_output_month = ApplyMapping.apply(frame=dyf_student_behavior_mark_agg,
                                                mappings=[("student_id", "long", "student_id", "long"),
                                                          ("class_id", "long", "class_id", "long"),
                                                          ("period_type_id", "long", "period_type_id", "long"),
                                                          ("agg_date_id", "string", "created_date_id", "long"),
                                                          ("agg_week_id", "string", "created_week_id", "long"),
                                                          ("agg_month_id", "string", "created_month_id", "long"),
                                                          ("grade_total", "long", "measure1", "long")])

        dfy_output_month = ResolveChoice.apply(frame=apply_output_month, choice="make_cols",
                                               transformation_ctx="resolvechoice2")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dfy_output_month,
            catalog_connection="nvn_knowledge",
            connection_options={
                "dbtable": "student_learning_history",
                "database": "nvn_knowledge_v2"
            },
            redshift_tmp_dir="s3n://dtsodin/temp/nvn_knowledge_v2/student_learning_history",
            transformation_ctx="datasink4"
        )

        df_temp = dyf_student_behaviors.toDF()
        flag = df_temp.agg({"transformed_at": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_student_testing_history.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
