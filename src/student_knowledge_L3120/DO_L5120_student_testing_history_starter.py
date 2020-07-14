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

    is_dev = False
    is_just_monthly_exam = False
    is_limit_test = False

    start_load_date = 0L

    BEHAVIOR_ID_TEST_NGAY = 20L
    BEHAVIOR_ID_TEST_TUAN = 21L

    PERIOD_DAYLY = 1L
    PERIOD_WEEKLY = 2L
    PERIOD_MONTHLY = 3L

    is_load_full = False

    # ------------------------------------------------------------------------------------------------------------------#
    my_partition_predicate = "(behavior_id=='20' or behavior_id=='21')"
    dyf_student_behavior = glueContext.create_dynamic_frame.from_catalog(
        database="od_student_behavior",
        table_name="student_behavior",
        push_down_predicate=my_partition_predicate
    )

    dyf_student_behavior.printSchema()
    dyf_student_behaviors = dyf_student_behavior.resolveChoice(
        specs=[('behavior_id', 'cast:long'), ('transformed_at', 'cast:long')])

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_student_testing_history_starter.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print('read from index: ', max_key)
    #
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     dyf_student_behaviors  = Filter.apply(frame=dyf_student_behaviors , f=lambda x: x['transformed_at'] > max_key)
    # except:
    #     print('read flag error ')

    if dyf_student_behaviors.count() > 0:
        ## Filter student_behavior test daily or week by AIP
        dyf_student_behaviors = Filter.apply(frame=dyf_student_behaviors,
                                            f=lambda x: x["student_behavior_id"] is not None
                                                        and x["student_id"] is not None
                                                        and x["behavior_id"] in [ BEHAVIOR_ID_TEST_NGAY,
                                                                                   BEHAVIOR_ID_TEST_TUAN]
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

        dyf_student_test_detail = glueContext.create_dynamic_frame.from_catalog(
            database="od_student_behavior",
            table_name="student_test_detail",
            push_down_predicate = my_partition_predicate
        )

        dyf_student_test_detail = dyf_student_test_detail\
            .select_fields(['student_behavior_id', 'test_type', 'received_point'])

        dyf_student_test_detail.printSchema()
        print "dyf_student_test_detail before filter: ", dyf_student_test_detail.count()

        dyf_student_test_detail = Filter.apply(frame=dyf_student_test_detail,
                                            f=lambda x: x["test_type"] == 'aip')
        dyf_student_test_detail.printSchema()
        print "dyf_student_test_detail after filter: ", dyf_student_test_detail.count()
        # dyf_student_test_detail = Filter.apply(frame=dyf_student_test_detail,
        #                                     f=lambda x: x["behavior_id"] in [BEHAVIOR_ID_TEST_TUAN,
        #                                                                      BEHAVIOR_ID_TEST_THANG
        #                                                                      ]
        #                                     )

        df_student_test_detail = dyf_student_test_detail.toDF()

        number_student_test_detail = df_student_test_detail.count()

        if is_dev:
            print('df_student_test_detail')
            print ('df_student_test_detail: ', number_student_test_detail)
            df_student_test_detail.printSchema()
            df_student_test_detail.show(3)

        if number_student_test_detail == 0:
            return

        df_student_behavior_mark = df_student_behavior\
            .join(df_student_test_detail,
                    on='student_behavior_id',
                    how='left')

        if is_dev:
            print('df_student_behavior_mark')
            print ('df_student_behavior_mark: ', df_student_behavior_mark.count())
            df_student_behavior_mark.printSchema()
            df_student_behavior_mark.show(3)

        df_student_behavior_mark = df_student_behavior_mark.dropDuplicates(['student_behavior_id', 'student_id',
                                                                            'behavior_id', 'received_point'])

        df_student_behavior_mark_daily = df_student_behavior_mark\
            .filter(df_student_behavior_mark.behavior_id == BEHAVIOR_ID_TEST_NGAY)
        df_student_behavior_mark_week = df_student_behavior_mark.filter(
            df_student_behavior_mark.behavior_id == BEHAVIOR_ID_TEST_TUAN)


        df_student_behavior_mark_daily = df_student_behavior_mark_daily\
            .withColumn('agg_date_id',  from_unixtime(df_student_behavior_mark_daily.student_behavior_date, "yyyyMMdd"))

        df_student_behavior_mark_week = df_student_behavior_mark_week \
            .withColumn('agg_week_id_temp',
                        from_unixtime(df_student_behavior_mark_week.student_behavior_date, "yyyyww"))

        if is_dev:
            print('df_student_behavior_mark_week')
            df_student_behavior_mark_daily.printSchema()
            df_student_behavior_mark_daily.show(3)

            print('df_student_behavior_mark_week')
            df_student_behavior_mark_week.printSchema()
            df_student_behavior_mark_week.show(3)

        df_student_behavior_mark_daily_agg = df_student_behavior_mark_daily.groupby('student_id', 'agg_date_id').agg(
            f.sum(df_student_behavior_mark_daily.received_point).cast('long').alias('grade_total'),
            f.lit(PERIOD_DAYLY).alias('period_type_id'),
            f.lit(None).cast('string').alias('agg_week_id'),
            f.lit(None).cast('string').alias('agg_month_id'),
            f.lit(59).cast('long').alias('class_id')
        )

        df_student_behavior_mark_week_agg = df_student_behavior_mark_week.groupby('student_id', 'agg_week_id_temp').agg(
            f.sum(df_student_behavior_mark_week.received_point).alias('grade_total'),
            f.lit(PERIOD_WEEKLY).alias('period_type_id'),
            f.lit(None).cast('string').alias('agg_date_id_temp'),
            f.lit(None).cast('string').alias('agg_month_id'),
            f.lit(60).cast('long').alias('class_id')
        )

        df_student_behavior_mark_week_agg = df_student_behavior_mark_week_agg\
            .withColumn("agg_date_id_temp", df_student_behavior_mark_week_agg.agg_week_id_temp) \
            .withColumn("agg_week_id_temp", f.lit(None).cast("string") )
        df_student_behavior_mark_week_agg = df_student_behavior_mark_week_agg\
            .withColumnRenamed("agg_date_id_temp", "agg_week_id")\
            .withColumnRenamed("agg_week_id_temp", "agg_date_id")
        df_student_behavior_mark_week_agg.show(5)

        if is_dev:
            print('df_student_behavior_mark_daily')
            df_student_behavior_mark_daily_agg.printSchema()
            df_student_behavior_mark_daily_agg.show(3)

            print('df_student_behavior_mark_week_agg')
            df_student_behavior_mark_week_agg.printSchema()
            df_student_behavior_mark_week_agg.show(3)

        df_student_behavior_mark_agg = df_student_behavior_mark_week_agg.union(df_student_behavior_mark_daily_agg)

        print "df_student_behavior_mark_agg", df_student_behavior_mark_agg.count()

        if is_dev:
            print('df_student_behavior_mark_agg')
            df_student_behavior_mark_agg.printSchema()
            df_student_behavior_mark_agg.show(3)

        dyf_student_behavior_mark_agg = DynamicFrame.fromDF(df_student_behavior_mark_agg, glueContext,
                                                              'dyf_student_behavior_mark_agg')

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
        df.write.parquet("s3a://dts-odin/flag/flag_student_testing_history_starter.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
