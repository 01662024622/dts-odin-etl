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
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType, IntegerType, LongType
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

    is_dev = True
    is_just_monthly_exam = False
    is_limit_test = False

    start_load_date = 0L

    BEHAVIOR_ID_MICRO = 18L
    BEHAVIOR_ID_AIT = 19L


    CLASS_ID_MICRO = 54L
    CLASS_ID_AIT = 55L

    PERIOD_DAYLY = 1L
    PERIOD_WEEKLY = 2L
    PERIOD_MONTHLY = 3L

    # ------------------------------------------------------------------------------------------------------------------#
    my_partition_predicate = "(behavior_id=='18' or behavior_id=='19')"
    dyf_student_behavior = glueContext.create_dynamic_frame.from_catalog(
        database="od_student_behavior",
        table_name="student_behavior",
        push_down_predicate=my_partition_predicate
    )

    if is_dev:
        print('dyf_student_behavior')
        dyf_student_behavior.printSchema()
        dyf_student_behavior.show(3)
    dyf_student_behavior = dyf_student_behavior.resolveChoice(specs=[('behavior_id', 'cast:long')])
    dyf_student_behavior = Filter.apply(frame=dyf_student_behavior,
                                        f=lambda x: x["student_behavior_id"] is not None
                                                    and x["student_id"] is not None
                                                    and (x["behavior_id"] == BEHAVIOR_ID_MICRO
                                                         or x["behavior_id"] == BEHAVIOR_ID_AIT
                                                        )

                                                    and start_load_date <= x["student_behavior_date"] < today_id_0h00
                                        )

    number_dyf_student_behavior = dyf_student_behavior.count()
    print('number_dyf_student_behavior after filtering: ', number_dyf_student_behavior)
    if number_dyf_student_behavior == 0:
        return

    dyf_student_behavior = dyf_student_behavior \
        .select_fields(['transformed_at','student_behavior_id',
                        'student_behavior_date',
                        'student_id',
                        'behavior_id'])
    # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_student_behavior_MICRO_AIT.parquet")
    max_key = df_flag.collect()[0]['flag']
    print("max_key: ", max_key)
    dyf_student_behavior = Filter.apply(frame=dyf_student_behavior,
                                     f=lambda x: x["transformed_at"] > max_key)
    number_dyf_student_behavior_2 =  dyf_student_behavior.count()
    print('number_dyf_student_behavior after filtering max_key: ', number_dyf_student_behavior_2)
    if number_dyf_student_behavior_2 == 0:
        return
    df_student_behavior = dyf_student_behavior.toDF()
    df_student_behavior = df_student_behavior.drop_duplicates()
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
        push_down_predicate=my_partition_predicate
    )

    number_student_test_detail = dyf_student_test_detail.count()
    if is_dev:
        print('dyf_student_test_detail::original')
        dyf_student_test_detail.printSchema()
        print('dyf_student_test_detail: ', number_student_test_detail)

    if number_student_test_detail == 0:
        return
    dyf_student_test_detail = dyf_student_test_detail.resolveChoice(specs=[('behavior_id', 'cast:long')])
    dyf_student_test_detail = Filter.apply(frame=dyf_student_test_detail,
                                           f=lambda x: x["behavior_id"] == BEHAVIOR_ID_MICRO
                                                       or x["behavior_id"] == BEHAVIOR_ID_AIT
                                           )

    dyf_student_test_detail = dyf_student_test_detail.select_fields(['student_behavior_id',
                                                               'duration',
                                                               'behavior_id'
                                                               ])\
        .rename_field('behavior_id','behavior_id_test_detail') \
        .rename_field('student_behavior_id', 'student_behavior_id_test_detail')

    df_student_test_detail = dyf_student_test_detail.toDF()
    df_student_test_detail = df_student_test_detail.drop_duplicates()
    student_test_detail = df_student_test_detail.count()
    if is_dev:
        print('student_test_detail')
        print ('student_test_detail: ', student_test_detail)
        df_student_test_detail.printSchema()
        df_student_test_detail.show(3)


    df_student_test_detail = df_student_test_detail.repartition('behavior_id_test_detail')

    df_student_test_detail = df_student_behavior.join(other=df_student_test_detail,
                                                    on=df_student_behavior.student_behavior_id == df_student_test_detail.student_behavior_id_test_detail,
                                                    how='inner'
                                                    )

    if is_dev:
        print ('df_student_test_detail')
        df_student_test_detail.printSchema()
        df_student_test_detail.show(3)

    df_student_test_detail = df_student_test_detail.withColumn('agg_date_id',
                                                           from_unixtime(df_student_test_detail.student_behavior_date,
                                                                         "yyyyMMdd")) \
        .withColumn('agg_week_id',
                    from_unixtime(df_student_test_detail.student_behavior_date, "yyyyww")) \
        .withColumn('agg_month_id',
                    from_unixtime(df_student_test_detail.student_behavior_date, "yyyyMM"))

    if is_dev:
        print ('df_student_test_detail')
        df_student_test_detail.printSchema()
        df_student_test_detail.show(3)

    df_behavior_test_detail_agg_date = df_student_test_detail.groupBy('behavior_id', 'student_id', 'agg_date_id')\
        .agg(f.count('student_behavior_id').alias('total_ca'),
             f.sum('duration').alias('total_duration'),
             f.first('agg_week_id').alias('agg_week_id'),
             f.first('agg_month_id').alias('agg_month_id'),
             when(df_student_test_detail.behavior_id == BEHAVIOR_ID_MICRO, CLASS_ID_MICRO).otherwise(CLASS_ID_AIT).alias('class_id'),
             f.lit(PERIOD_DAYLY).alias('period_type_id')
             )

    if is_dev:
        print ('df_behavior_test_detail_agg_date')
        df_behavior_test_detail_agg_date.printSchema()
        df_behavior_test_detail_agg_date.show(3)

    df_behavior_test_detail_agg_date.cache()

    #-------------------------------------------------------------------------------------------------------------------#
    #save for date
    dyf_behavior_test_detail_agg_date = DynamicFrame.fromDF(df_behavior_test_detail_agg_date, glueContext, 'dyf_behavior_test_detail_agg_date')
    apply_output_date = ApplyMapping.apply(frame=dyf_behavior_test_detail_agg_date,
                                     mappings=[("student_id", "long", "student_id", "long"),

                                               ("class_id", "long", "class_id", "long"),
                                               ("period_type_id", "long", "period_type_id", "long"),

                                               ("agg_date_id", "string", "created_date_id", "long"),
                                               ("agg_week_id", "string", "created_week_id", "long"),
                                               ("agg_month_id", "string", "created_month_id", "long"),

                                               ("total_duration", "long", "measure1", "long")
                                               ])

    dfy_output_date = ResolveChoice.apply(frame=apply_output_date, choice="make_cols", transformation_ctx="resolvechoice2")

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dfy_output_date,
        catalog_connection="nvn_knowledge",
        connection_options={
            "dbtable": "student_learning_history",
            "database": "nvn_knowledge_v2"
        },
        redshift_tmp_dir="s3n://dtsodin/temp/nvn_knowledge_v2/student_learning_history",
        transformation_ctx="datasink4"
        )

    # -------------------------------------------------------------------------------------------------------------------#
    df_behavior_test_detail_agg_week = df_behavior_test_detail_agg_date.groupBy('behavior_id', 'student_id', 'agg_week_id') \
        .agg(f.sum('total_ca').alias('total_ca_w'),
             f.sum('total_duration').alias('total_duration_w'),
             when(df_student_test_detail.behavior_id == BEHAVIOR_ID_MICRO, CLASS_ID_MICRO).otherwise(CLASS_ID_AIT).alias(
                 'class_id'),
             f.lit(PERIOD_WEEKLY).alias('period_type_id')
             )

    if is_dev:
        print ('df_behavior_test_detail_agg_week')
        df_behavior_test_detail_agg_week.printSchema()
        df_behavior_test_detail_agg_week.show(3)

    dyf_behavior_test_detail_agg_week = DynamicFrame.fromDF(df_behavior_test_detail_agg_week, glueContext,
                                                         'dyf_behavior_test_detail_agg_week')
    apply_output_week = ApplyMapping.apply(frame=dyf_behavior_test_detail_agg_week,
                                           mappings=[("student_id", "long", "student_id", "long"),

                                                     ("class_id", "long", "class_id", "long"),
                                                     ("period_type_id", "long", "period_type_id", "long"),

                                                     ("agg_week_id", "string", "created_week_id", "long"),

                                                     ("total_duration", "long", "measure1", "long")
                                                     ])

    dfy_output_week = ResolveChoice.apply(frame=apply_output_week, choice="make_cols", transformation_ctx="resolvechoice2")

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dfy_output_week,
        catalog_connection="nvn_knowledge",
        connection_options={
            "dbtable": "student_learning_history",
            "database": "nvn_knowledge_v2"
        },
        redshift_tmp_dir="s3n://dtsodin/temp/nvn_knowledge_v2/student_learning_history",
        transformation_ctx="datasink4"
    )

    # -------------------------------------------------------------------------------------------------------------------#
    df_behavior_test_detail_agg_month = df_behavior_test_detail_agg_date.groupBy('behavior_id', 'student_id', 'agg_month_id') \
        .agg(f.sum('total_ca').alias('total_ca_m'),
             f.sum('total_duration').alias('total_duration_m'),
             when(df_student_test_detail.behavior_id == BEHAVIOR_ID_MICRO, CLASS_ID_MICRO).otherwise(CLASS_ID_AIT).alias(
                 'class_id'),
             f.lit(PERIOD_MONTHLY).alias('period_type_id')
             )

    if is_dev:
        print ('df_behavior_test_detail_agg_month')
        df_behavior_test_detail_agg_month.printSchema()
        df_behavior_test_detail_agg_month.show(3)

        dyf_behavior_test_detail_agg_month = DynamicFrame.fromDF(df_behavior_test_detail_agg_month, glueContext,
                                                             'dyf_behavior_test_detail_agg_month')
        apply_output_month = ApplyMapping.apply(frame=dyf_behavior_test_detail_agg_month,
                                               mappings=[("student_id", "long", "student_id", "long"),

                                                         ("class_id", "long", "class_id", "long"),
                                                         ("period_type_id", "long", "period_type_id", "long"),

                                                         ("agg_month_id", "string", "created_month_id", "long"),

                                                         ("total_duration", "long", "measure1", "long")
                                                         ])

        dyf_output_month = ResolveChoice.apply(frame=apply_output_month, choice="make_cols",
                                         transformation_ctx="resolvechoice2")
        df_output_month = dyf_output_month.toDF()
        df_output_month = df_output_month.drop_duplicates()
        dyf_output_month = DynamicFrame.fromDF(df_output_month, glueContext,
                                                                 'dyf_mapping_student_test_detail_v2')
        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=dyf_output_month,
            catalog_connection="nvn_knowledge",
            connection_options={
                "dbtable": "student_learning_history",
                "database": "nvn_knowledge_v2"
            },
            redshift_tmp_dir="s3n://dtsodin/temp/nvn_knowledge_v2/student_learning_history",
            transformation_ctx="datasink4"
        )
        # ghi data vao redshift
        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output_date,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "student_learning_history",
                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dtsodin/temp/student_learning_history/",
                                                                   transformation_ctx="datasink4")
        print('----------------------> complete------------------>')
        # ghi flag
        # lay max key trong data source
        dyf_student_behavior_tmp = dyf_student_behavior.toDF()
        flag = dyf_student_behavior_tmp.agg({"transformed_at": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_student_behavior_MICRO_AIT.parquet", mode="overwrite")

    df_student_behavior.unpersist()
    df_behavior_test_detail_agg_date.unpersist()


if __name__ == "__main__":
    main()
