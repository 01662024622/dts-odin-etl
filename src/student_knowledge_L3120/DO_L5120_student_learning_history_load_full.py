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
    is_load_full = True

    start_load_date = 0L

    BEHAVIOR_ID_LS = 11
    BEHAVIOR_ID_SC = 12
    BEHAVIOR_ID_LT = 13

    CLASS_ID_LS = 56L
    CLASS_ID_SC = 57L
    CLASS_ID_LT = 58L

    PERIOD_DAYLY = 1L
    PERIOD_WEEKLY = 2L
    PERIOD_MONTHLY = 3L

    # ------------------------------------------------------------------------------------------------------------------#
    my_partition_predicate = "(behavior_id=='11' or behavior_id=='12')"
    dyf_student_behavior = glueContext.create_dynamic_frame.from_catalog(
        database="od_student_behavior",
        table_name="student_behavior",
        push_down_predicate=my_partition_predicate
    )

    dyf_student_behavior = dyf_student_behavior.resolveChoice(specs=[('behavior_id', 'cast:int')])

    if is_dev:
        print('dyf_student_behavior')
        dyf_student_behavior.printSchema()
        dyf_student_behavior.show(3)

    dyf_student_behavior = Filter.apply(frame=dyf_student_behavior,
                                        f=lambda x: x["student_behavior_id"] is not None
                                                    and x["student_id"] is not None
                                                    and x["behavior_id"] in [BEHAVIOR_ID_LS,
                                                                             BEHAVIOR_ID_SC
                                                                             ]
                                                    and start_load_date <= x["student_behavior_date"] < today_id_0h00
                                        )

    number_dyf_student_behavior = dyf_student_behavior.count()
    print('number_dyf_student_behavior after filtering: ', number_dyf_student_behavior)
    if number_dyf_student_behavior == 0:
        return

    # ("student_behavior_id", "string", "student_behavior_id", "string"),
    # ("student_behavior_date", "long", "student_behavior_date", "long"),
    # ("behavior_id", "long", "behavior_id", "long"),
    # ("student_id", "long", "student_id", "long"),
    # ("contact_id", "string", "contact_id", "string"),
    #
    # ("package_code", "string", "package_code", "string"),
    # ("package_endtime", "long", "package_endtime", "long"),
    # ("package_starttime", "long", "package_starttime", "long"),
    #
    # ("student_level_code", "string", "student_level_code", "string"),
    # ("student_status_code", "string", "student_status_code", "string"),
    #
    # ("transformed_at", "long", "transformed_at", "long")

    dyf_student_behavior = dyf_student_behavior \
        .select_fields(['student_behavior_id',
                        'student_behavior_date',
                        'student_id',
                        'behavior_id'])

    df_student_behavior = dyf_student_behavior.toDF()
    df_student_behavior = df_student_behavior.drop_duplicates(['student_behavior_id'])
    # if is_limit_test:
    #     df_student_behavior = df_student_behavior.limit(1000)

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
    dyf_student_learning = glueContext.create_dynamic_frame.from_catalog(
        database="od_student_behavior",
        table_name="student_learning",
        push_down_predicate=my_partition_predicate
    )

    number_student_learning = dyf_student_learning.count()
    if is_dev:
        print('dyf_student_learning::original')
        dyf_student_learning.printSchema()
        print('number_student_learning: ', number_student_learning)

    if number_student_learning == 0:
        return

    dyf_student_learning = Filter.apply(frame=dyf_student_learning,
                                        f=lambda x: x["behavior_id"] in [BEHAVIOR_ID_LS,
                                                                         BEHAVIOR_ID_SC,
                                                                         BEHAVIOR_ID_LT
                                                                         ]
                                        )

    dyf_student_learning = dyf_student_learning.select_fields(['student_behavior_id',
                                                               'class_type',
                                                               'duration',
                                                               'behavior_id'
                                                               ])\
        .rename_field('behavior_id', 'behavior_id_learning') \
        .rename_field('student_behavior_id', 'student_behavior_id_learning')

    df_student_learning = dyf_student_learning.toDF()
    df_student_learning = df_student_learning.drop_duplicates(['student_behavior_id_learning'])
    student_learning_number = df_student_learning.count()
    if is_dev:
        print('dy_student_learning_number')
        print ('dy_student_learning_number: ', student_learning_number)
        df_student_learning.printSchema()
        df_student_learning.show(3)

    if student_learning_number == 0:
        return

    df_student_learning = df_student_learning.repartition('behavior_id_learning')

    df_behavior_learning = df_student_behavior.join(other=df_student_learning,
                                                    on=df_student_behavior.student_behavior_id == df_student_learning.student_behavior_id_learning,
                                                    how='inner'
                                                    )

    if is_dev:
        print ('df_behavior_learning')
        df_behavior_learning.printSchema()
        df_behavior_learning.show(3)

    df_behavior_learning = df_behavior_learning.withColumn('agg_date_id',
                                                           from_unixtime(df_behavior_learning.student_behavior_date,
                                                                         "yyyyMMdd")) \
        .withColumn('agg_week_id',
                    from_unixtime(df_behavior_learning.student_behavior_date, "yyyyww")) \
        .withColumn('agg_month_id',
                    from_unixtime(df_behavior_learning.student_behavior_date, "yyyyMM"))

    if is_dev:
        print ('df_behavior_learning with student_behavior_date_agg_id')
        df_behavior_learning.printSchema()
        df_behavior_learning.show(3)

    df_behavior_learning_agg_date = df_behavior_learning.groupBy('behavior_id', 'student_id', 'agg_date_id')\
        .agg(f.count('student_behavior_id').alias('total_ca'),
             f.sum('duration').alias('total_duration'),
             f.first('agg_week_id').alias('agg_week_id'),
             f.first('agg_month_id').alias('agg_month_id'),
             when(df_behavior_learning.behavior_id == BEHAVIOR_ID_LS, CLASS_ID_LS).otherwise(CLASS_ID_SC).alias('class_id'),
             f.lit(PERIOD_DAYLY).alias('period_type_id')
             )

    if is_dev:
        print ('df_behavior_learning_agg_date')
        df_behavior_learning_agg_date.printSchema()
        df_behavior_learning_agg_date.show(3)

    df_behavior_learning_agg_date.cache()

    #-------------------------------------------------------------------------------------------------------------------#
    #save for date
    dyf_behavior_learning_agg_date = DynamicFrame.fromDF(df_behavior_learning_agg_date, glueContext, 'dyf_behavior_learning_agg_date')
    apply_output_date = ApplyMapping.apply(frame=dyf_behavior_learning_agg_date,
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

    def getClassId(behavior_id):
        if behavior_id == BEHAVIOR_ID_LS:
            return CLASS_ID_LS
        if behavior_id == BEHAVIOR_ID_SC:
            return CLASS_ID_SC
        if behavior_id == BEHAVIOR_ID_LT:
            return CLASS_ID_LT
        return None

    getClassId = f.udf(getClassId, LongType())



    df_behavior_learning_agg_week = df_behavior_learning_agg_date.groupBy('behavior_id', 'student_id', 'agg_week_id') \
        .agg(f.sum('total_ca').alias('total_ca_w'),
             f.sum('total_duration').alias('total_duration_w'),
             getClassId(df_behavior_learning.behavior_id).alias('class_id'),
             f.lit(PERIOD_WEEKLY).alias('period_type_id')
             )

    df_behavior_learning_agg_week = df_behavior_learning_agg_week.filter(df_behavior_learning_agg_week.class_id.isNull())

    if is_dev:
        print ('df_behavior_learning_agg_week')
        df_behavior_learning_agg_week.printSchema()
        df_behavior_learning_agg_week.show(3)

    dyf_behavior_learning_agg_week = DynamicFrame.fromDF(df_behavior_learning_agg_week, glueContext,
                                                         'dyf_behavior_learning_agg_week')
    apply_output_week = ApplyMapping.apply(frame=dyf_behavior_learning_agg_week,
                                           mappings=[("student_id", "long", "student_id", "long"),

                                                     ("class_id", "long", "class_id", "long"),
                                                     ("period_type_id", "long", "period_type_id", "long"),

                                                     ("agg_week_id", "string", "created_week_id", "long"),

                                                     ("total_duration", "long", "measure1", "long")
                                                     ])
    #
    dfy_output_week = ResolveChoice.apply(frame=apply_output_week, choice="make_cols", transformation_ctx="resolvechoice2")
    #
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
    df_behavior_learning_agg_month = df_behavior_learning_agg_date.groupBy('behavior_id', 'student_id', 'agg_month_id') \
        .agg(f.sum('total_ca').alias('total_ca_m'),
             f.sum('total_duration').alias('total_duration_m'),
             when(df_behavior_learning.behavior_id == BEHAVIOR_ID_LS, CLASS_ID_LS).otherwise(CLASS_ID_SC).alias(
                 'class_id'),
             f.lit(PERIOD_MONTHLY).alias('period_type_id')
             )

    if is_dev:
        print ('df_behavior_learning_agg_month')
        df_behavior_learning_agg_month.printSchema()
        df_behavior_learning_agg_month.show(3)

        dyf_behavior_learning_agg_month = DynamicFrame.fromDF(df_behavior_learning_agg_month, glueContext,
                                                             'dyf_behavior_learning_agg_month')
        apply_output_month = ApplyMapping.apply(frame=dyf_behavior_learning_agg_month,
                                               mappings=[("student_id", "long", "student_id", "long"),

                                                         ("class_id", "long", "class_id", "long"),
                                                         ("period_type_id", "long", "period_type_id", "long"),

                                                         ("agg_month_id", "string", "created_month_id", "long"),

                                                         ("total_duration", "long", "measure1", "long")
                                                         ])

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

    df_student_behavior.unpersist()
    df_behavior_learning_agg_date.unpersist()


if __name__ == "__main__":
    main()
