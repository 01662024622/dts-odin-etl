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
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField
from datetime import date, datetime, timedelta
import pytz
from pyspark.sql import Row
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

    lastest_number_days = 30
    chosen_word_number = 24

    yesterday = date.today() - timedelta(1)
    yesterday_id = long(yesterday.strftime("%Y%m%d"))

    lasted_30_day = today - timedelta(lastest_number_days)
    lasted_30_day_id = long(lasted_30_day.strftime("%Y%m%d"))

    StructPlusNumber = StructType([
        StructField("lo_plus_number", LongType(), False),
        StructField("learning_object_id", LongType(), False),
        StructField("learning_last_date_id", LongType(), False)])


    def getBestWords(plus_number_pair_list):
        plus_number_pair_list = \
            sorted(plus_number_pair_list, key=lambda x: x['lo_plus_number'], reverse=True)
        a = plus_number_pair_list[0: chosen_word_number]
        return a

    getBestWords = udf(getBestWords, ArrayType(StructPlusNumber))


    #--------------------------------------------------------
    StructMiniNumber = StructType([
        StructField("lo_minus_number", LongType(), False),
        StructField("learning_object_id", LongType(), False),
        StructField("learning_last_date_id", LongType(), False)])

    def getWorstWords(minus_number_pair_list):
        minus_number_pair_list = \
            sorted(minus_number_pair_list, key=lambda x: x['lo_minus_number'], reverse=True)
        a = minus_number_pair_list[0: chosen_word_number]
        return a

    getWorstWords = udf(getWorstWords, ArrayType(StructMiniNumber))
    #----------------------------------------


    print('lasted_30_day_id: ', lasted_30_day_id)

    push_down_predicate = "(created_date_id >= " + str(lasted_30_day_id) + ")"

    dyf_mapping_lo_student_history = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_student_history",
        additional_options={"path": "s3://dts-odin/nvn_knowledge/mapping_lo_student_history/*/*"},
        push_down_predicate=push_down_predicate
    )

    print ('dyf_mapping_lo_student_history')
    dyf_mapping_lo_student_history.printSchema()
    dyf_mapping_lo_student_history.show(3)

    print('dyf_mapping_lo_student_history::number: ', dyf_mapping_lo_student_history.count())

    dyf_mapping_lo_student_history = dyf_mapping_lo_student_history.select_fields(
        ['student_id', 'learning_object_id', 'minus_number', 'plus_number', 'lu_type', 'created_date_id'])

    dyf_mapping_lo_student_history = Filter.apply(frame=dyf_mapping_lo_student_history,
                                                  f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                              and x["learning_object_id"] is not None
                                                              and x["created_date_id"] >= lasted_30_day_id
                                                              and x["lu_type"] == 1
                                                  )


    print ('dyf_mapping_lo_student_history')
    dyf_mapping_lo_student_history.printSchema()
    dyf_mapping_lo_student_history.show(3)

    df_mapping_lo_student_history = dyf_mapping_lo_student_history.toDF()

    print('df_mapping_lo_student_history: ', df_mapping_lo_student_history.count())

    if df_mapping_lo_student_history.count() < 1:
        return


    df_group_plus_minus_number = df_mapping_lo_student_history.groupby('student_id', 'learning_object_id').agg(
        f.sum('plus_number').alias('lo_plus_number'),
        f.sum('minus_number').alias('lo_minus_number'),
        f.max('created_date_id').alias('learning_last_date_id')
    )



    # print('df_group_plus_minus_number')
    df_group_plus_minus_number.printSchema()
    df_group_plus_minus_number.show(3)

    df_group_plus_minus_number = df_group_plus_minus_number.na.fill({'lo_plus_number': 0L, 'lo_minus_number': 0L})

    df_group_plus_minus_number = df_group_plus_minus_number.select('student_id',
                            f.struct('lo_plus_number', 'learning_object_id', 'learning_last_date_id')
                                                                   .alias('plus_number_pair'),
                            f.struct('lo_minus_number', 'learning_object_id', 'learning_last_date_id')
                                                                   .alias('minus_number_pair')
                                                                   )

    df_group_l2 = df_group_plus_minus_number.groupby('student_id').agg(
        f.collect_list('plus_number_pair').alias('plus_number_pair_list'),
        f.collect_list('minus_number_pair').alias('minus_number_pair_list')
    )

    print('df_group_l2')
    df_group_l2.printSchema()
    df_group_l2.show(2)

    df_group_l2 = df_group_l2.withColumn('right_list', getBestWords(df_group_l2.plus_number_pair_list))\
            .withColumn('wrong_list', getWorstWords(df_group_l2.minus_number_pair_list))

    print('df_group_l2---')
    df_group_l2.printSchema()
    df_group_l2.show(1)

    df_group_l2_right = df_group_l2.select('student_id', f.explode('right_list').alias('str_right_item'))
    df_group_l2_wrong = df_group_l2.select('student_id', f.explode('wrong_list').alias('str_wrong_item'))

    df_group_l2_right = df_group_l2_right.select('student_id',
                                                 f.col('str_right_item').getItem("lo_plus_number").alias("learning_object_number"),
                                                 f.col('str_right_item').getItem("learning_object_id").alias("learning_object_id"),
                                                 f.col('str_right_item').getItem("learning_last_date_id").alias("learning_last_date_id"),
                                                 f.lit(1l).alias("number_type"),
                                                 f.lit(yesterday_id).alias('created_date_id')
                                                 )

    df_group_l2_right = df_group_l2_right.filter(df_group_l2_right.learning_object_number.isNotNull())

    df_group_l2_wrong = df_group_l2_wrong.select('student_id',
                                                 f.col('str_wrong_item').getItem("lo_minus_number").alias(
                                                      "learning_object_number"),
                                                 f.col('str_wrong_item').getItem("learning_object_id").alias(
                                                     "learning_object_id"),
                                                 f.col('str_wrong_item').getItem("learning_last_date_id").alias(
                                                     "learning_last_date_id"),
                                                 f.lit(-1l).alias("number_type"),
                                                 f.lit(yesterday_id).alias('created_date_id')
                                                 )

    df_group_l2_wrong = df_group_l2_wrong.filter((df_group_l2_wrong.learning_object_number.isNotNull())
                                                 & (df_group_l2_wrong.learning_object_number != 0L))
    print('df_group_l2_right')
    df_group_l2_right.printSchema()
    df_group_l2_right.show(2)

    print('df_group_l2_wrong')
    df_group_l2_wrong.printSchema()
    df_group_l2_wrong.show(2)

    total_plus_minus = df_group_l2_right.union(df_group_l2_wrong)

    print('total_plus_minus')
    total_plus_minus.printSchema()


    dyf_total_plus_minus = DynamicFrame.fromDF(total_plus_minus, glueContext, 'dyf_total_plus_minus')

    clear_before_saving = 'DELETE student_phonetic_number_history where created_date_id = ' + str(yesterday_id)

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_total_plus_minus,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": clear_before_saving,
                                                                   "dbtable": "student_phonetic_number_history",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3://dts-odin/temp/nvn/knowledge/student_phonetic_number_history/v4",
                                                               transformation_ctx="datasink4")


if __name__ == "__main__":
    main()
