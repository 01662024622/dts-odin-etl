from datetime import datetime
import time

import pytz
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, array, collect_list, explode
from pyspark.sql.types import LongType, ArrayType, StringType
import pyspark.sql.functions as f
from pyspark.storagelevel import StorageLevel

from awsglue import DynamicFrame
from awsglue.context import GlueContext



sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

FLAG_BC200_ADVISOR_FILE = 's3://toxd-olap/olap/flag/flag_bc200_advisor.parquet'

IS_DEV = True

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'
REDSHIFT_DATABASE = "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/"
REDSHIFT_TMP_DIR = 's3://datashine-dev-redshift-backup/log_transform/bc200_test'

#-------------------test function -------------------------------------------------------------------------------------#

def save_dynamic_frame_for_testing(database, table, dyf):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dyf,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir="s3://dts-odin/temp/bc200/" + database + '/' + table,
                                                    transformation_ctx="datasink4")



def retrieve_data_frame(glue_context, database, table_name, push_down_predicate, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database,
                                                                   table_name=table_name,
                                                                   push_down_predicate=push_down_predicate)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)

    data_frame = dynamic_frame.toDF()
    display(data_frame, table_name)
    return data_frame


def display(data_frame, message):
    if IS_DEV:
        print "log_data_frame:", message, data_frame.count()
        data_frame.printSchema()
        data_frame.show(10)


def retrieve_data_frame_from_redshift(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": REDSHIFT_DATABASE + database,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": table_name,
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/" + database}
    )

    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)

    data_frame = dynamic_frame.toDF()
    display(data_frame, table_name)
    return data_frame


def append_pre_advisor(inputs):
    inputs = sorted(inputs, key=lambda x: x[2])

    for i in range(1, len(inputs)):
        inputs[i].append(inputs[i - 1][1])
        inputs[i].append(inputs[i - 1][2])

    inputs[0].append(-1)
    inputs[0].append('BEGIN')

    return inputs


udf_append_pre_advisor = udf(append_pre_advisor, ArrayType(ArrayType(StringType())))


def get_year_month_id_from_date(input):
    date = datetime.fromtimestamp(long(input))
    month_id = long(date.strftime("%Y%m"))
    return month_id


udf_get_year_month_id_from_date = udf(get_year_month_id_from_date, LongType())


def duration_from_date(input):
    date = datetime.fromtimestamp(long(input))
    day_id = long(date.strftime("%d"))
    return day_id


udf_duration_from_date = udf(duration_from_date, LongType())


def next_month_id(month_id):
    if month_id % 100 < 12:
        return month_id + 1
    else:
        return (month_id // 100 + 1) * 100 + 1


def split_month_id(inputs):
    today_second = datetime.today().strftime("%s")
    now_month_id = get_year_month_id_from_date(today_second)
    if inputs[1] > now_month_id:
        inputs[1] = now_month_id
        inputs[3] = duration_from_date(today_second)

    if inputs[0] == inputs[1]:
        return [[inputs[0], inputs[3] - inputs[2]]]

    result = [[inputs[0], 31 - inputs[2]]]

    i = next_month_id(inputs[0])
    while i < inputs[1]:
        result.append([i, 31])
        i = next_month_id(i)

    result.append([inputs[1], inputs[3]])
    return result


udf_split_month_id = udf(split_month_id, ArrayType(ArrayType(LongType())))


def is_advisor(advisor_type, pre_advisor_type, duration):
    if advisor_type == 'GVCM' and pre_advisor_type == 'GVCM':
        if duration >= 15:
            return 1
        else:
            return 0
    else:
        return 1


udf_is_advisor = udf(is_advisor, LongType())


def calculate_advisor(glue_context):
    df_toa_advisor_info = retrieve_data_frame_from_redshift(
        glue_context,
        'student_native_report',
        'toa.toa_advisor_info',
        ['advisor_id', 'advisor_type', 'ip_phone']
    )

    df_ad_student_advisor = retrieve_data_frame_from_redshift(
        glue_context,
        'transaction_log',
        'ad_student_advisor',
        ['advisor_id', 'contact_id', 'start_date', 'end_date']
    )

    df_student_advisor = df_ad_student_advisor.join(
        df_toa_advisor_info,
        on=['advisor_id'],
        how='inner'
    )

    df_student_advisor = df_student_advisor \
        .withColumn('type_start_end_date', array('ip_phone', 'advisor_id', 'advisor_type', 'start_date', 'end_date'))

    display(df_student_advisor, 'df_student_advisor 1')

    df_student_advisor = df_student_advisor \
        .groupBy('contact_id') \
        .agg(collect_list('type_start_end_date').alias('type_start_end_date_s'))

    df_student_advisor = df_student_advisor \
        .withColumn('pre_type_start_end_date_s', udf_append_pre_advisor(df_student_advisor['type_start_end_date_s']))

    display(df_student_advisor, 'df_student_advisor 2')

    df_student_advisor = df_student_advisor \
        .withColumn('pre_type_start_end_date', explode('pre_type_start_end_date_s'))

    dyf_student_advisor = DynamicFrame\
        .fromDF(df_student_advisor, glueContext, 'dyf_student_advisor')


    save_dynamic_frame_for_testing('student_native_report', 'bc200_advisor.checcking_df_student_advisor', )


    df_student_advisor = df_student_advisor \
        .withColumn('ip_phone', (df_student_advisor['pre_type_start_end_date'][0])) \
        .withColumn('advisor_id', (df_student_advisor['pre_type_start_end_date'][1]).cast('long')) \
        .withColumn('advisor_type', df_student_advisor['pre_type_start_end_date'][2]) \
        .withColumn('start_date', df_student_advisor['pre_type_start_end_date'][3]) \
        .withColumn('end_date', df_student_advisor['pre_type_start_end_date'][4]) \
        .withColumn('pre_advisor_id', (df_student_advisor['pre_type_start_end_date'][5]).cast('long')) \
        .withColumn('pre_advisor_type', df_student_advisor['pre_type_start_end_date'][6])

    display(df_student_advisor, 'df_student_advisor 3')

    df_student_advisor = df_student_advisor \
        .withColumn('start_month_id', udf_get_year_month_id_from_date(df_student_advisor['start_date'])) \
        .withColumn('end_month_id', udf_get_year_month_id_from_date(df_student_advisor['end_date'])) \
        .withColumn('start_duration', udf_duration_from_date(df_student_advisor['start_date'])) \
        .withColumn('end_duration', udf_duration_from_date(df_student_advisor['end_date']))

    display(df_student_advisor, 'df_student_advisor 4')

    df_student_advisor = df_student_advisor.select('contact_id', 'ip_phone', 'advisor_id', 'advisor_type', 'start_date',
                                                   'end_date',
                                                   'pre_advisor_id', 'pre_advisor_type', 'start_month_id',
                                                   'end_month_id', 'start_duration', 'end_duration')

    df_student_advisor = df_student_advisor \
        .withColumn('month_id_start_end_duration',
                    array('start_month_id', 'end_month_id', 'start_duration', 'end_duration'))

    df_student_advisor = df_student_advisor \
        .withColumn('month_id_duration_s', udf_split_month_id('month_id_start_end_duration'))

    df_student_advisor = df_student_advisor \
        .withColumn('month_id_duration', explode('month_id_duration_s'))

    df_student_advisor = df_student_advisor \
        .withColumn('month_id', df_student_advisor['month_id_duration'][0]) \
        .withColumn('duration', df_student_advisor['month_id_duration'][1])

    display(df_student_advisor, 'df_student_advisor_5')

    df_student_advisor = df_student_advisor \
        .withColumn('is_advisor',
                    udf_is_advisor('advisor_type', 'pre_advisor_type', 'duration'))

    df_student_advisor = df_student_advisor \
        .groupBy('ip_phone', 'month_id', 'advisor_id') \
        .agg(f.sum('is_advisor').alias('total_student_managed'))

    display(df_student_advisor, 'df_student_advisor_6')

    # dyf_student_advisor = DynamicFrame.fromDF(df_student_advisor, glue_context, 'test')
    # save_data_to_redshift(glue_context,
    #                       dyf_student_advisor,
    #                       'student_native_report',
    #                       'bc200_advisor.df_student_advisor_8',
    #                       REDSHIFT_TMP_DIR,
    #                       'bc200_test')

    return df_student_advisor


def save_data_to_redshift(glue_context, dynamic_frame, database, table, redshift_tmp_dir, transformation_ctx):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir=redshift_tmp_dir,
                                                    transformation_ctx=transformation_ctx)


def is_call(call_status, answer_duration, requested_rating, value_rating):
    result = []

    # is_answer
    if call_status == 'ANSWERED':
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30
    if call_status == 'ANSWERED' and answer_duration > 30:
        result.append(1)
    else:
        result.append(0)

    # is_answer_press_8
    if call_status == 'ANSWERED' and requested_rating == 1:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_press_8
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating > 0:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_0
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 0:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_1
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 1:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_2
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 2:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_3
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 3:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_4
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 4:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_5
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating == 5:
        result.append(1)
    else:
        result.append(0)

    # is_answer_m30_ratting_greater_5
    if call_status == 'ANSWERED' and answer_duration > 30 and requested_rating == 1 and value_rating > 5:
        result.append(1)
    else:
        result.append(0)

    # is_duration_answer_m30
    if call_status == 'ANSWERED' and answer_duration > 30:
        result.append(answer_duration)
    else:
        result.append(0)

    return result


udf_is_call = udf(is_call, ArrayType(LongType()))


def calculate_call(df_input):
    df_result = df_input \
        .withColumn('is_call_s',
                    udf_is_call('call_status', 'answer_duration', 'requested_rating', 'value_rating')) \
        .withColumn('month_id', udf_get_year_month_id_from_date(df_input['student_behavior_date']))

    df_result = df_result \
        .withColumn('is_answer', df_result['is_call_s'][0]) \
        .withColumn('is_answer_m30', df_result['is_call_s'][1]) \
        .withColumn('is_answer_press_8', df_result['is_call_s'][2]) \
        .withColumn('is_answer_m30_press_8', df_result['is_call_s'][3]) \
        .withColumn('is_answer_m30_ratting', df_result['is_call_s'][4]) \
        .withColumn('is_answer_m30_ratting_0', df_result['is_call_s'][5]) \
        .withColumn('is_answer_m30_ratting_1', df_result['is_call_s'][6]) \
        .withColumn('is_answer_m30_ratting_2', df_result['is_call_s'][7]) \
        .withColumn('is_answer_m30_ratting_3', df_result['is_call_s'][8]) \
        .withColumn('is_answer_m30_ratting_4', df_result['is_call_s'][9]) \
        .withColumn('is_answer_m30_ratting_5', df_result['is_call_s'][10]) \
        .withColumn('is_answer_m30_ratting_greater_5', df_result['is_call_s'][11]) \
        .withColumn('is_duration_answer_m30', df_result['is_call_s'][12])

    df_result = df_result \
        .groupBy('ip_phone', 'month_id') \
        .agg(f.count('idcall').alias('total_call_month'),
             f.sum('is_answer').alias('total_call_answer_month'),
             f.sum('is_answer_m30').alias('total_call_answer_m30_month'),
             f.sum('is_answer_press_8').alias('total_call_answer_press_8_month'),
             f.sum('is_answer_m30_press_8').alias('total_call_answer_m30_and_press_8_month'),
             f.sum('is_answer_m30_ratting').alias('total_call_answer_m30_ratting_month'),
             f.sum('is_answer_m30_ratting_0').alias('total_call_answer_m30_ratting_0_month'),
             f.sum('is_answer_m30_ratting_1').alias('total_call_answer_m30_ratting_1_month'),
             f.sum('is_answer_m30_ratting_2').alias('total_call_answer_m30_ratting_2_month'),
             f.sum('is_answer_m30_ratting_3').alias('total_call_answer_m30_ratting_3_month'),
             f.sum('is_answer_m30_ratting_4').alias('total_call_answer_m30_ratting_4_month'),
             f.sum('is_answer_m30_ratting_5').alias('total_call_answer_m30_ratting_5_month'),
             f.sum('is_answer_m30_ratting_greater_5').alias('total_call_answer_m30_ratting_greater_5_month'),
             f.sum('is_duration_answer_m30').alias('total_duration_answer_m30_month'),
             f.sum('answer_duration').alias('total_duration_answer_month'))

    df_result = df_result \
        .withColumn('is_student_call_month',
                    f.when(df_result['total_call_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_month',
                    f.when(df_result['total_call_answer_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_month',
                    f.when(df_result['total_call_answer_m30_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_press_8_month',
                    f.when(df_result['total_call_answer_press_8_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_press_8_month',
                    f.when(df_result['total_call_answer_m30_and_press_8_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_month',
                    f.when(df_result['total_call_answer_m30_ratting_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_0_month',
                    f.when(df_result['total_call_answer_m30_ratting_0_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_1_month',
                    f.when(df_result['total_call_answer_m30_ratting_1_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_2_month',
                    f.when(df_result['total_call_answer_m30_ratting_2_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_3_month',
                    f.when(df_result['total_call_answer_m30_ratting_3_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_4_month',
                    f.when(df_result['total_call_answer_m30_ratting_4_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_5_month',
                    f.when(df_result['total_call_answer_m30_ratting_5_month'] > 0, 1).otherwise(0)) \
        .withColumn('is_student_answer_m30_ratting_greater_5_month',
                    f.when(df_result['total_call_answer_m30_ratting_greater_5_month'] > 0, 1).otherwise(0))

    df_result = df_result \
        .groupBy('ip_phone', 'month_id') \
        .agg(f.sum('total_call_month').alias('total_call'),
             f.sum('total_call_answer_month').alias('total_call_answer'),
             f.sum('total_call_answer_m30_month').alias('total_call_answer_m30'),
             f.sum('total_call_answer_press_8_month').alias('total_call_answer_press_8'),
             f.sum('total_call_answer_m30_and_press_8_month').alias('total_call_answer_m30_and_press_8'),
             f.sum('total_call_answer_m30_ratting_month').alias('total_call_answer_m30_ratting'),
             f.sum('total_call_answer_m30_ratting_0_month').alias('total_call_answer_m30_ratting_0'),
             f.sum('total_call_answer_m30_ratting_1_month').alias('total_call_answer_m30_ratting_1'),
             f.sum('total_call_answer_m30_ratting_2_month').alias('total_call_answer_m30_ratting_2'),
             f.sum('total_call_answer_m30_ratting_3_month').alias('total_call_answer_m30_ratting_3'),
             f.sum('total_call_answer_m30_ratting_4_month').alias('total_call_answer_m30_ratting_4'),
             f.sum('total_call_answer_m30_ratting_5_month').alias('total_call_answer_m30_ratting_5'),
             f.sum('total_call_answer_m30_ratting_greater_5_month').alias(
                 'total_call_answer_m30_ratting_greater_5'),
             f.sum('total_duration_answer_m30_month').alias('total_duration_answer_m30'),
             f.sum('total_duration_answer_month').alias('total_duration_answer'),

             f.sum('is_student_call_month').alias('total_student_call'),
             f.sum('is_student_answer_month').alias('total_student_answer'),
             f.sum('is_student_answer_m30_month').alias('total_student_answer_m30'),
             f.sum('is_student_answer_press_8_month').alias('total_student_answer_press_8'),
             f.sum('is_student_answer_m30_press_8_month').alias('total_student_answer_m30_press_8'),
             f.sum('is_student_answer_m30_ratting_month').alias('total_student_answer_m30_ratting'),
             f.sum('is_student_answer_m30_ratting_0_month').alias('total_student_answer_m30_ratting_0'),
             f.sum('is_student_answer_m30_ratting_1_month').alias('total_student_answer_m30_ratting_1'),
             f.sum('is_student_answer_m30_ratting_2_month').alias('total_student_answer_m30_ratting_2'),
             f.sum('is_student_answer_m30_ratting_3_month').alias('total_student_answer_m30_ratting_3'),
             f.sum('is_student_answer_m30_ratting_4_month').alias('total_student_answer_m30_ratting_4'),
             f.sum('is_student_answer_m30_ratting_5_month').alias('total_student_answer_m30_ratting_5'),
             f.sum('is_student_answer_m30_ratting_greater_5_month').alias('total_student_answer_m30_ratting_greater_5'))

    return df_result


def get_flag(spark, data_frame):
    flag = data_frame.agg({"time_id": "max"}).collect()[0][0]
    flag_data = [flag]
    return spark.createDataFrame(flag_data, "string").toDF('flag')


def save_flag(data_frame, flag_path):
    data_frame.write.parquet(flag_path, mode="overwrite")


def select_student_advisor_fact(dyf_result):
    dyf_result = dyf_result.select_fields([
        'period_id',
        'time_id',
        'advisor_id',
        'total_call',
        'total_call_answer',
        'total_call_answer_m30',
        'total_call_answer_press_8',
        'total_call_answer_m30_and_press_8',
        'total_call_answer_m30_ratting',
        'total_call_answer_m30_ratting_0',
        'total_call_answer_m30_ratting_1',
        'total_call_answer_m30_ratting_2',
        'total_call_answer_m30_ratting_3',
        'total_call_answer_m30_ratting_4',
        'total_call_answer_m30_ratting_5',
        'total_call_answer_m30_ratting_greater_5',
        'total_duration_answer_m30',
        'total_duration_answer',

        'total_student_call',
        'total_student_answer',
        'total_student_answer_m30',
        'total_student_answer_press_8',
        'total_student_answer_m30_press_8',
        'total_student_answer_m30_ratting',
        'total_student_answer_m30_ratting_0',
        'total_student_answer_m30_ratting_1',
        'total_student_answer_m30_ratting_2',
        'total_student_answer_m30_ratting_3',
        'total_student_answer_m30_ratting_4',
        'total_student_answer_m30_ratting_5',
        'total_student_answer_m30_ratting_greate_5',
    ])
    dyf_result = dyf_result.resolveChoice([
        ('period_id', 'cast:long'),

        ('time_id', 'cast:long'),
        ('advisor_id', 'cast:long'),
        ('total_call', 'cast:long'),
        ('total_call_answer', 'cast:long'),
        ('total_call_answer_m30', 'cast:long'),
        ('total_call_answer_press_8', 'cast:long'),
        ('total_call_answer_m30_and_press_8', 'cast:long'),
        ('total_call_answer_m30_ratting', 'cast:long'),
        ('total_call_answer_m30_ratting_0', 'cast:long'),
        ('total_call_answer_m30_ratting_1', 'cast:long'),
        ('total_call_answer_m30_ratting_2', 'cast:long'),
        ('total_call_answer_m30_ratting_3', 'cast:long'),
        ('total_call_answer_m30_ratting_4', 'cast:long'),
        ('total_call_answer_m30_ratting_5', 'cast:long'),
        ('total_call_answer_m30_ratting_greater_5', 'cast:long'),
        ('total_duration_answer_m30', 'cast:long'),
        ('total_duration_answer', 'cast:long'),

        ('total_student_call', 'cast:long'),
        ('total_student_answer', 'cast:long'),
        ('total_student_answer_m30', 'cast:long'),
        ('total_student_answer_press_8', 'cast:long'),
        ('total_student_answer_m30_press_8', 'cast:long'),
        ('total_student_answer_m30_ratting', 'cast:long'),
        ('total_student_answer_m30_ratting_0', 'cast:long'),
        ('total_student_answer_m30_ratting_1', 'cast:long'),
        ('total_student_answer_m30_ratting_2', 'cast:long'),
        ('total_student_answer_m30_ratting_3', 'cast:long'),
        ('total_student_answer_m30_ratting_4', 'cast:long'),
        ('total_student_answer_m30_ratting_5', 'cast:long'),
        ('total_student_answer_m30_ratting_greate_5', 'cast:long')
    ])
    display(dyf_result, 'dyf_student_advisor_fact')
    return dyf_result


def main():


    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)

    # ------------------------------------------------------------------------------------------------------------------#
    start_year_month_id = 201900
    end_year_month_id = today.strftime("%Y%m")

    try:
        df_flag = spark.read.parquet(FLAG_BC200_ADVISOR_FILE)
        display(df_flag, "df_flag")

        start_year_month_id = df_flag.collect()[0]['flag']
    except:
        print 'read flag file error '

    if start_year_month_id >= start_year_month_id:
        print 'The data was etl on this week', start_year_month_id, end_year_month_id
        pass

    print('start_year_month_id: ', start_year_month_id)
    print('end_year_month_id: ', end_year_month_id)

    # ------------------------------------------------------------------------------------------------------------------#
    push_down_predicate = "( year_month_id >= '" + str(start_year_month_id) + "' " \
                          + " and year_month_id <= '" + str(end_year_month_id) + "') "

    df_student_care_advisor = retrieve_data_frame(
        glue_context,
        database='callcenter',
        table_name='student_care_advisor',
        push_down_predicate=push_down_predicate,
        fields=['idcall', 'student_behavior_date', 'ip_phone', 'student_id', 'contact_id', 'call_status',
                'answer_duration', 'requested_rating', 'value_rating']
    )

    if df_student_care_advisor.count() <= 0:
        pass

    # -----------------------------------------------------------------------------------------------------------------#
    df_call = calculate_call(df_student_care_advisor)
    df_call.persist(StorageLevel.DISK_ONLY_2)

    df_student_advisor = calculate_advisor(glue_context=glue_context)
    df_student_advisor.persist(StorageLevel.DISK_ONLY_2)

    # -----------------------------------------------------------------------------------------------------------------#
    df_result = df_call.join(df_student_advisor,
                             on=['ip_phone', 'month_id'],
                             how='inner')

    # -----------------------------------------------------------------------------------------------------------------#
    df_result = df_result \
        .withColumn('period_id', f.lit(2)) \
        .withColumnRenamed('month_id', 'time_id')

    # -----------------------------------------------------------------------------------------------------------------#
    df_result = df_result.dropDuplicates()

    # -----------------------------------------------------------------------------------------------------------------#
    dyf_result = DynamicFrame.fromDF(df_result, glue_context, 'test')
    dyf_result = select_student_advisor_fact(dyf_result)
    save_data_to_redshift(glue_context,
                          dyf_result,
                          'student_native_report',
                          'bc200_advisor.advisor_care_fact',
                          REDSHIFT_TMP_DIR,
                          'advisor_care_fact')

    # -----------------------------------------------------------------------------------------------------------------#
    df_flag = get_flag(spark=spark, data_frame=df_result)
    if df_flag.collect()[0]['flag'] is not None:
        print 'save_flag done'
        save_flag(df_flag, FLAG_BC200_ADVISOR_FILE)

    # -----------------------------------------------------------------------------------------------------------------#
    df_call.unpersist()
    df_student_advisor.unpersist()


if __name__ == '__main__':
    main()
