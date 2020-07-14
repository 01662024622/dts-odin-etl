import sys
from array import ArrayType
from datetime import datetime

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
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField, StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import TimestampType
import pytz
from datetime import date, datetime, timedelta
import boto3

is_dev = True
MAX_DATE = 99999999999

ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.now(ho_chi_minh_timezone)
print('today: ', today)
yesterday = today - timedelta(1)
print('yesterday: ', yesterday)
today_id = long(today.strftime("%Y%m%d"))
yesterday_id = long(yesterday.strftime("%Y%m%d"))
print('today_id: ', today_id)
print('yesterday_id: ', yesterday_id)


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame


def display(data_frame, message):
    print "log_data_frame:", message, data_frame.count()
    data_frame.printSchema()
    data_frame.show(10)


# def map_end_time(start_time=[]):
#     result = []
#     start_time_len = len(start_time)
#
#     for i in range(0, start_time_len - 1):
#         item = [start_time[i][0], start_time[i][1], start_time[i + 1][1]]
#         result.append(item)
#
#     result.append([start_time[start_time_len - 1][0], start_time[start_time_len - 1][1], MAX_DATE])
#
#     return result


def map_end_time(time_product_status_list=[]):
    result = []
    list_len = len(time_product_status_list)

    for i in range(0, list_len - 1):
        item = [time_product_status_list[i][1],
                time_product_status_list[i][2],
                time_product_status_list[i][0],
                time_product_status_list[i + 1][0]]
        result.append(item)

    result.append(
        [time_product_status_list[list_len - 1][1],
        time_product_status_list[list_len - 1][2],
        time_product_status_list[list_len - 1][0],
        MAX_DATE])

    return result


udf_map_end_time = udf(map_end_time, ArrayType(ArrayType(StringType())))


def etl_package_category(glueContext):
    # dfy_tpe_category = glueContext.create_dynamic_frame.from_catalog(
    #     database="tig_market",
    #     table_name="tpe_category"
    # )

    # dfy_tpe_category = dfy_tpe_category \
    #     .select_fields(['id', 'cat_code',
    #                     ]) \
    #     .rename_field('cat_code', 'package_code') \
    #
    # dfy_tpe_category = Filter.apply(frame=dfy_tpe_category,
    #                                f=lambda x: x['cat_code'] is not None)
    #
    # df_tpe_category = dfy_tpe_category.toDF()
    # df_tpe_category = df_tpe_category.dropDuplicates(['cat_code'])
    #
    # if is_dev:
    #     print ('df_tpe_category')
    #     df_tpe_category.printSchema()
    #     df_tpe_category.show(3)




    dyf_tpe_attr_value = glueContext.create_dynamic_frame.from_catalog(
        database="tig_market",
        table_name="tpe_attr_value"
    )

    dyf_tpe_attr_value = dyf_tpe_attr_value \
        .select_fields(['_key',
                        'cat_code', 'package_name', 'package_description', 'package_status', 'package_type',
                        'gen_code', 'package_time',
                        'timecreated'
                        ]) \
        .rename_field('cat_code', 'package_code') \
        .rename_field('timecreated', 'package_time_created')

    df_tpe_attr_value = dyf_tpe_attr_value.toDF()
    df_tpe_attr_value = df_tpe_attr_value.dropDuplicates(['package_code'])

    if is_dev:
        print ('df_tpe_attr_value')
        df_tpe_attr_value.printSchema()
        df_tpe_attr_value.show(3)


def etl_student_package(glueContext):
    # output: 'contact_id', 'status_new', 'timecreated', 'used_product_id'
    dyf_tpe_enduser_used_product_history = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_enduser_used_product_history',
        ['id', 'contact_id', 'status_new', 'timecreated', 'used_product_id'])

    dyf_tpe_enduser_used_product_history = Filter.apply(frame=dyf_tpe_enduser_used_product_history,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                   and x["used_product_id"] is not None)

    df_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.toDF()
    df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.dropDuplicates(['id'])

    if is_dev:
        print('df_tpe_enduser_used_product_history original')
        df_tpe_enduser_used_product_history.printSchema()
        df_tpe_enduser_used_product_history.show(3)


    #-------------------------------------------------------------------------------------------------------------------#
    dyf_tpe_enduser_used_product = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_enduser_used_product',
        ['id', 'product_id'])\
        .rename_field('id', 'enduser_used_product_id')

    dyf_tpe_enduser_used_product = Filter.apply(frame=dyf_tpe_enduser_used_product,
                                                        f=lambda x: x["product_id"] is not None and x[
                                                            "product_id"] != '')

    df_tpe_enduser_used_product = dyf_tpe_enduser_used_product.toDF()
    df_tpe_enduser_used_product = df_tpe_enduser_used_product.dropDuplicates(['enduser_used_product_id'])

    if is_dev:
        print('df_tpe_enduser_used_product original')
        df_tpe_enduser_used_product.printSchema()
        df_tpe_enduser_used_product.show(3)

    # output: 'id', 'cat_code'
    dyf_tpe_invoice_product_details = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_invoice_product_details',
        ['id', 'cat_code']
    ).rename_field('cat_code', 'package_code')

    df_tpe_invoice_product_details = dyf_tpe_invoice_product_details.toDF()
    df_tpe_invoice_product_details = df_tpe_invoice_product_details.dropDuplicates(['id'])

    # output: 'student_id', 'contact_id'
    dyf_student_contact = retrieve_dynamic_frame(
        glueContext,
        'tig_advisor',
        'student_contact',
        ['id', 'student_id', 'contact_id']
    )

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                      f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                  and x["contact_id"] is not None and x["contact_id"] != '')

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])

    #------------------------------------------------------------------------------------------------------------------#
    # get packge status start_time, end_time by contact_id
    df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history\
        .orderBy(f.asc('contact_id'), f.asc('timecreated'))

    df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.select(
        'contact_id',
        f.array('timecreated', 'used_product_id', 'status_new').alias('time_product_status')
    )

    if is_dev:
        print('df_tpe_enduser_used_product_history')
        df_tpe_enduser_used_product_history.printSchema()
        df_tpe_enduser_used_product_history.show(3)

    df_tpe_enduser_used_product_history_group_contact = df_tpe_enduser_used_product_history\
        .groupBy('contact_id')\
        .agg(
            f.collect_list('time_product_status').alias('time_product_status_list')
        )

    if is_dev:
        print('df_tpe_enduser_used_product_history_group_contact')
        df_tpe_enduser_used_product_history_group_contact.printSchema()
        df_tpe_enduser_used_product_history_group_contact.show(3)

    df_tpe_enduser_used_product_history_result =df_tpe_enduser_used_product_history_group_contact.select(
        'contact_id',
        udf_map_end_time('time_product_status_list').alias('product_status_start_end_list')
    )

    if is_dev:
        print('df_tpe_enduser_used_product_history_result')
        df_tpe_enduser_used_product_history_result.printSchema()
        df_tpe_enduser_used_product_history_result.show(3)

    df_tpe_enduser_used_product_history_result_explore = df_tpe_enduser_used_product_history_result\
        .select(
            'contact_id',
            f.explode('product_status_start_end_list').alias('product_status_start_end')
        )

    if is_dev:
        print('df_tpe_enduser_used_product_history_result_explore')
        df_tpe_enduser_used_product_history_result_explore.printSchema()
        df_tpe_enduser_used_product_history_result_explore.show(3)

    df_tpe_enduser_used_product_history_result_explore = df_tpe_enduser_used_product_history_result_explore\
        .select(
            'contact_id',
            df_tpe_enduser_used_product_history_result_explore['product_status_start_end'][0].alias('used_product_id'),
            df_tpe_enduser_used_product_history_result_explore['product_status_start_end'][1].alias('status_code'),
            df_tpe_enduser_used_product_history_result_explore['product_status_start_end'][2].alias('start_time'),
            df_tpe_enduser_used_product_history_result_explore['product_status_start_end'][3].alias('end_time')
        )

    if is_dev:
        print('df_tpe_enduser_used_product_history_result_explore get detail')
        df_tpe_enduser_used_product_history_result_explore.printSchema()
        df_tpe_enduser_used_product_history_result_explore.show(3)

    #------------------------------------------------------------------------------------------------------------------#

    # get package_code, student_id
    df_result = df_tpe_enduser_used_product_history_result_explore\
        .join(df_tpe_enduser_used_product,
              df_tpe_enduser_used_product_history_result_explore.used_product_id == df_tpe_enduser_used_product.enduser_used_product_id, 'inner') \
        .join(df_tpe_invoice_product_details,
              df_tpe_enduser_used_product.product_id == df_tpe_invoice_product_details.id, 'inner')\
        .join(df_student_contact, 'contact_id', 'left')\
        .withColumn('created_at', f.lit(today_id))

    if is_dev:
        print('df_result get detail')
        df_result.printSchema()
        df_result.show(3)


    dyf_result = DynamicFrame \
        .fromDF(df_result, glueContext, "dyf_result")

    # | -- contact_id: string(nullable=true)
    # | -- used_product_id: string(nullable=true)
    # | -- status: string(nullable=true)
    # | -- start_time: string(nullable=true)
    # | -- end_time: string(nullable=true)
    # | -- id: string(nullable=true)
    # | -- package_code: string(nullable=true)
    # | -- student_id: string(nullable=true)

    mapping_result = ApplyMapping.apply(frame=dyf_result,
                                      mappings=[("student_id", "string", "student_id", "long"),
                                                ("contact_id", 'string', 'contact_id', 'string'),

                                                ("package_code", 'string', 'package_code', 'string'),
                                                ("status_code", 'string', 'package_status_code', 'string'),

                                                ("start_time", 'string', 'package_start_time', 'long'),
                                                ("end_time", 'string', 'package_end_time', 'long'),

                                                ("created_at", 'long', 'created_at', 'long')

                                                ])

    resolvechoice = ResolveChoice.apply(frame=mapping_result,
                                        choice="make_cols",
                                        transformation_ctx="resolvechoice2")

    dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")


    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "postactions": """TRUNCATE TABLE ad_student_package """,
                                                       "dbtable": "ad_student_package",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package",
                                                   transformation_ctx="datasink4")

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # etl_package_category(glueContext)
    etl_student_package(glueContext)


if __name__ == "__main__":
    main()
    # resutl = map_end_time(['121212', '234234', '234234', '567567', '567356'])
    # print resutl
