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
from pyspark.sql.window import Window
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

student_id_unavailable = 0L
package_endtime_unavailable = 99999999999L
package_starttime_unavailable = 0L

is_dev = True
is_checking = False
is_checking_level_2 = False
contact_id_checking = '2020041105576639'
ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.now(ho_chi_minh_timezone)
print('today: ', today)
yesterday = today - timedelta(1)
print('yesterday: ', yesterday)
today_id = long(today.strftime("%Y%m%d"))
yesterday_timestamp = long(yesterday.strftime("%s"))
yesterday_id = long(yesterday.strftime("%Y%m%d"))
print('today_id: ', today_id)
print('yesterday_id: ', yesterday_id)

UNAVAILABLE = 'unavailable'
START = -1
END = -1

MIN_DATE = 946663200L
MAX_DATE = 99999999999L
MIN_TIME_CREATED = 1514739600L

TOPICA_EMAIL_END = '%@topica.edu.vn'


def do_check_endtime(val):
    if val is not None:
        return val
    return 4094592235


check_endtime_null = udf(do_check_endtime, LongType())

LevelStartEnd = StructType([
    StructField("level", StringType(), False),
    StructField("start", StringType(), False),
    StructField("end", StringType(), False),
])


# -----------------------------------------------------------------------------------------------------------------------#

def get_level_start_end(date_level_pairs):
    level = UNAVAILABLE
    start = START
    end = END
    if date_level_pairs is None:
        level = UNAVAILABLE
        start = START
        end = END

    # for date_level_pair in date_level_pairs:
    print('date_level_pairs')
    print(date_level_pairs)

    length_l_p = len(date_level_pairs)

    date_level_pairs = sorted(date_level_pairs, key=lambda x: x[0])

    result = []
    for i in range(0, length_l_p - 1):
        print('i: ', i)
        element_current = date_level_pairs[i]
        element_next = date_level_pairs[i + 1]
        # print('element')
        # print (element)
        #
        # print('element_1: ', element[0])
        # print('element_2: ', element[1])
        level_start_end = Row('level', 'start', 'end')(element_current[1], element_current[0], element_next[0])
        print ('level_start_end')
        print (level_start_end)
        result.append(level_start_end)
    # a2 = Row('level', 'start', 'end')(level, start, end)

    level_pairs_final = date_level_pairs[length_l_p - 1]
    final_element = Row('level', 'start', 'end')(
        level_pairs_final[1],
        level_pairs_final[0],
        MAX_DATE
    )
    print ('final_element')
    print (final_element)
    result.append(final_element)

    return result


get_level_start_end = f.udf(get_level_start_end, ArrayType(LevelStartEnd))


# def get_df_student_contact(glueContext):
#     dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
#         database="tig_advisor",
#         table_name="student_contact"
#     )
#
#     dyf_student_contact = dyf_student_contact.select_fields(
#         ['contact_id', 'student_id', 'level_study'])
#
#     dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])
#
#     dyf_student_contact = Filter.apply(frame=dyf_student_contact,
#                                        f=lambda x: x['student_id'] is not None
#                                                    and x['contact_id'] is not None
#                                                    and x['level_study'] is not None and x['level_study'] != '')
#
#     df_student_contact = dyf_student_contact.toDF()
#     return df_student_contact


def get_df_change_level_new(df_student_level):
    df_student_level_new = df_student_level.select(
        'contact_id',
        df_student_level.level_modified.alias('level'),
        'time_level_created'
    )
    return df_student_level_new
    # dyf_student_level = dyf_student_level.select_fields(
    #     ['_key', 'id', 'contact_id', 'level_current', 'level_modified', 'time_created']) \
    #     .rename_field('time_created', 'time_level_created') \
    #     .rename_field('level_modified', 'level') \
    #     .rename_field('level_current', 'old_level')


def get_df_change_level_init(df_student_level):
    df_student_level_new = df_student_level.select(
        'contact_id',
        df_student_level.level_current.alias('level'),
        f.lit(MIN_DATE).alias('time_level_created')
    )

    df_student_level_new = df_student_level_new.orderBy(f.asc('contact_id'), f.asc('time_level_created'))
    df_student_level_first = df_student_level_new.groupBy('contact_id').agg(
        f.first('level').alias('level'),
        f.first('time_level_created').alias('time_level_created')
    )

    return df_student_level_first


def get_level_from_student_contact(glueContext, df_contact_list):
    df_student_contact = get_df_student_contact(glueContext)

    if is_dev:
        print ('df_student_contact')
        df_student_contact.printSchema()
        df_student_contact.show(3)

    df_student_contact = df_student_contact.join(df_contact_list, 'contact_id', 'left_anti')
    # print('get_current_level_student_contact')
    # df_student_contact.printSchema()
    # df_student_contact.show(3)

    df_student_contact_level = df_student_contact.select(
        'contact_id',
        df_student_contact.level_study.alias('level'),
        f.lit(MIN_DATE).alias('time_level_created')
    )

    return df_student_contact_level


def change_level(glueContext):
    if is_dev:
        print('start change_level-----------------------------------------')

    dyf_student_level = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_student_level_study"
    )

    # print('dyf_student_level')
    # dyf_student_level.printSchema()
    # dyf_student_level.show(3)

    dyf_student_level = dyf_student_level.select_fields(
        ['_key', 'id', 'contact_id', 'level_current', 'level_modified', 'time_created']) \
        .rename_field('time_created', 'time_level_created')

    dyf_student_level = dyf_student_level.resolveChoice(specs=[('time_level_created', 'cast:long')])

    dyf_student_level = Filter.apply(frame=dyf_student_level,
                                     f=lambda x: x['contact_id'] is not None and x['contact_id'] != ''
                                                 and x['time_level_created'] is not None
                                                 and x['level_modified'] is not None
                                     )

    df_student_level = dyf_student_level.toDF()
    # print('df_student_level__A1')
    # df_student_level.printSchema()
    # df_student_level.show(3)

    df_student_level = df_student_level \
        .orderBy(f.asc('contact_id'), f.asc('time_level_created'), f.desc('id'))
    df_student_level = df_student_level.dropDuplicates(['contact_id', 'time_level_created'])

    df_student_level.cache()

    print('df_student_level')
    df_student_level.printSchema()
    df_student_level.show(3)
    number_student_level = df_student_level.count()
    print('number_student_level: ', number_student_level)

    df_change_level_new = get_df_change_level_new(df_student_level)
    df_change_level_init = get_df_change_level_init(df_student_level)

    # ------------------------------------------------------------------------------------------------------------------#
    # get student and level in student contact
    df_contact_id_list = df_student_level.select(
        'contact_id'
    )

    df_contact_id_list = df_contact_id_list.dropDuplicates(['contact_id'])

    df_level_from_student_contact = get_level_from_student_contact(glueContext, df_contact_id_list)

    print('df_level_from_student_contact')
    df_level_from_student_contact.printSchema()
    df_level_from_student_contact.show(3)

    # -------------------------------------------------------------------------------------------------------------------#

    change_level_total = df_change_level_new \
        .union(df_change_level_init) \
        .union(df_level_from_student_contact)

    if is_dev:
        print ('change_level_total')
        change_level_total.printSchema()
        change_level_total.show(3)

    # -------------------------------------------------------------------------------------------------------------------#

    # df_student_contact = get_df_student_contact(glueContext)

    if number_student_level < 1:
        return

    change_level_total = change_level_total.orderBy(f.asc('contact_id'), f.asc('time_level_created'))

    if is_dev:
        print('change_level_total________________--at here')
        change_level_total.printSchema()
        change_level_total.show(3)

    change_level_total = change_level_total.select(
        'contact_id',
        f.struct('time_level_created', 'level').alias("s_time_level")
    )

    print('df_student_level')
    change_level_total.printSchema()
    change_level_total.show(3)

    df_student_level_list = change_level_total.groupBy('contact_id') \
        .agg(f.collect_list('s_time_level').alias('l_time_level'))

    print('df_student_level_list')
    df_student_level_list.printSchema()
    df_student_level_list.show(3)

    df_student_level_list = df_student_level_list.select(
        'contact_id',
        get_level_start_end('l_time_level').alias('l_level_start_ends')
    )

    print('df_student_level_list_v2')
    df_student_level_list.printSchema()
    df_student_level_list.show(3)

    df_student_level_list = df_student_level_list.select(
        'contact_id',
        f.explode('l_level_start_ends').alias('level_start_end')
    )

    print('df_student_level_list_v2 after explode')
    df_student_level_list.printSchema()
    df_student_level_list.show(3)

    df_student_level_start_end = df_student_level_list.select(
        'contact_id',
        f.col("level_start_end").getItem("level").alias("level"),
        f.col("level_start_end").getItem("start").alias("start").cast('long'),
        f.col("level_start_end").getItem("end").alias("end").cast('long'),
        f.lit(today_id).alias('created_at')

    )

    print('df_student_level_start_end')
    df_student_level_start_end.printSchema()
    df_student_level_start_end.show(3)

    dyf_student_level_start_end_student = DynamicFrame \
        .fromDF(df_student_level_start_end, glueContext, "dyf_student_level_start_end_student")

    applyMapping = ApplyMapping.apply(frame=dyf_student_level_start_end_student,
                                      mappings=[("contact_id", "string", "contact_id", "string"),
                                                ("level", 'string', 'level_code', 'string'),
                                                ("start", "long", "start_date", "long"),
                                                ("end", "long", "end_date", "long"),
                                                ("created_at", "long", "created_at", "long")
                                                ])

    resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols", transformation_ctx="resolvechoice2")

    dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "preactions": """TRUNCATE TABLE ad_student_level """,
                                                       "dbtable": "ad_student_level",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level",
                                                   transformation_ctx="datasink4")

    df_student_level.unpersist()


# -----------------------------------------------------------------------------------------------------------------------#
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


def map_end_time(time_product_status_list):
    result = []
    list_len_ = len(time_product_status_list) - 1

    time_product_status_list = sorted(time_product_status_list, key=lambda x: x[0])

    if list_len_ > 0:
        for i in range(0, list_len_):
            element_current = time_product_status_list[i]
            element_next = time_product_status_list[i + 1]

            item = [element_current[1],
                    element_current[2],
                    str(element_current[0]),
                    str(element_next[0])]
            result.append(item)

    final_element = time_product_status_list[list_len_]
    result.append([
        final_element[1],
        final_element[2],
        str(final_element[0]),
        long(MAX_DATE)])

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

def get_df_student_contact_v2(glueContext):
    dyf_student_contact = retrieve_dynamic_frame(
        glueContext,
        'tig_advisor',
        'student_contact',
        ['student_id', 'contact_id', 'user_name', 'time_lms_created', 'product_id']
    )

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                   and x["contact_id"] is not None and x["contact_id"] != ''
                                                   and x["user_name"] is not None and x["user_name"] != ''
                                       )

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])
    return df_student_contact


def get_df_market_product_history(glueContext):
    dyf_tpe_enduser_used_product_history = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_enduser_used_product_history',
        ['id', 'contact_id', 'status_new', 'timecreated', 'used_product_id'])

    dyf_tpe_enduser_used_product_history = Filter \
        .apply(frame=dyf_tpe_enduser_used_product_history, f=lambda x: x["contact_id"] is not None
                                                                       and x["contact_id"] != ''
                                                                       and x["timecreated"] is not None
                                                                       and x["used_product_id"] is not None)

    if is_checking:
        dyf_tpe_enduser_used_product_history = Filter \
            .apply(frame=dyf_tpe_enduser_used_product_history,
                   f=lambda x: x["contact_id"] == contact_id_checking)

    if is_dev:
        print('dyf_tpe_enduser_used_product_history')
        dyf_tpe_enduser_used_product_history.printSchema()
        dyf_tpe_enduser_used_product_history.show(10)

    dyf_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.resolveChoice(
        specs=[('timecreated', 'cast:long'), ('id', 'cast:long')])

    df_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.toDF()

    # df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history \
    #     .orderBy(f.asc('contact_id'), f.asc('timecreated'), f.desc('id'))

    w2 = Window.partitionBy("contact_id", "timecreated").orderBy(f.col("id").desc())
    df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history \
        .withColumn("row", f.row_number().over(w2)) \
        .where(f.col('row') <= 1)

    df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.drop('row')

    df_tpe_enduser_used_product_history.cache()

    if is_dev:
        print('df_tpe_enduser_used_product_history_after_drop duplicate timecreated')
        df_tpe_enduser_used_product_history.printSchema()
        df_tpe_enduser_used_product_history.show(10)


    #-------------------------------------------------------------------------------------------------------------------#
    dyf_tpe_enduser_used_product = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_enduser_used_product',
        ['id', 'product_id']) \
        .rename_field('id', 'enduser_used_product_id')

    dyf_tpe_enduser_used_product = Filter.apply(frame=dyf_tpe_enduser_used_product,
                                                f=lambda x: x["product_id"] is not None and x[
                                                    "product_id"] != '')

    df_tpe_enduser_used_product = dyf_tpe_enduser_used_product.toDF()
    df_tpe_enduser_used_product = df_tpe_enduser_used_product.dropDuplicates(['enduser_used_product_id'])

    #-------------------------------------------------------------------------------------------------------------------#
    dyf_tpe_invoice_product_details = retrieve_dynamic_frame(
        glueContext,
        'tig_market',
        'tpe_invoice_product_details',
        ['id', 'cat_code']
    ).rename_field('cat_code', 'package_code')

    df_tpe_invoice_product_details = dyf_tpe_invoice_product_details.toDF()
    df_tpe_invoice_product_details = df_tpe_invoice_product_details.dropDuplicates(['id'])

    #------------------------------------------------------------------------------------------------------------------#

    df_market_product_history = df_tpe_enduser_used_product_history \
        .join(df_tpe_enduser_used_product,
              on=df_tpe_enduser_used_product_history.used_product_id == df_tpe_enduser_used_product.enduser_used_product_id,
              how='inner') \
        .join(df_tpe_invoice_product_details,
              df_tpe_enduser_used_product.product_id == df_tpe_invoice_product_details.id, 'inner')

    df_market_product_history = df_market_product_history\
        .select(
            'contact_id',
            f.col('timecreated').cast('long').alias('timecreated'),
            f.col('product_id').cast('long').alias('used_product_id'),
            'package_code',
            'status_new'
        )


    return df_market_product_history

def get_df_student_involved_study_origin(df_student_contact):
    dyf_student_involved_study_origin = retrieve_dynamic_frame(
        glueContext,
        'tig_advisor',
        'student_involved_study',
        ['id', 'contact_id', 'package_code', 'time_start', 'time_end', 'status', 'product_id', '_key'])

    dyf_student_involved_study_origin = dyf_student_involved_study_origin\
        .resolveChoice(specs=[('_key', 'cast:long')])

    if is_checking:
        dyf_student_involved_study_origin = Filter\
            .apply(frame=dyf_student_involved_study_origin,
                        f=lambda x: x["contact_id"] == contact_id_checking
                   )
        print('CHECKING::dyf_student_involved_study_origin')
        dyf_student_involved_study_origin.printSchema()
        dyf_student_involved_study_origin.show(3)


    dyf_student_involved_study_origin = Filter\
        .apply(frame=dyf_student_involved_study_origin,
                    f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                and x["package_code"] is not None and x["package_code"] != ''
               )


    if is_dev:
        print('CHECKING::dyf_student_involved_study_origin::after cleaning')
        dyf_student_involved_study_origin.printSchema()
        dyf_student_involved_study_origin.show(3)

    df_student_involved_study_origin = dyf_student_involved_study_origin.toDF()


    df_student_involved_study_origin = df_student_involved_study_origin\
        .join(df_student_contact,
              on=['contact_id', 'product_id'],
              how='inner')
    if is_dev:
        print ('get_df_student_involved_study_origin::df_student_involved_study_origin')
        df_student_involved_study_origin.printSchema()
        df_student_involved_study_origin.show(10)
    # root
    # | -- contact_id: string(nullable=true)
    # | -- product_id: integer(nullable=true)
    # | -- _key: long(nullable=true)
    # | -- package_code: string(nullable=true)
    # | -- time_start: integer(nullable=true)
    # | -- time_end: integer(nullable=true)
    # | -- status: string(nullable=true)
    # | -- id: integer(nullable=true)
    # | -- user_name: string(nullable=true)
    # | -- time_lms_created: integer(nullable=true)
    # | -- student_id: string(nullable=true)

    df_student_involved_study_origin = df_student_involved_study_origin\
        .select(
            'contact_id',
            'product_id',
            'package_code',
            'time_start',
            'time_end',
            'status',
            'id',
            '_key',
            'time_lms_created'
    )

    w3 = Window.partitionBy("contact_id").orderBy(f.col('id').desc(), f.col("_key").desc())
    df_student_involved_study_origin = df_student_involved_study_origin \
        .withColumn("row", f.row_number().over(w3)) \
        .where(f.col('row') <= 1)

    df_student_involved_study_origin = df_student_involved_study_origin.drop('row', '_key')

    return df_student_involved_study_origin

def get_advisor_student_package(df_student_involved_study_origin):

    df_student_package_active_suspend_expired = df_student_involved_study_origin\
        .filter((f.col('time_start').isNotNull())
                & (f.col('time_end').isNotNull())
                & ((f.col('status') == 'ACTIVED')
                     | (f.col('status') == 'SUSPENDED')
                     | (f.col('status') == 'EXPIRED')
                     )
                )

    # dyf_student_package_active_suspend_expired = Filter.apply(frame=dyf_student_involved_study_origin,
    #                                           f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
    #
    #                                                       and x["time_start"] is not None and x["time_end"] is not None
    #                                                       and x["status"] in ['ACTIVED', 'SUSPENDED', 'EXPIRED']
    #                                           )

    # df_student_package_active_suspend_expired = dyf_student_package_active_suspend_expired.toDF()
    number_package_active_suspend_expired = df_student_package_active_suspend_expired.count()
    if number_package_active_suspend_expired < 1:
        return None, None

    df_package_active = df_student_package_active_suspend_expired \
        .select(
        'contact_id',
        f.col('time_start').cast('long').alias('timecreated'),
        f.col('product_id').cast('long').alias('used_product_id'),
        'package_code',
        f.lit('ACTIVED').cast('string').alias('status_new')
    )

    if is_dev:
        print ('df_package_active')
        df_package_active.printSchema()
        df_package_active.show(10)

    df_package_expired = df_student_package_active_suspend_expired \
        .select(
        'contact_id',
        f.col('time_end').cast('long').alias('timecreated'),
        f.col('product_id').cast('long').alias('used_product_id'),
        'package_code',
        f.lit('EXPIRED').cast('string').alias('status_new')
    )

    if is_dev:
        print ('df_package_expired')
        df_package_expired.printSchema()
        df_package_expired.show(10)

    df_package_current = df_student_package_active_suspend_expired \
        .select(
        'contact_id',
        f.lit(yesterday_timestamp).cast('long').alias('timecreated'),
        f.col('product_id').cast('long').alias('used_product_id'),
        'package_code',
        f.col('status').cast('string').alias('status_new')
    )

    if is_dev:
        print ('df_package_current')
        df_package_current.printSchema()
        df_package_current.show(10)

    df_advisor_total = df_package_active\
        .union(df_package_expired)\
        .union(df_package_current)

    w3 = Window.partitionBy("contact_id", "product_id").orderBy(f.col("time_end").desc())
    df_final_expire = df_student_package_active_suspend_expired \
        .withColumn("row", f.row_number().over(w3)) \
        .where(f.col('row') <= 1)

    df_final_expire = df_final_expire.select(
        f.col('contact_id').alias('contact_id_final'),
        f.col('product_id').cast('long').alias('product_id_final'),
        f.col('time_end').cast('long').alias('expire_time_final')
    )


    return df_advisor_total, df_final_expire



def get_package_deactived_cancel(df_student_involved_study_origin):
    df_student_involved_study_cancel_deactivated = df_student_involved_study_origin \
        .filter((f.col('status') == 'DEACTIVED')
                    | (f.col('status') == 'CANCELLED')
                )

    # dyf_student_involved_study_cancel_deactivated = Filter.apply(frame=dyf_student_involved_study_origin,
    #                                           f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
    #                                                       and x["status"] in ['DEACTIVED', 'CANCELLED']
    #                                           )

    # df_student_involved_study_cancel_deactivated = dyf_student_involved_study_cancel_deactivated.toDF()

    if is_dev:
        print ('df_student_involved_study_cancel_deactivated__step1')
        df_student_involved_study_cancel_deactivated.printSchema()
        df_student_involved_study_cancel_deactivated.show(3)

    w4 = Window.partitionBy("contact_id").orderBy(f.col("id").desc())
    df_student_involved_study_cancel_deactivated = df_student_involved_study_cancel_deactivated \
        .withColumn("row", f.row_number().over(w4)) \
        .where(f.col('row') <= 1)

    if is_dev:
        print ('df_student_involved_study_cancel_deactivated__step2__get top')
        df_student_involved_study_cancel_deactivated.printSchema()
        df_student_involved_study_cancel_deactivated.show(3)

    number_student_involved_study_cancel_deactivated = df_student_involved_study_cancel_deactivated.count()
    if is_dev:
        print ('number_student_involved_study_cancel_deactivated: ', number_student_involved_study_cancel_deactivated)

    if number_student_involved_study_cancel_deactivated < 1:
        return

    if is_dev:
        print ('df_student_involved_study_cancel_deactivated_contact')
        df_student_involved_study_cancel_deactivated.printSchema()
        df_student_involved_study_cancel_deactivated.show(3)

    df_student_involved_study_cancel_deactivated = df_student_involved_study_cancel_deactivated \
        .select(
        'contact_id',
        f.col('time_lms_created').cast('long').alias('timecreated'),
        'package_code',
        f.col('status').cast('string').alias('status_new')
    )

    return df_student_involved_study_cancel_deactivated

def etl_student_package(glueContext):
    # output: 'contact_id', 'status_new', 'timecreated', 'used_product_id'
    df_student_contact = get_df_student_contact_v2(glueContext)

    df_student_contact.cache()
    # if is_dev:
    #     print ('df_student_contact')
    #     df_student_contact.printSchema()
    #     df_student_contact.show()


    #------------------------------------------------------------------------------------------------------------------#

    df_market_product_history = get_df_market_product_history(glueContext)
    if is_dev:
        print ('df_market_product_history')
        df_market_product_history.printSchema()
        df_market_product_history.show(10)

    # ------------------------------------------------------------------------------------------------------------------#
    df_student_involved_study_origin = get_df_student_involved_study_origin(df_student_contact)

    df_student_involved_study_origin.cache()
    if is_dev:
        print ('get_advisor_student_package::df_student_involved_study_origin')
        df_student_involved_study_origin.printSchema()
        df_student_involved_study_origin.show(10)



    # -------------------------------------------------------------------------------------------------------------------#

    df_advisor_total, df_final_expire = get_advisor_student_package(df_student_involved_study_origin)

    if is_dev and df_advisor_total is not None:
        print ('df_advisor_total')
        df_advisor_total.printSchema()
        df_advisor_total.show(10)

    if is_dev and df_final_expire is not None:
        print ('df_final_expire')
        df_final_expire.printSchema()
        df_final_expire.show(10)

    if df_advisor_total is not None:
        df_market_product_history = df_market_product_history\
            .union(df_advisor_total)

        if df_final_expire is not None:
            df_market_product_history = df_market_product_history \
                .join(df_final_expire,
                      on=(df_market_product_history.contact_id == df_final_expire.contact_id_final)
                         & (df_market_product_history.used_product_id == df_final_expire.product_id_final)
                         & (df_market_product_history.timecreated <= df_final_expire.expire_time_final),
                      how='inner'
                      )
        if is_dev:
            print ('df_market_product_history :: append from advisor')
            df_market_product_history.printSchema()
            df_market_product_history.show(10)

    df_market_product_history = df_market_product_history\
        .select(
            'contact_id',
            'timecreated',
            'package_code',
            'status_new'
        )

    if is_dev:
        print ('df_market_product_history with advisor::get package_code')
        df_market_product_history.printSchema()
        df_market_product_history.show(10)

    # root
    # | -- contact_id: string(nullable=true)
    # | -- timecreated: long(nullable=true)
    # | -- package_code: string(nullable=true)
    # | -- status_new: string(nullable=true)

    # -------------------------------------------------------------------------------------------------------------------#
    df_package_deactived_cancel = get_package_deactived_cancel(df_student_involved_study_origin)
    if is_dev:
        print ('df_package_deactived_cancel')
        df_package_deactived_cancel.printSchema()
        df_package_deactived_cancel.show(10)

    df_market_product_history = df_market_product_history\
        .union(df_package_deactived_cancel)

    df_market_product_history = df_market_product_history.na.fill({
            'timecreated': MIN_TIME_CREATED
        })

    # if is_dev:
    #     print ('df_market_product_history__all')
    #     df_market_product_history.printSchema()
    #     df_market_product_history.show(10)

    if is_checking_level_2:
        print('df_tpe_enduser_used_product_history_check___')
        df_tpe_enduser_used_product_history_check = df_market_product_history\
            .filter(df_market_product_history.contact_id == contact_id_checking)
        df_tpe_enduser_used_product_history_check.printSchema()
        df_tpe_enduser_used_product_history_check.show(50)

    # -------------------------------------------------------------------------------------------------------------------#

    df_market_product_history_struct = df_market_product_history\
        .select(
            'contact_id',
            'timecreated',
            f.struct('timecreated', 'package_code', 'status_new')
                .alias('time_product_status')
    )

    df_market_product_history_struct_group = df_market_product_history_struct \
        .groupBy('contact_id') \
        .agg(
        f.collect_list('time_product_status').alias('time_product_status_list')
    )

    if is_dev:
        print('df_market_product_history_struct_group')
        df_market_product_history_struct_group.printSchema()

    df_market_product_history_struct_group_result = df_market_product_history_struct_group.select(
        'contact_id',
        udf_map_end_time('time_product_status_list').alias('product_status_start_end_list')
    )

    df_market_product_history_struct_group_explore = df_market_product_history_struct_group_result \
        .select(
        'contact_id',
        f.explode('product_status_start_end_list').alias('product_status_start_end')
    )

    df_market_product_history_struct_group_explore = df_market_product_history_struct_group_explore \
        .select(
        'contact_id',
        df_market_product_history_struct_group_explore['product_status_start_end'][0].alias('package_code'),
        df_market_product_history_struct_group_explore['product_status_start_end'][1].alias('status_code'),
        df_market_product_history_struct_group_explore['product_status_start_end'][2].alias('start_time'),
        df_market_product_history_struct_group_explore['product_status_start_end'][3].alias('end_time')
    )

    # -------------------------------------------------------------------------------------------------------------------#
    df_result = df_market_product_history_struct_group_explore\
        .join(df_student_contact,
              on=['contact_id'],
              how='inner')

    df_result = df_result.where('user_name NOT LIKE \'' + TOPICA_EMAIL_END + "\'")


    df_result = df_result.na.fill({
        'start_time': str(MIN_DATE),
        'end_time': str(MAX_DATE)
    })

    if is_dev:
        print('df_result get detail')
        df_result.printSchema()
        df_result.show(30)

    # -------------------------------------------------------------------------------------------------------------------#
    df_result = df_result.withColumn('created_at', f.lit(today_id))
    dyf_result = DynamicFrame \
        .fromDF(df_result, glueContext, "dyf_result")

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
                                                                   "preactions": """TRUNCATE TABLE ad_student_package """,
                                                                   "dbtable": "ad_student_package",
                                                                   "database": "transaction_log"
                                                               },
                                                               redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package",
                                                               transformation_ctx="datasink4")

    # -------------------------------------------------------------------------------------------------------------------#

    df_student_contact.unpersist()
    df_student_involved_study_origin.unpersist()


def change_package_and_package_status(glueContext):
    if is_dev:
        print('start change_package_and_package_status-----------------------------------------')
    etl_student_package(glueContext)
    return


# -----------------------------------------------------------------------------------------------------------------------#
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")

ADVISOR_MAPPING = [("user_id", "int", "advisor_id", "bigint"), \
                   ("user_name", "string", "user_name", "string"), \
                   ("user_display_name", "string", "name", "string"), \
                   ("user_email", "string", "email", "string"), \
                   ("level", "int", "level", "int"), \
                   ("type_manager", "string", "advisor_type", "string")]

STUDENT_ADVISOR_MAPPING = [("id", "string", "student_advisor_id", "string"), \
                           ("contact_id", "string", "contact_id", "string"), \
                           ("advisor_id_new", "string", "advisor_id", "string"), \
                           ("start", "string", "start_date", "string"), \
                           ("end", "string", "end_date", "string"), \
                           ("created_at", "string", "created_at", "int"), \
                           ("updated_at", "string", "updated_at", "string")]

STUDENT_ADVISOR_ADD_COL = {'created_at': f.lit(d4),
                           'updated_at': f.lit('')}

start_end_date_struct = StructType([
    StructField("advisor_id", StringType()),
    StructField("start", LongType()),
    StructField("end", LongType())])


# ------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="", table_name="", select_fields=[], fillter=[], cache=False, duplicates=[]):
    if database == "" and table_name == "":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database,
                                                        table_name=table_name)

    if is_dev:
        print("full schema of table: ", database, "and table: ", table_name)
        dyf.printSchema()
    #     select felds
    if len(select_fields) != 0:
        dyf = dyf.select_fields(select_fields)

    if len(fillter) != 0:
        dyf = fillterOutNull(dyf, fillter)
    else:
        dyf = fillterOutNull(dyf, select_fields)

    if is_limit and cache:
        df = dyf.toDF()
        df = df.limit(500)
        df = checkDumplicate(df, duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit == False and cache:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit and cache == False:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        df = df.limit(500)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    else:
        df = dyf.toDF()
        df = checkDumplicate(df, duplicates)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)

    if is_dev:
        print("full schema of table: ", database, " and table: ", table_name, " after select")
        dyf.printSchema()
    return dyf


# ----------------------------------------------------------------------------------------------------------------------#

def print_is_dev(string):
    if is_dev:
        print string


# ----------------------------------------------------------------------------------------------------------------------#

def checkDumplicate(df, duplicates):
    if len(duplicates) == 0:
        return df.dropDuplicates()
    else:
        return df.dropDuplicates(duplicates)


# ----------------------------------------------------------------------------------------------------------------------#
def parquetToS3(dyf, path="default", format="parquet"):
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={
                                                     "path": path
                                                 },
                                                 format=format,
                                                 transformation_ctx="datasink6")


# -----------------------------------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != "")
    return dynamicFrame


# -----------------------------------------------------------------------------------------------------------------------#
def myFunc(data_list):
    for val in data_list:
        if val is not None and val != "":
            return val
    return None


# -----------------------------------------------------------------------------------------------------------------------#
UNAVALIABLE_ADVISOR_ID = -2L
unavaliable_item = Row("advisor_id", "start", "end")(UNAVALIABLE_ADVISOR_ID, MIN_DATE, MIN_DATE)


def transpose_start_end_date_function(data_list):
    len_data = len(data_list)

    if data_list is None or len_data == 0:
        return [unavaliable_item]
    if len_data == 1:
        start_data = data_list[0]
        return [Row("advisor_id", "start", "end")( start_data[0], start_data[1], MAX_DATE)]

    result = []

    data_list = sorted(data_list, key=lambda x: x[1])

    for i in range(0, (len_data - 1)):
        start_data = data_list[i]
        end_data = data_list[i + 1]
        start_end_row = Row("advisor_id", "start", "end")(start_data[0], start_data[1], end_data[1])
        result.append(start_end_row)
    final = data_list[len_data - 1]
    final_fix_end = Row("advisor_id", "start", "end")(
        final[0],
        final[1],
        MAX_DATE
    )
    result.append(final_fix_end)
    return result


transpose_start_end_udf = f.udf(transpose_start_end_date_function, ArrayType(start_end_date_struct))


# -----------------------------------------------------------------------------------------------------------------------#
def addCollum(df, add_col):
    if len(add_col) != 0:
        for k, v in add_col.items():
            df = df.withColumn(k, v)
    return df


# -----------------------------------------------------------------------------------------------------------------------#
def mappingForAll(dynamicFrame, mapping):
    df = dynamicFrame.toDF()
    df = df.dropDuplicates()
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
    print("-------------------------------------------------")
    dyf.printSchema()
    print(mapping)

    applymapping2 = ApplyMapping.apply(frame=dyf, mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_mapping


# -------------------------------------------------------Main-----------------------------------------------------------#


def etl_advisor():
    dyf_advisor_account = connectGlue(database="tig_advisor", table_name="advisor_account",
                                      select_fields=["user_id", "user_name", "user_display_name", "user_email", "level",
                                                     "type_manager"],
                                      fillter=["user_id", "type_manager"])

    dyf_advisor_account.show(10)
    # ------------------------------------------------------------------------------------------------------------------#
    number_advisor_account = dyf_advisor_account.count()

    if is_dev:
        print ("number_advisor_account: ", number_advisor_account)
    if number_advisor_account < 1:
        return
    print_is_dev(
        "etl_advisor_profile___****************************************************************************************")
    # ETL for profile
    myUdf = f.udf(myFunc, StringType())

    df_advisor_account = dyf_advisor_account.toDF()
    df_advisor_account = df_advisor_account.groupBy("user_id") \
        .agg(f.collect_list("user_name").alias("user_name"),
             f.collect_list("user_email").alias("user_email"),
             f.first("level").alias("level"),
             f.first("type_manager").alias("type_manager"),
             f.collect_list("user_display_name").alias("user_display_name")) \
        .withColumn("user_name", myUdf("user_name")) \
        .withColumn("user_email", myUdf("user_email")) \
        .withColumn("user_display_name", myUdf("user_display_name"))
    dyf_advisor_account = DynamicFrame.fromDF(df_advisor_account, glueContext, "dyf_advisor_account")
    dyf_advisor_account_mapping = mappingForAll(dyf_advisor_account, mapping=ADVISOR_MAPPING)

    dyf_advisor_account_mapping.printSchema()
    dyf_advisor_account_mapping.show(10)
    ########  save to s3######
    # parquetToS3(dyf_advisor_account_mapping, path="s3://dtsodin/student_behavior/advisor_thangvm/advisor_history")
    ############################################################
    # ------------------------------------------------------------------------------------------------------------------#


def get_df_change_advisor_new(df_student_advisor):
    df_student_advisor_new = df_student_advisor.select(
        'contact_id',
        df_student_advisor.advisor_id_new.alias('advisor_id'),
        'created_at'
    )
    return df_student_advisor_new


# root
# |-- created_at: string
# |-- updated_at: string
# |-- advisor_id_new: string
# |-- contact_id: string
# |-- id: string


def get_df_change_advisor_init(df_student_advisor):
    df_student_advisor_new = df_student_advisor.select(
        'contact_id',
        df_student_advisor.advisor_id_old.alias('advisor_id')
    )

    df_student_advisor_new = df_student_advisor_new.orderBy(f.asc('contact_id'), f.asc('created_at'))
    df_student_advisor_first = df_student_advisor_new.groupBy('contact_id').agg(
        f.first('advisor_id').alias('advisor_id'),
        f.lit(MIN_DATE).alias('created_at')
    )

    df_student_advisor_first = df_student_advisor_first \
        .filter(df_student_advisor_first.advisor_id.isNotNull())

    return df_student_advisor_first


def get_df_student_contact(glueContext):
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id', 'advisor_id', 'level_study'])

    # dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x['student_id'] is not None
                                                   and x['contact_id'] is not None and x['contact_id'] != ''
                                                   and x['advisor_id'] is not None and x['advisor_id'] != ''
                                                   and x['level_study'] is not None and x['level_study'] != ''
                                       )

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['contact_id'])

    return df_student_contact


def get_advisor_from_student_contact(glueContext, df_contact_list):
    df_student_contact = get_df_student_contact(glueContext)

    df_student_contact = df_student_contact.join(df_contact_list, 'contact_id', 'left_anti')
    print('get_current_level_student_contact')
    df_student_contact.printSchema()
    df_student_contact.show(3)

    df_student_contact_level = df_student_contact.select(
        'contact_id',
        'advisor_id',
        f.lit(MIN_DATE).alias('created_at')
    )

    return df_student_contact_level


def get_df_student_advisor(glueContext):
    dyf_student_advisor = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_change_assignment_advisor"
    )

    dyf_student_advisor = dyf_student_advisor.select_fields(
        ["id", "contact_id", "advisor_id_new", "created_at", "updated_at"])

    dyf_student_advisor = RenameField.apply(dyf_student_advisor, 'advisor_id_new', 'advisor_id')

    dyf_student_advisor = Filter.apply(frame=dyf_student_advisor,
                                       f=lambda x: x['contact_id'] is not None and x['contact_id'] != ''
                                                   and x['advisor_id'] is not None and x['advisor_id'] != ''
                                                   and x['created_at'] is not None and  x['created_at'] != ''
                                       )

    dyf_student_advisor = dyf_student_advisor.resolveChoice(specs=[('id', 'cast:long')])\
        .resolveChoice(specs=[('advisor_id', 'cast:long')])

    if is_dev:
        print ('dyf_student_advisor')
        dyf_student_advisor.printSchema()
        dyf_student_advisor.show(20)

    df_student_advisor = dyf_student_advisor.toDF()

    df_student_advisor = df_student_advisor \
        .withColumn('created_at_t',
                    f.unix_timestamp('created_at', format='yyyy-MM-dd HH:mm:ss').cast('long'))

    df_student_advisor = df_student_advisor.filter(df_student_advisor.created_at_t.isNotNull())

    df_student_advisor_cache = df_student_advisor.cache()

    if is_dev:
        print ('df_student_advisor_cache_after_order')
        df_student_advisor_cache.printSchema()
        df_student_advisor_cache.show(50)

    return df_student_advisor_cache


def etl_student_advisor(glueContext):
    df_change_advisor_total = get_df_student_advisor(glueContext)

    df_change_advisor_total.cache()

    number_student_advisor = df_change_advisor_total.count()
    if is_dev:
        print ("df_change_advisor_total: ", number_student_advisor)
        df_change_advisor_total.printSchema()
        df_change_advisor_total.show(3)

    if number_student_advisor < 1:
        return

    # ------------------------------------------------------------------------------------------------------------------#
    # process contact_id with advisor from log_change_assignment_advisor and student_contact
    if is_dev:
        print ('df_change_advisor_total')
        df_change_advisor_total.printSchema()
        df_change_advisor_total.show(3)

    # -------------------------------------------------------------------------------------------------------------------#
    df_change_advisor_total = df_change_advisor_total \
        .orderBy(f.asc('contact_id'), f.asc('created_at_t'), f.desc('id'))

    df_change_advisor_total = df_change_advisor_total.dropDuplicates(['contact_id', 'created_at_t'])

    if is_dev:
        print('df_change_advisor_total_after_order')
        df_change_advisor_total.printSchema()
        df_change_advisor_total.show(3)

    df_change_advisor_total = df_change_advisor_total.select("contact_id",
                                                             f.struct('advisor_id',
                                                                      'created_at_t')
                                                             .alias('end_start'))

    print('df_change_advisor_total')
    df_change_advisor_total.printSchema()
    df_change_advisor_total.show(3)

    df_change_advisor_group_contact = df_change_advisor_total.groupBy("contact_id") \
        .agg(f.collect_list("end_start").alias("l_created_at"))

    print('df_change_advisor_group_contact_______________')
    df_change_advisor_group_contact.printSchema()
    df_change_advisor_group_contact.show(3)

    df_change_advisor_group_contact = df_change_advisor_group_contact \
        .withColumn("l_created_at", transpose_start_end_udf("l_created_at"))

    df_change_advisor_group_contact.printSchema()
    df_change_advisor_explore = df_change_advisor_group_contact \
        .select("contact_id",
                f.explode("l_created_at").alias("start_end_transpose")
                )

    df_change_advisor_explore.printSchema()
    df_change_advisor_explore = df_change_advisor_explore.select(
        "contact_id",
        f.col("start_end_transpose").getItem("advisor_id").alias("advisor_id"),
        f.col("start_end_transpose").getItem("start").alias("start"),
        f.col("start_end_transpose").getItem("end").alias("end"),
        f.lit(d4).alias('created_at'),
        f.lit('').alias('updated_at'),
    )

    df_change_advisor_explore = df_change_advisor_explore\
        .filter(df_change_advisor_explore.advisor_id != UNAVALIABLE_ADVISOR_ID)

    df_change_advisor_explore.printSchema()
    dyf_change_advisor_explore = DynamicFrame.fromDF(df_change_advisor_explore, glueContext,
                                                     "dyf_change_advisor_explore")
    # dyf_change_advisor_explore = mappingForAll(dyf_change_advisor_explore, mapping = STUDENT_ADVISOR_MAPPING)

    print('dyf_change_advisor_explore')
    dyf_change_advisor_explore.printSchema()
    dyf_change_advisor_explore.show(10)

    apply_ouput = ApplyMapping.apply(frame=dyf_change_advisor_explore,
                                     mappings=[("contact_id", "string", "contact_id", "string"),
                                               ("advisor_id", "string", "advisor_id", "long"),
                                               ("start", "long", "start_date", "long"),
                                               ("end", "long", "end_date", "long"),
                                               ("created_at", "string", "created_at", "long")
                                               ])
    #
    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "preactions": """TRUNCATE TABLE ad_student_advisor """,
                                                       "dbtable": "ad_student_advisor",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package",
                                                   transformation_ctx="datasink4")

    df_change_advisor_total.unpersist()


def change_advisor(glueContext):
    if is_dev:
        print('start change_advisor-----------------------------------------')
    etl_student_advisor(glueContext)
    if is_dev:
        print('end change_advisor-----------------------------------------')
    return


# -----------------------------------------------------------------------------------------------------------------------

def main():
    ########## dyf_student_contact
    change_level(glueContext)
    print('finish change_level')
    change_advisor(glueContext)
    print('change_advisor')
    change_package_and_package_status(glueContext)
    print('change_package_and_package_status')

if __name__ == "__main__":
    main()
