import sys
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
import boto3

student_id_unavailable = 0L
package_endtime_unavailable = 99999999999L
package_starttime_unavailable = 0L


def do_check_endtime(val):
    if val is not None:
        return val
    return 4094592235


check_endtime_null = udf(do_check_endtime, LongType())

is_dev = True

UNAVAILABLE = 'unavailable'
START = -1
END = -1

MIN_DATE = 946663200
MAX_DATE = "99999999999"

LevelStartEnd = StructType([
    StructField("level", StringType(), False),
    StructField("start", StringType(), False),
    StructField("end", StringType(), False),
])


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


def get_df_student_contact(glueContext):
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id', 'level_study'])

    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x['student_id'] is not None
                                                   and x['contact_id'] is not None
                                                   and x['level_study'] is not None and x['level_study'] != '')

    df_student_contact = dyf_student_contact.toDF()
    return df_student_contact


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


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    ########## dyf_student_contact

    dyf_student_level = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_student_level_study"
    )

    dyf_student_level = dyf_student_level.select_fields(
        ['_key', 'id', 'contact_id', 'level_current', 'level_modified', 'time_created']) \
        .rename_field('time_created', 'time_level_created')

    df_student_level = dyf_student_level.toDF()
    df_student_level = df_student_level.dropDuplicates(['id'])
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
        .union(df_change_level_init)\
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

    change_level_total = change_level_total.select(
        'contact_id',
        f.struct(df_student_level.time_level_created.cast('string'), 'level').alias("s_time_level")
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
        f.col("level_start_end").getItem("end").alias("end").cast('long')

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
                                                ("end", "long", "end_date", "long")])

    resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols", transformation_ctx="resolvechoice2")

    dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "postactions": """TRUNCATE TABLE ad_student_level """,
                                                       "dbtable": "ad_student_level",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level",
                                                   transformation_ctx="datasink4")


if __name__ == "__main__":
    main()
