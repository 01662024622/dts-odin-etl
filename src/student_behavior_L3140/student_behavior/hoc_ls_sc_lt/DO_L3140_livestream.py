import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import datetime
from pyspark.sql.functions import lit, md5, udf, expr, col, from_unixtime, min, max, sum, count
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, LongType
from pyspark.storagelevel import StorageLevel
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glue_context = GlueContext(sc)
glueContext = glue_context
spark = glue_context.spark_session
# job = Job(glue_context)
# job.init(args['JOB_NAME'], args)


flag_file = 's3://toxd-olap/transaction_log/flag/flag_student_behavior_livestream.parquet'

student_behavior_s3_path = "s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior/"
student_behavior_s3_partition = ["behavior_id", "year_month_id"]

student_learning_s3_path = "s3://toxd-olap/transaction_log/student_behavior/sb_student_learning/"
student_learning_s3_partition = ["behavior_id", "year_month_id"]

redshift_database = "transaction_log"
redshift_table_student_behavior = "sb_student_behavior"
redshift_tmp_dir_student_behavior = "s3://datashine-dev-redshift-backup/translation_log/student_behavior/sb_student_behavior"
redshift_table_student_learning = "sb_student_learning"
redshift_tmp_dir_student_learning = "s3://datashine-dev-redshift-backup/translation_log/student_behavior/sb_student_learning"
transformation_ctx = "datasink4"

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'

IS_DEV = True


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame.toDF()


def retrieve_data_frame_from_redshift(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/" + database,
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": table_name,
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )

    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame.toDF()


def save_flag(data_frame, flag_path):
    data_frame.write.parquet(flag_path, mode="overwrite")


def get_flag(spark, data_frame):
    flag = data_frame.agg({"_key": "max"}).collect()[0][0]
    flag_data = [flag]
    return spark.createDataFrame(flag_data, "string").toDF('flag')


def filter_latest(spark, data_frame, config_file):
    try:
        df_flag = spark.read.parquet(config_file)
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ' +  str(start_read))

        result = data_frame.where(col('_key') > start_read)
        return result
    except:
        print('read flag file error ')
        return data_frame


def data_frame_filter_not_null(data_frame, fields=[]):
    for i in fields:
        data_frame = data_frame.where(col(i).isNotNull())

    return data_frame


def display(data_frame, message):
    if IS_DEV == True:
        data_frame.printSchema()
        data_frame.show(10)


def from_data_frame(data_frame, glue_context, name):
    return DynamicFrame.fromDF(data_frame, glue_context, name)


def concat_text(student_behavior_date, behavior_id, student_id, contact_id,
                package_code,
                student_level_code, package_status_code, transformed_at):
    text_concat = ""
    if student_behavior_date is not None:
        text_concat += str(student_behavior_date)
    if behavior_id is not None:
        text_concat += str(behavior_id)
    if student_id is not None:
        text_concat += str(student_id)
    if contact_id is not None:
        text_concat += str(contact_id)
    if package_code is not None:
        text_concat += str(package_code)
    if student_level_code is not None:
        text_concat += str(student_level_code)
    if package_status_code is not None:
        text_concat += str(package_status_code)
    if transformed_at is not None:
        text_concat += str(transformed_at)
    return text_concat


def save_data_to_s3(glue_context, frame, path, partition_keys=[]):
    glue_context.write_dynamic_frame.from_options(frame=frame, connection_type="s3",
                                                  connection_options={
                                                      "path": path,
                                                      "partitionKeys": partition_keys},
                                                  format="parquet")


def save_data_to_redshift(glue_context, dynamic_frame, database, table, redshift_tmp_dir, transformation_ctx):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir=redshift_tmp_dir,
                                                    transformation_ctx=transformation_ctx)


def round_duration(time):
    if time > 2160:
        return 2160
    return time

concat_text_udf = udf(concat_text, StringType())
round_duration_udf = udf(round_duration, LongType())
def main():
    # ========== init

    # =========== create_dynamic_frame
    df_log_in_out = retrieve_dynamic_frame(glue_context, 'native_livestream', 'log_in_out',
                                           casts=[('time_in', 'cast:long'),
                                                  ('time_out', 'cast:long'),
                                                  ('thoigianhoc', 'cast:long')])

    if IS_DEV:
        print('df_log_in_out')
        df_log_in_out.printSchema()
        df_log_in_out.show(3)



    # display(data_frame=df_log_in_out, message="dyf_log_in_out")

    # ========== clear data
    # df_log_in_out = filter_latest(spark=spark, data_frame=df_log_in_out, config_file=flag_file)

    df_log_in_out_origin = df_log_in_out

    if df_log_in_out.count() < 0:
        return

    df_log_in_out = df_log_in_out.dropDuplicates(['student_id', 'room_id', 'time_in'])
    df_log_in_out = df_log_in_out.groupBy('student_id', 'room_id').agg(
        min('time_in').alias('time_in'),
        max('time_out').alias('time_out'),
        sum('thoigianhoc').alias('thoigianhoc'),
        count('time_in').cast('long').alias('number_in_out')
    )

    if IS_DEV:
        print('df_log_in_out_after_group_by_room')
        df_log_in_out.printSchema()
        df_log_in_out.show(3)

    df_log_in_out = df_log_in_out \
        .withColumn('student_behavior_date', df_log_in_out['time_in']) \
        .withColumn('end_learning_time', df_log_in_out['time_out']) \
        .withColumn('duration', df_log_in_out['thoigianhoc'])

    df_log_in_out = df_log_in_out.withColumn('student_behavior_date',
                                             expr("student_behavior_date div 1000"))
    df_log_in_out = df_log_in_out.withColumn('start_learning_time',
                                             df_log_in_out['student_behavior_date'])
    df_log_in_out = df_log_in_out.withColumn('end_learning_time',
                                             expr("end_learning_time div 1000"))
    df_log_in_out = df_log_in_out.withColumn('duration',
                                             round_duration_udf(expr("duration div 1000")))

    df_log_in_out = data_frame_filter_not_null(df_log_in_out,
                                               ['student_behavior_date', 'student_id', 'start_learning_time',
                                                'end_learning_time', 'duration'])

    display(data_frame=df_log_in_out, message="dyf_log_in_out clear data")

    # =========== create_dynamic_frame
    df_streaming_calendar_teach = retrieve_dynamic_frame(glue_context, 'topicalms', 'streaming_calendar_teach',
                                                         ['id', 'type_class', 'teacher_available_id', 'assistant_id',
                                                          'hour_id'], [('room_id', 'cast:int')])
    # dyf_streaming_material = retrieve_dynamic_frame(glue_context, 'topicalms', 'streaming_material',
    #                                                 ['calendar_teach_id', 'subject'])
    df_student_contact = retrieve_dynamic_frame(glue_context, 'tig_advisor', 'student_contact',
                                                ['contact_id', 'student_id'])

    df_student_level = retrieve_data_frame_from_redshift(
        glue_context,
        'transaction_log',
        'ad_student_level',
        ['contact_id', 'level_code', 'start_date', 'end_date']
    )
    df_student_level = df_student_level.withColumnRenamed('contact_id', 'contact_id_level')
    display(df_student_level, "df_student_level")

    df_student_package = retrieve_data_frame_from_redshift(
        glue_context,
        'transaction_log',
        'ad_student_package',
        ['contact_id', 'package_code', 'package_status_code', 'package_start_time', 'package_end_time']
    )
    df_student_package = df_student_package.withColumnRenamed('contact_id', 'contact_id_package')
    display(df_student_package, "df_student_package")

    df_student_advisor = retrieve_data_frame_from_redshift(
        glue_context,
        'transaction_log',
        'ad_student_advisor',
        ['contact_id', 'advisor_id', 'start_date', 'end_date']
    )
    df_student_advisor = df_student_advisor\
        .withColumnRenamed('contact_id', 'contact_id_advisor')\
        .withColumnRenamed('start_date', 'start_date_advisor') \
        .withColumnRenamed('end_date', 'end_date_advisor')

    display(df_student_advisor, "df_student_advisor")

    # ============ join student_contact table
    df_result = df_log_in_out.join(df_student_contact,
                                   on=['student_id'],
                                   how='left_outer')

    df_result = data_frame_filter_not_null(df_result,
                                           ['contact_id'])

    display(df_result, "join student_contact table")

    # ============ join streaming_calendar_teach table
    df_result = df_result.join(
        df_streaming_calendar_teach,
        df_result.room_id == df_streaming_calendar_teach.id,
        how='left_outer'
    )
    df_result = df_result.withColumnRenamed('teacher_available_id', 'teacher_id')
    df_result = df_result.withColumnRenamed('type_class', 'class_type')
    df_result = df_result \
        .where(col('class_type').eqNullSafe('NORMAL')) \
        .withColumn('class_type', lit('LIVESTREAM').cast('string'))

    df_result = data_frame_filter_not_null(df_result,
                                           ['room_id', 'class_type', 'hour_id'])

    display(df_result, "join streaming_calendar_teach table")

    df_result = df_result \
        .join(df_student_level,
              (df_result.contact_id == df_student_level.contact_id_level)
              & (df_result.student_behavior_date >= df_student_level.start_date)
              & (df_result.student_behavior_date < df_student_level.end_date),
              'left'
              ) \
        .join(df_student_package,
              (df_result.contact_id == df_student_package.contact_id_package)
              & (df_result.student_behavior_date >= df_student_package.package_start_time)
              & (df_result.student_behavior_date < df_student_package.package_end_time),
              'left'
              ) \
        .join(df_student_advisor,
              (df_result.contact_id == df_student_advisor.contact_id_advisor)
              & (df_result.student_behavior_date >= df_student_advisor.start_date_advisor)
              & (df_result.student_behavior_date < df_student_advisor.end_date_advisor),
              'left'
              )

    display(df_result, "join df_student_level, df_student_package, df_student_advisor table")

    # ============ add column
    df_result = df_result \
        .withColumn('behavior_id', lit(13).cast('long')) \
        .withColumnRenamed('level_code', 'student_level_code') \
        .withColumn('transformed_at', lit(datetime.now()).cast("long")) \
        .withColumn('platform', lit('MOBILE').cast('string')) \
        .withColumn('teacher_type', lit(None).cast('string')) \
        .withColumn('year_month_id',
                    lit(from_unixtime(df_result['student_behavior_date'], format="yyyyMM")).cast('long')) \
        .withColumn('role_in_class', lit('UNAVAILABLE')) \
        .withColumn('vcr_type', lit('UNAVAILABLE'))

    contact_id_unavailable = 0
    student_id_unavailable = 0
    package_endtime_unavailable = 99999999999
    package_starttime_unavailable = 0
    student_level_code_unavailable = 'UNAVAILABLE'
    student_status_code_unavailable = 'UNAVAILABLE'
    package_code_unavailable = 'UNAVAILABLE'
    class_type_unavailable = 'UNAVAILABLE'
    teacher_type_unavailable = 'UNAVAILABLE'
    advisor_unavailable = 0
    measure1_unavailable = float(0.0)
    measure2_unavailable = float(0.0)
    measure3_unavailable = float(0.0)
    measure4_unavailable = float(0.0)
    role_in_class_unavailable = 'UNAVAILABLE'
    number_in_out_unavailable = 1

    df_result = df_result.na.fill({
        'package_code': package_code_unavailable,

        'student_level_code': student_level_code_unavailable,
        'package_status_code': student_status_code_unavailable,
        'class_type': class_type_unavailable,

        'teacher_type': teacher_type_unavailable,

        'advisor_id': advisor_unavailable,

        'role_in_class': role_in_class_unavailable,

        'number_in_out': number_in_out_unavailable
    })

    df_result = df_result.dropDuplicates()

    df_result = df_result.withColumn('student_behavior_id',
                                     md5(concat_text_udf(
                                         df_result.student_behavior_date,
                                         df_result.behavior_id,
                                         df_result.student_id,
                                         df_result.contact_id,
                                         df_result.package_code,
                                         df_result.student_level_code,
                                         df_result.package_status_code,
                                         df_result.transformed_at)))

    df_result.persist(StorageLevel.DISK_ONLY_2)

    display(df_result, "df_result add column")

    # ============ select student_behavior
    dyf_result = from_data_frame(data_frame=df_result, glue_context=glue_context, name='dyf_result')
    dyf_student_behavior = dyf_result.select_fields([
        'student_behavior_id',
        'student_behavior_date',
        'behavior_id',
        'student_id',
        'contact_id',
        'package_code',
        'student_level_code',
        'package_status_code',
        'advisor_id',
        'transformed_at',
        'year_month_id'
    ])

    dyf_student_behavior = dyf_student_behavior.resolveChoice([
        ('student_behavior_id', 'cast:string'),
        ('student_behavior_date', 'cast:long'),
        ('behavior_id', 'cast:int'),
        ('student_id', 'cast:long'),
        ('contact_id', 'cast:string'),
        ('package_code', 'cast:string'),
        ('student_level_code', 'cast:string'),
        ('package_status_code', 'cast:string'),
        ('advisor_id', 'cast:long'),
        ('transformed_at', 'cast:long'),
        ('year_month_id', 'cast:long')
    ])

    # -----------------------------------------------------------------------------------------------------------------#
    dyf_student_learning = dyf_result.select_fields([
        'student_behavior_id',
        'class_type',
        'platform',
        'teacher_id',
        'teacher_type',
        'assistant_id',
        'hour_id',
        'start_learning_time',
        'end_learning_time',
        'duration',
        'behavior_id',
        'year_month_id',
        'role_in_class',
        'number_in_out',
        'vcr_type'
    ])

    dyf_student_learning = dyf_student_learning.resolveChoice([
        ('student_behavior_id', 'cast:string'),
        ('class_type', 'cast:string'),
        ('platform', 'cast:string'),
        ('teacher_id', 'cast:long'),
        ('teacher_type', 'cast:string'),
        ('assistant_id', 'cast:long'),
        ('hour_id', 'cast:int'),
        ('start_learning_time', 'cast:long'),
        ('end_learning_time', 'cast:long'),
        ('duration', 'cast:long'),
        ('behavior_id', 'cast:int'),
        ('year_month_id', 'cast:long'),
        ('role_in_class', 'cast:string'),
        ('number_in_out', 'cast:long'),
        ('vcr_type', 'cast:string')
    ])

    # -----------------------------------------------------------------------------------------------------------------#
    df_flag = get_flag(spark=spark, data_frame=df_log_in_out_origin)

    # ============ save
    display(dyf_student_behavior, 'dyf_student_behavior')
    display(dyf_student_learning, 'dyf_student_learning')
    display(df_flag, "df_flag")

    save_data_to_s3(glue_context, dyf_student_behavior, student_behavior_s3_path, student_behavior_s3_partition)
    save_data_to_s3(glue_context, dyf_student_learning, student_learning_s3_path, student_learning_s3_partition)

    if df_flag.collect()[0]['flag'] is not None:
        print('save_flag done')
        save_flag(df_flag, flag_file)

    df_result.unpersist()

if __name__ == "__main__":
    main()

# job.commit()
