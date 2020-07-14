import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
import pyspark.sql.functions as f
from pyspark.sql import Window
from datetime import date, datetime, timedelta
import pytz

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')

today = datetime.now(ho_chi_minh_timezone)
print('today: ', today)
yesterday = today - timedelta(1)
print('yesterday: ', yesterday)
today_id = int(today.strftime("%Y%m%d"))
yesterday_id = int(yesterday.strftime("%Y%m%d"))
print('today_id: ', today_id)
print('yesterday_id: ', yesterday_id)

IS_DEV = False
IS_DEFAULT = False
IS_LOAD_FULL = False
IS_LOG = True
PERIOD_DAILY = 1
PERIOD_WEEKLY = 2
period = PERIOD_WEEKLY


def get_start_from_flag():
    if IS_LOAD_FULL:
        return date(2019, 9, 29)
    if IS_DEFAULT:
        return yesterday
    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_knowledge/student_knowledge_number_history.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        start = datetime.strptime(str(read_from_index), '%Y%m%d')
        return start
    except:
        print('read flag file error ')
        return yesterday


def main():
    start = get_start_from_flag()
    end = today

    if IS_DEV:
        print('start: ')
        print(start)
        print(start.strftime("%Y%m%d"))
        print('end: ')
        print(end)
        print(end.strftime("%Y%m%d"))

    date_ = start
    i = 0
    date_generated = []
    end_id = int(end.strftime("%Y%m%d"))
    while int(date_.strftime("%Y%m%d")) < int(end_id):
        date_id = int(date_.strftime("%Y%m%d"))
        date_generated.append(date_id)
        date_ = date_ + timedelta(days=1)

    print('date_generated')
    print(date_generated)

    if len(date_generated) < 1:
        return

    if IS_DEV:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "thanhtv3_dev_mapping_lo_student",
                "redshiftTmpDir": "s3://dts-odin/temp1/thanhtv3_dev_mapping_lo_student/v9"}
        )
    else:
        try:
            dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog(
                database="nvn_knowledge",
                table_name="mapping_lo_student"
            )
        except:
            dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options={
                    "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                    "user": "dtsodin",
                    "password": "DWHDtsodin@123",
                    "dbtable": "mapping_lo_student",
                    "redshiftTmpDir": "s3://dts-odin/temp1/dyf_mapping_lo_student_current/v9"}
            )

    dyf_mapping_lo_student_current = dyf_mapping_lo_student_current.resolveChoice(specs=[('backup_date', 'cast:long')])

    dyf_mapping_lo_student_current = Filter.apply(frame=dyf_mapping_lo_student_current,
                                                  f=lambda x: x["student_id"] is not None and x["student_id"] != 0
                                                              and x["knowledge_pass_date_id"] is not None)

    dy_mapping_lo_student_current = dyf_mapping_lo_student_current.toDF()
    dy_mapping_lo_student_current.cache()

    number_original_rows = dy_mapping_lo_student_current.count()
    print('number_original_rows: ', number_original_rows)
    if number_original_rows < 1:
        return

    dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )

    df_learning_object = dyf_learning_object.toDF()

    dy_mapping_lo_student_current_for_number = dy_mapping_lo_student_current.join(df_learning_object,
                                                                                  (dy_mapping_lo_student_current[
                                                                                       'learning_object_id'] ==
                                                                                   df_learning_object[
                                                                                       'learning_object_id']),
                                                                                  'left')

    if IS_LOG:
        print('dy_mapping_lo_student_current_for_number')
        dy_mapping_lo_student_current_for_number.printSchema()
        dy_mapping_lo_student_current_for_number.show(3)

    if dy_mapping_lo_student_current_for_number.count() < 1:
        return

    dy_mapping_lo_student_current_for_number = dy_mapping_lo_student_current_for_number \
        .groupBy('student_id', 'knowledge_pass_date_id').agg(
        f.count(when((f.col('learning_object_type') == 'vocabulary'), True)).alias('vocabulary_pass'),
        f.count(when((f.col('learning_object_type') == 'grammar'), True)).alias('grammar_pass'),
        f.count(when((f.col('learning_object_type') == 'phonetic'), True)).alias('phonetic_pass')
    )

    dy_mapping_lo_student_current_for_number = dy_mapping_lo_student_current_for_number\
        .withColumnRenamed('knowledge_pass_date_id', 'created_date_id')


    dy_mapping_lo_student_current_for_number.cache()

    dy_student = dy_mapping_lo_student_current_for_number.select('student_id').dropDuplicates(['student_id'])
    # dy_student = dy_student.withColumn('date_generated', f.lit(date_generated))
    # dy_student_date = dy_student.select('student_id', f.explode('date_generated').alias('date_id'))

    # print('dy_student')
    # dy_student.printSchema()
    # dy_student.show(10)

    df_date = spark.createDataFrame(date_generated, "int")

    # print('df_date')
    # df_date.printSchema()
    # df_date.show(10)

    dy_student_date = dy_student.crossJoin(df_date.select("value"))

    dy_student_date.cache()

    dy_student_date = dy_student_date.select(dy_student_date.value.alias('date_id'),
                                             dy_student_date.student_id.alias('student_id_date'),
                                             )

    # print('dy_student_date')
    # dy_student_date.printSchema()
    # dy_student_date.show(100)

    dy_mapping_lo_student_number_date = dy_mapping_lo_student_current_for_number \
        .join(dy_student_date,
              (dy_mapping_lo_student_current_for_number.student_id == dy_student_date.student_id_date)
              & (dy_mapping_lo_student_current_for_number.created_date_id == dy_student_date.date_id),
              'right')

    print('dy_mapping_lo_student_number_date')
    dy_mapping_lo_student_number_date.printSchema()
    dy_mapping_lo_student_number_date.show(100)

    dy_mapping_lo_student_number_date = dy_mapping_lo_student_number_date.select(
        'student_id_date',
        'date_id',
        'vocabulary_pass',
        'grammar_pass',
        'phonetic_pass'
    ).na.fill(0)

    dy_mapping_lo_student_number_date = dy_mapping_lo_student_number_date.orderBy('student_id_date', 'date_id')

    # print ('dy_mapping_lo_student_number_date')
    # dy_mapping_lo_student_number_date.printSchema()
    # dy_mapping_lo_student_number_date.show(300)

    window_cum = (Window.partitionBy('student_id_date').orderBy('date_id')
                  .rangeBetween(Window.unboundedPreceding, 0))

    dy_mapping_lo_student_number_date_cum = dy_mapping_lo_student_number_date.select(
        dy_mapping_lo_student_number_date.student_id_date.alias('student_id'),
        'date_id',
        f.sum('vocabulary_pass').over(window_cum).alias('cum_vocabulary'),
        f.sum('grammar_pass').over(window_cum).alias('cum_grammar'),
        f.sum('phonetic_pass').over(window_cum).alias('cum_phonetic')
    )

    print('dy_mapping_lo_student_number_date_cum')
    dy_mapping_lo_student_number_date_cum.printSchema()
    dy_mapping_lo_student_number_date_cum.show(300)

    dyf_mapping_lo_student_number_date_cum = DynamicFrame \
        .fromDF(dy_mapping_lo_student_number_date_cum, glueContext, 'dyf_mapping_lo_student_number_date_cum')

    apply_ouput = ApplyMapping.apply(frame=dyf_mapping_lo_student_number_date_cum,
                                     mappings=[("student_id", "long", "student_id", "long"),
                                               ("date_id", "int", "created_date_id", "long"),
                                               ("cum_vocabulary", "long", "vocabulary_number", "long"),
                                               ("cum_grammar", "long", "grammar_number", "long"),
                                               ("cum_phonetic", "long", "phonetic_number", "long")
                                               ])
    #
    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")
    clear_before_saving = 'DELETE student_knowledge_number_history where created_date_id >= ' + str(
        start.strftime("%Y%m%d"))
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": clear_before_saving,
                                                                   "dbtable": "student_knowledge_number_history",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/knowledge/student_knowledge_number_history",
                                                               transformation_ctx="datasink4")

    # save_flag
    flag_data = [today_id]
    df = spark.createDataFrame(flag_data, "long").toDF('flag')
    df.write.parquet("s3a://dts-odin/flag/student_knowledge/student_knowledge_number_history.parquet",
                     mode="overwrite")

    dy_student_date.unpersist()
    dy_mapping_lo_student_current_for_number.unpersist()


if __name__ == "__main__":
    main()

# job.commit()