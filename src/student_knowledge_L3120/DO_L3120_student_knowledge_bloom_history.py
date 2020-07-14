import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms.dynamicframe_filter import Filter
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField
from datetime import date, datetime, timedelta
import pytz


## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')


IS_DEV = True
PERIOD_DAILY = 1
PERIOD_WEEKLY = 2

period = PERIOD_WEEKLY

def get_date_list(start_date):
    if period == PERIOD_WEEKLY:
        end_date = start_date + timedelta(6 - start_date.weekday())
    else:
        end_date = start_date + timedelta(1)
    date_id_list = []
    date_focus = start_date
    while (date_focus < end_date):
        date_id_list.append(int(date_focus.strftime("%Y%m%d")))
        date_focus += timedelta(1)
    return date_id_list

udf_get_date_list = f.udf(get_date_list, ArrayType(LongType()))


def get_df_boom_point_weekly(start_date, df_boom_point):
    end_date = start_date + timedelta(7)
    start_date_id = int(start_date.strftime("%Y%m%d"))
    end_date_id = int(end_date.strftime("%Y%m%d"))

    print('start_date_id: ' + str(start_date_id))
    print('end_date_id: ' + str(end_date_id))

    date_id_list = []
    date_focus = start_date
    while (date_focus < end_date):
        date_id_list.append(int(date_focus.strftime("%Y%m%d")))
        date_focus += timedelta(1)

    print('date_id_list')
    print(date_id_list)

    if IS_DEV:
        print('df_boom_point')
        df_boom_point.printSchema()
        df_boom_point.show(3)

    df_boom_point_weekly = df_boom_point.withColumn('date_id_list', udf_get_date_list(f.lit(start_date)))
    if IS_DEV:
        print('df_boom_point_weekly')
        df_boom_point_weekly.printSchema()
        df_boom_point_weekly.show(3)

    df_boom_point_weekly = df_boom_point_weekly.select(
        'student_id',
        'bloom_point',
        f.explode('date_id_list').alias('created_date_id')
    )

    return df_boom_point_weekly



def main():
    today = datetime.now(ho_chi_minh_timezone)
    print('today: ', today)
    yesterday = today - timedelta(1)
    print('yesterday: ', yesterday)
    today_id = int(today.strftime("%Y%m%d"))
    yesterday_id = int(yesterday.strftime("%Y%m%d"))
    print('today_id: ', today_id)
    print('yesterday_id: ', yesterday_id)

    def totalScore(total_knowledge, total_comprehension, total_application,
                   total_analysis, total_synthesis, total_evaluation):
        return (total_knowledge + total_comprehension + total_application
                + total_analysis + total_synthesis + total_evaluation)

    totalScore = udf(totalScore, LongType())

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
        # try:
        #     dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog(
        #         database="nvn_knowledge",
        #         table_name="mapping_lo_student"
        #     )
        # except:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "mapping_lo_student",
                "redshiftTmpDir": "s3://dts-odin/temp1/dyf_mapping_lo_student_current/v9"}
        )


    # print('mapping_lo_student_current')
    # dyf_mapping_lo_student_current.printSchema()
    # dyf_mapping_lo_student_current.show(3)
    # #
    # # # Filter all

    # dyf_mapping_lo_student_current = Filter.apply(frame=dyf_mapping_lo_student_current,
    #                                               f=lambda x: x["student_id"] is not None and x["student_id"] != 0)

    print('mapping_lo_student_current')
    dyf_mapping_lo_student_current.printSchema()
    dyf_mapping_lo_student_current.show(3)

    dy_mapping_lo_student_current = dyf_mapping_lo_student_current.toDF()
    # dy_mapping_lo_student_current.cache()

    df_boom_point = dy_mapping_lo_student_current.groupBy('student_id').agg(
        totalScore(f.sum('knowledge'), f.sum('comprehension'), f.sum('application'),
                   f.sum('analysis'), f.sum('synthesis'), f.sum('evaluation')).alias('bloom_point')
    )

    # df_boom_point = df_boom_point.withColumn('created_date_id', f.lit(yesterday_id))
    df_boom_point = get_df_boom_point_weekly(yesterday, df_boom_point)

    if IS_DEV:
        print('df_boom_point_week')
        df_boom_point.printSchema()
        df_boom_point.show(3)

    dyf_boom_point = DynamicFrame.fromDF(df_boom_point, glueContext, 'dyf_boom_point')

    apply_ouput = ApplyMapping.apply(frame=dyf_boom_point,
                                     mappings=[("student_id", "long", "student_id", "long"),
                                               ("bloom_point", "long", "bloom_point", "long"),
                                               ("created_date_id", "long", "created_date_id", "long")
                                               ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    clear_before_saving = 'DELETE student_knowledge_bloom_history where created_date_id >= ' + str(yesterday_id)
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": clear_before_saving,
                                                                   "dbtable": "student_knowledge_bloom_history",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/student_knowledge_bloom_history",
                                                               transformation_ctx="datasink4")


if __name__ == "__main__":
    main()



# job.commit()