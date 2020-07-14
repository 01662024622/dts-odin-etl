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
from datetime import date, datetime, timedelta
import pytz

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

    try:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog (
            database="nvn_knowledge",
            table_name="mapping_lo_student"
        )
    except:
        dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options (
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
                                                  and x['backup_date'] == long(today_id)
                                                  and x["knowledge_pass_date_id"] is not None)

    dy_mapping_lo_student_current = dyf_mapping_lo_student_current.toDF()
    dy_mapping_lo_student_current.cache()

    number_original_rows = dy_mapping_lo_student_current.count()
    print ('number_original_rows: ', number_original_rows)
    if number_original_rows < 1:
        return


    #Xu luy vao bang lich su
    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dts-odin/flag/nvn_knowledge/student_knowledge_number_history.parquet")
        start_date_id = df_flag.collect()[0]['flag']
        start_date_id = long(start_date_id)
    except:
        print('read flag file error ')
        start_date_id = 0

    dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object"
    )

    df_learning_object = dyf_learning_object.toDF()

    dy_mapping_lo_student_current_for_number = dy_mapping_lo_student_current.join(df_learning_object,
                    (dy_mapping_lo_student_current['learning_object_id'] == df_learning_object['learning_object_id']),
                                       'left')

    dy_mapping_lo_student_current_for_number = dy_mapping_lo_student_current_for_number\
        .groupBy('student_id').agg(
            f.count(when((f.col('learning_object_type') == 'vocabulary'), True)).alias('vocabulary_number'),
            f.count(when((f.col('learning_object_type') == 'grammar'), True)).alias('grammar_number'),
            f.count(when((f.col('learning_object_type') == 'phonetic'), True)).alias('phonetic_number'),
            f.lit(yesterday_id).alias('created_date_id')
    )

    print ('dy_mapping_lo_student_current_for_number')
    dy_mapping_lo_student_current_for_number.printSchema()
    dy_mapping_lo_student_current_for_number.show(2)

    dyf_mapping_lo_learning_object = DynamicFrame.fromDF(dy_mapping_lo_student_current_for_number, glueContext, 'dyf_mapping_lo_learning_object')

    apply_ouput = ApplyMapping.apply(frame=dyf_mapping_lo_learning_object,
                                     mappings=[("student_id", "long", "student_id", "long"),
                                               ("created_date_id", "long", "created_date_id", "long"),
                                               ("vocabulary_number", "long", "vocabulary_number", "long"),
                                               ("grammar_number", "long", "grammar_number", "long"),
                                               ("phonetic_number", "long", "phonetic_number", "long")
                                               ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    clear_query = 'DELETE student_knowledge_number_history where created_date_id = ' + str(yesterday_id)
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": clear_query,
                                                                   "dbtable": "student_knowledge_number_history",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/student_knowledge_number_history",
                                                               transformation_ctx="datasink4")


if __name__ == "__main__":
    main()
