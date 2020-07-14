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
from pyspark.sql import Window
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

    # d = date(2005, 7, 14)
    # print(d)

    start = date(2019, 9, 29)
    end = date(2019, 12, 2)

    date_ = start
    i = 0

    date_generated = []

    while date_ <= end:
        date_id = long(date_.strftime("%Y%m%d"))
        date_generated.append(date_id)
        date_ = date_ + timedelta(days=1)

    print('date_generated')
    print(date_generated)


    # date_generated = [start + date.timedelta(days=1) for x in range(0, (end - start))]
    #
    # print('date_generated')
    # print(date_generated)
    #
    # for date_ in date_generated:
    #     print date_.strftime("%d-%m-%Y")

    # try:
    dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_catalog (
        database="nvn_knowledge",
        table_name="mapping_lo_student"
    )
    # except:
    # dyf_mapping_lo_student_current = glueContext.create_dynamic_frame.from_options (
    #     connection_type="redshift",
    #     connection_options={
    #         "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
    #         "user": "dtsodin",
    #         "password": "DWHDtsodin@123",
    #         "dbtable": "temp_for_test_mapping_lo_student",
    #         "redshiftTmpDir": "s3://dts-odin/temp1/dyf_mapping_lo_student_current/v9"}
    #     )

    dyf_mapping_lo_student_current = dyf_mapping_lo_student_current.resolveChoice(specs=[('backup_date', 'cast:long')])

    dyf_mapping_lo_student_current = Filter.apply(frame=dyf_mapping_lo_student_current,
                                                  f=lambda x: x["student_id"] is not None and x["student_id"] != 0
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
        .groupBy('student_id', 'created_date_id').agg(
            f.count(when((f.col('learning_object_type') == 'vocabulary'), True)).alias('vocabulary_pass'),
            f.count(when((f.col('learning_object_type') == 'grammar'), True)).alias('grammar_pass'),
            f.count(when((f.col('learning_object_type') == 'phonetic'), True)).alias('phonetic_pass')
    )

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

    dy_mapping_lo_student_number_date = dy_mapping_lo_student_current_for_number\
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


    print ('dy_mapping_lo_student_number_date_cum')
    dy_mapping_lo_student_number_date_cum.printSchema()
    dy_mapping_lo_student_number_date_cum.show(300)

    # dy_mapping_lo_student_number_date_cum
    # root
    # | -- student_id: long(nullable=true)
    # | -- date_id: integer(nullable=true)
    # | -- cum_vocabulary: long(nullable=true)
    # | -- cum_grammar: long(nullable=true)
    # | -- cum_phonetic: long(nullable=true)

    dyf_mapping_lo_student_number_date_cum = DynamicFrame\
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
    #
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dfy_output,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "student_knowledge_number_history_thanhtv3",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/thanhtv3/student_knowledge_number_history_thanhtv3",
                                                               transformation_ctx="datasink4")

    dy_student_date.unpersist()
    dy_mapping_lo_student_current_for_number.unpersist()

if __name__ == "__main__":
    main()
