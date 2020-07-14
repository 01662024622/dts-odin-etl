import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import time
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pytz
from pyspark.sql.types import DateType
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = True
is_saved_temporary = False


###-------------------------###-----------------------###------------------------------------------###
def insert_time_dim(start_date_id, end_date_id):
    time_begin = datetime.strptime(str(start_date_id), "%Y%m%d").date()
    time_end = datetime.strptime(str(end_date_id), "%Y%m%d").date()

    print('time_begin')
    print(time_begin)

    print('time_end')
    print(time_end)

    # tao dataframe tu time_begin va time_end
    data = [(time_begin, time_end)]
    df = spark.createDataFrame(data, ["minDate", "maxDate"])
    # convert kieu dl va ten field
    df = df.select(df.minDate.cast(DateType()).alias("minDate"), df.maxDate.cast(DateType()).alias("maxDate"))

    # chay vong lap lay tat ca cac ngay giua mindate va maxdate
    df = df.withColumn("daysDiff", f.datediff("maxDate", "minDate")) \
        .withColumn("repeat", f.expr("split(repeat(',', daysDiff), ',')")) \
        .select("*", f.posexplode("repeat").alias("date", "val")) \
        .withColumn("date", f.expr("to_date(date_add(minDate, date))")) \
        .select('date')

    # convert date thanh cac option ngay_thang_nam
    df = df.withColumn('id', f.date_format(df.date, "yyyyMMdd")) \
        .withColumn('ngay_trong_thang', f.dayofmonth(df.date)) \
        .withColumn('ngay_trong_tuan', f.from_unixtime(f.unix_timestamp(df.date, "yyyy-MM-dd"), "EEEEE")) \
        .withColumn('tuan_trong_nam', f.weekofyear(df.date)) \
        .withColumn('thang', f.month(df.date)) \
        .withColumn('quy', f.quarter(df.date)) \
        .withColumn('nam', f.year(df.date))
    df = df.withColumn('tuan_trong_thang', (df.ngay_trong_thang - 1) / 7 + 1)

    data_time = DynamicFrame.fromDF(df, glueContext, 'data_time')

    # convert data
    data_time = data_time.resolveChoice(specs=[('tuan_trong_thang', 'cast:int')])

    # chon cac truong va kieu du lieu day vao db
    applymapping1 = ApplyMapping.apply(frame=data_time,
                                       mappings=[("id", "string", "id", "bigint"),
                                                 ("ngay_trong_thang", 'int', 'ngay_trong_thang', 'int'),
                                                 ("ngay_trong_tuan", "string", "ngay_trong_tuan", "string"),
                                                 ("tuan_trong_thang", "int", "tuan_trong_thang", "int"),
                                                 ("tuan_trong_nam", "int", "tuan_trong_nam", "int"),
                                                 ("thang", "int", "thang", "int"),
                                                 ("quy", "int", "quy", "int"),
                                                 ("nam", "int", "nam", "int"),
                                                 ("date", "date", "ngay", "timestamp")])

    resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols", transformation_ctx="resolvechoice2")
    dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

    # ghi dl vao db
    preactions = 'delete student.time_dim where id >= ' + str(start_date_id)
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": preactions,
                                                                   "dbtable": "student.time_dim",
                                                                   "database": "student_native_report"},
                                                               redshift_tmp_dir="s3n://dts-odin/temp/tu-student_native_report/student/time_dim",
                                                               transformation_ctx="datasink4")


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame.toDF()

def insert_advisor():
    # ========== init
    glue_context = GlueContext(SparkContext.getOrCreate())

    # ========== retrieve dynamic frame
    df_advisor = retrieve_dynamic_frame(
        glue_context,
        'tig_advisor',
        'advisor_account',
        ['user_id', 'user_name', 'user_email', 'ip_phone_number', 'level', 'type_manager']
    )

    df_advisor = df_advisor.dropDuplicates(['user_id'])


    dyf_advisor = DynamicFrame.fromDF(
        df_advisor,
        glue_context,
        "dyf_advisor"
    )

    mapping_result = ApplyMapping.apply(frame=dyf_advisor,
                                        mappings=[("user_id", "int", "advisor_id", "int"),
                                                  ("user_name", 'string', 'user_name', 'string'),

                                                  ("user_email", 'string', 'email', 'string'),
                                                  ("ip_phone_number", 'int', 'ip_phone', 'int'),

                                                  ("level", 'int', 'level', 'int'),
                                                  ("type_manager", 'string', 'type_manager', 'string')

                                                  ])

    resolvechoice = ResolveChoice.apply(frame=mapping_result,
                                        choice="make_cols",
                                        transformation_ctx="resolvechoice2")

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=resolvechoice,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "preactions": """TRUNCATE table student.advisor_dim""",
                                                                   "dbtable": "student.advisor_dim",
                                                                   "database": "student_native_report"
                                                               },
                                                               redshift_tmp_dir="s3n://dts-odin/temp/student/advisor_dim",
                                                               transformation_ctx="datasink4")


def main():
    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    date_id = long(today.strftime("%Y%m%d"))
    print('today_id: ', date_id)

    start_date = today - timedelta(10)
    start_date_id = long(start_date.strftime("%Y%m%d"))

    start_date_id = start_date_id
    end_date_id = date_id
    print('start_date_id: ', start_date_id)
    print('end_date_id: ', end_date_id)
    #

    insert_time_dim(start_date_id, end_date_id)
    insert_advisor()


if __name__ == "__main__":
    main()
