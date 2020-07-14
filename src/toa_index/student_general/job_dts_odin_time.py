import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import SQLContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
import pyspark.sql.functions as f
from pyspark.sql.functions import from_unixtime,unix_timestamp,date_format
from pyspark.sql.types import DateType
import datetime
from dateutil.relativedelta import relativedelta

## @params: [TempDir, JOB_NAME]

def main():

    # args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    # job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)

    # tao timebegin bang ngay hien tai + 1 nga
    # time_begin = datetime.date.today() + datetime.timedelta(days=1)
    #20190908
    time_begin = datetime.date(2019, 1, 1)

    # tao timeend bang ngay hien tai + 1 thang - 1 ngay
    # time_end = time_begin + relativedelta(months=1) - datetime.timedelta(days=1)

    time_end = datetime.date(2020, 2, 28)

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
    df = df.withColumn('id', date_format(df.date, "yyyyMMdd")) \
        .withColumn('ngay_trong_thang', f.dayofmonth(df.date)) \
        .withColumn('ngay_trong_tuan', from_unixtime(unix_timestamp(df.date, "yyyy-MM-dd"), "EEEEE")) \
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
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={"dbtable": "student.time_dim",
                                                                                   "database": "student_native_report"},
                                                               redshift_tmp_dir="s3n://dts-odin/temp/tu-hoc/hwb/fdfdf",
                                                               transformation_ctx="datasink4")


if __name__ == "__main__":
    main()