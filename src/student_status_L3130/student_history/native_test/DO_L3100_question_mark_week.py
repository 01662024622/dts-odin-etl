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


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # 1. ETL du lieu hoc vien lam bai thi native test tuan
    # START
    # doc datasource
    dyf_topica_question_mark_week = glueContext.create_dynamic_frame.from_catalog(
        database="native_test",
        table_name="top_topica_question_mark_week"
    )

    # chon cac field
    dyf_topica_question_mark_week = dyf_topica_question_mark_week.select_fields(
        ['_key', 'username', 'created_at'])
    # convert kieu du lieu
    dyf_topica_question_mark_week = dyf_topica_question_mark_week.resolveChoice(specs=[('_key', 'cast:long')])

    # doc moc flag tu s3
    df_flag = spark.read.parquet("s3://dts-odin/flag/flag_question_mark_week.parquet")
    max_key = df_flag.collect()[0]['flag']
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # student_contact = Filter.apply(frame=student_contact, f=lambda x: x["_key"] > max_key)
    print("max_key:  ", max_key)
    print("Count dyf_topica_question_mark_week:  ", dyf_topica_question_mark_week.count())
    if (dyf_topica_question_mark_week.count() > 0):
        try:
            dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
                database="tig_advisor",
                table_name="student_contact"
            )

            # chon cac field
            dyf_student_contact = dyf_student_contact.select_fields(['contact_id', 'student_id', 'user_name'])
            dyf_question_mark_week = Join.apply(dyf_topica_question_mark_week, dyf_student_contact, 'username',
                                                'user_name')
            df_question_mark_week = dyf_question_mark_week.toDF()
            df_question_mark_week = df_question_mark_week.drop_duplicates()

            df_question_mark_week = df_question_mark_week.withColumn('date_id', from_unixtime(
                unix_timestamp(df_question_mark_week.created_at, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
            df_question_mark_week = df_question_mark_week.groupby('student_id', 'date_id').agg(
                f.count('student_id').alias("measure1"))

            dyf_question_mark_week = DynamicFrame.fromDF(df_question_mark_week, glueContext, "dyf_question_mark_week")
            print("Count dyf_question_mark_week:  ", dyf_question_mark_week.count())
            dyf_question_mark_week.printSchema()
            dyf_question_mark_week.show()

            # chon cac truong va kieu du lieu day vao db
            applyMapping = ApplyMapping.apply(frame=dyf_question_mark_week,
                                              mappings=[("student_id", "string", "student_id", "string"),
                                                        ("date_id", "string", "date_id", "int"),
                                                        ("measure1", "long", "measure1", "double")])
            #
            resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols",
                                                transformation_ctx="resolvechoice")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")
            #
            # # ghi dl vao db, thuc hien update data
            # print("Count data student:  ", dropnullfields.count())
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "temp_top_topica_question_mark_week",
                                                                           "database": "dts_odin",
                                                                           "postactions": """INSERT INTO mapping_changed_status_student ( student_id, change_status_date_id, to_status_id, measure1 ) 
                                                                                                SELECT DISTINCT
                                                                                                student_id :: BIGINT, date_id AS change_status_date_id, 102 AS to_status_id, measure1
                                                                                                FROM
                                                                                                    temp_top_topica_question_mark_week;
                                                                                                -- cap nhat lai user_id
                                                                                                UPDATE mapping_changed_status_student 
                                                                                                SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 2 AND source_id = student_id ) 
                                                                                                WHERE
                                                                                                    user_id IS NULL;
                                                                                            DROP TABLE IF EXISTS temp_top_topica_question_mark_week"""
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/backup/top_topica_question_mark_week/",
                                                                       transformation_ctx="datasink4")

            # lay max key trong data source
            datasource = dyf_topica_question_mark_week.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # convert kieu dl
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_question_mark_week.parquet", mode="overwrite")
        except:
            datasource = dyf_topica_question_mark_week.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dts-odin/flag/flag_question_mark_week.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
