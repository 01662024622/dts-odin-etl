import sys
import pydevd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField, StringType
from pyspark.sql.functions import udf
import pyspark.sql.functions as f

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    student_id_unavailable = '0'
    package_endtime_unavailable = 99999999999L
    package_starttime_unavailable = 0L
    student_level_code_unavailable = 'UNAVAILABLE'
    student_status_code_unavailable = 'UNAVAILABLE'

    package_endtime = 'package_endtime'
    package_starttime = 'package_starttime'
    student_level_code = 'student_level_code'
    student_status_code = 'student_status_code'

    SUSPENDED = 'SUSPENDED'

    dyf_tpe_enduser_used_product_history = glueContext.create_dynamic_frame.from_catalog(
        database="tig_market",
        table_name="tpe_enduser_used_product_history"
    )
    dyf_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.select_fields(
        ['_key', 'contact_id', 'used_product_id', 'status_old', 'status_new', 'status_description', 'timecreated'])
        # .rename_field('contact_id', 'contactid')

    dyf_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.resolveChoice(specs=[('_key', 'cast:long')])
    # try:
    #     df_flag = spark.read.parquet("s3://dtsodin/flag/flag_trang_thai_tai_khoan_suspened.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print("max_key:  ", max_key)
    #     # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    #     dyf_tpe_enduser_used_product_history = Filter.apply(frame=dyf_tpe_enduser_used_product_history, f=lambda x: x["_key"] > max_key)
    # except:
    #     print('read flag file error ')
    print dyf_tpe_enduser_used_product_history.count()
    if dyf_tpe_enduser_used_product_history.count() > 0:
        try:
            dyf_tpe_invoice_product_details = glueContext.create_dynamic_frame.from_catalog(
                database="tig_market",
                table_name="tpe_invoice_product_details"
            )
            dyf_tpe_invoice_product_details = dyf_tpe_invoice_product_details.select_fields(
                ['id', 'cat_code'])

            dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
                database="tig_advisor",
                table_name="student_contact"
            )
            dyf_student_contact = dyf_student_contact.select_fields(
                ['contact_id', 'student_id']).rename_field('contact_id', 'contactid')

            ##################### Join and Filter data
            df_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.toDF()
            df_tpe_used_product_history_step1 = df_tpe_enduser_used_product_history.groupby('contact_id',
                                                                                            'used_product_id').agg(
                f.max("timecreated").alias("max_timecreated")) \
                .withColumnRenamed("contact_id", "contact_id_temp")
            print df_tpe_used_product_history_step1.count()
            df_tpe_used_product_history_step1.show()

            df_tpe_used_product_history_step2 = df_tpe_used_product_history_step1.groupby('contact_id_temp').agg(
                f.max("max_timecreated").alias("max_timecreated"),
                f.count("used_product_id").alias("count_used_product_id"))
            print df_tpe_used_product_history_step2.count()
            df_tpe_used_product_history_step2.show()
            print "EEEEEEEEEEEEEEEEEEEEEEEEE"

            dyf_tpe_used_product_history = DynamicFrame.fromDF(df_tpe_used_product_history_step2, glueContext,
                                                               "dyf_tpe_used_product_history")

            dyf_part_one = Filter.apply(frame=dyf_tpe_used_product_history,
                                        f=lambda x: x["count_used_product_id"] <= 1)

            # dyf_part_two = Filter.apply(frame=df_tpe_enduser_used_product_history,
            #                             f=lambda x: x["used_product_id"] > 1)
            df_part_one = dyf_part_one.toDF()
            df_part_one = df_part_one.join(df_tpe_enduser_used_product_history,
                                           (df_part_one.contact_id_temp == df_tpe_enduser_used_product_history.contact_id)
                                           & (df_part_one.max_timecreated == df_tpe_enduser_used_product_history.timecreated))

            dyf_part_one = DynamicFrame.fromDF(df_part_one, glueContext, "dyf_part_one")
            dyf_part_one = dyf_part_one.select_fields(['contact_id', 'used_product_id', 'status_old',
                                                       'status_new', 'status_description', 'timecreated'])


            dyf_join_part_one_product_details = Join.apply(dyf_part_one,
                                                           dyf_tpe_invoice_product_details, 'used_product_id', 'id')

            dyf_join_part_one_product_details.printSchema()
            print "total 01: ", dyf_join_part_one_product_details.count()
            dyf_join_part_one_product_details.toDF().show(2)

            dyf_join_part_one_contact = Join.apply(dyf_join_part_one_product_details,
                                                   dyf_student_contact, 'contact_id', 'contactid')
            dyf_join_part_one_contact = dyf_join_part_one_contact \
                .select_fields(['contact_id', 'student_id', 'status_new', 'status_description', 'timecreated'])


            dyf_join_part_one_contact.printSchema()
            print "total 02: ", dyf_join_part_one_contact.count()
            dyf_join_part_one_contact.toDF().show(2)
            # df_join_part_one = dyf_join_part_one_contact.toDF()

            ######################################
            ######## START suspened
            dyf_join_suspened_status = Filter.apply(frame=dyf_join_part_one_contact,
                                                    f=lambda x: x["status_new"] == SUSPENDED)
            print "dyf_join_suspened_status ", dyf_join_suspened_status.count()
            dyf_join_suspened_status.toDF().show(2)
            df_join_suspened_status = dyf_join_suspened_status.toDF()

            df_join_suspened_status = df_join_suspened_status \
                .withColumn("change_status_date_id",
                            from_unixtime(df_join_suspened_status.timecreated, 'yyyyMMdd').cast("long")) \
                .withColumn("from_status_id", f.lit(None).cast("long")) \
                .withColumn("to_status_id", f.lit(207).cast("long")) \
                .withColumn("measure1", f.lit(None).cast("long")) \
                .withColumn("measure2", f.lit(None).cast("long")) \
                .withColumn("description", df_join_suspened_status.status_description) \
                .withColumn("timestamp1", f.lit(None).cast("long"))
            df_join_suspened_status.show(3)
            dyf_join_suspened_status = DynamicFrame.fromDF(df_join_suspened_status, glueContext,
                                                           "dyf_join_suspened_status")

            dyf_join_suspened_status = dyf_join_suspened_status \
                .select_fields(['contact_id', 'student_id', 'change_status_date_id', 'from_status_id',
                                'to_status_id', 'measure1', 'measure2', 'description', 'timestamp1'])
            dyf_join_suspened_status.printSchema()
            df_join_suspened_status = dyf_join_suspened_status.toDF()
            ####### END
            
            df_join_suspened_status = df_join_suspened_status.withColumn("user_id", f.lit(None).cast("long"))

            dyf_join_status = DynamicFrame.fromDF(df_join_suspened_status, glueContext, "dyf_join_status")

            applymapping1 = ApplyMapping.apply(frame=dyf_join_status,
                                               mappings=[
                                                   ("student_id", "string", "student_id", "long"),
                                                   ("user_id", "long", "user_id", "long"),
                                                   ("change_status_date_id", "long", "change_status_date_id", "long"),
                                                   ("from_status_id", "long", "from_status_id", "long"),
                                                   ("to_status_id", "long", "to_status_id", "long"),
                                                   ("measure1", "long", "measure1", "double"),
                                                   ("measure2", "long", "measure2", "double"),
                                                   ("description", "string", "description", "string"),
                                                   ("timestamp1", "long", "timestamp1", "long"),
                                                   ("contact_id", "string", "contact_id", "string")
                                               ])

            resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice1")
            dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields1")
            print resolvechoice1.count()
            resolvechoice1.printSchema()
            resolvechoice1.show(5)
            print('START WRITE TO REDSHIFT -------------------------')
            datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_changed_status_student",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3a://dtsodin/temp/mapping_changed_status_student/",
                                                                       transformation_ctx="datasink1")

            print('START WRITE TO S3-------------------------')
            # datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
            #                                                          connection_options={
            #                                                              "path": "s3://dtsodin/student_behavior/student_behavior/",
            #                                                              "partitionKeys": ["behavior_id"]},
            #                                                          format="parquet",
            #                                                          transformation_ctx="datasink6")
            print('END WRITE TO S3-------------------------')

            df_temp = dyf_tpe_enduser_used_product_history.toDF()
            flag = df_temp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_trang_thai_tai_khoan_suspened.parquet", mode="overwrite")
        except Exception as e:
            print "Something was wrong ",e


if __name__ == "__main__":
    main()