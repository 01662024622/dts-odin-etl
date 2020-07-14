import sys
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.dynamicframe_filter import Filter
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType


def main():
    def checknull(level_modified, level_study):
        if level_modified is not None :
            return level_modified
        else:
            return level_study
    checknull_ =  udf(checknull, StringType())

    def concaText(student_behavior_date, behavior_id, student_id, contact_id,
                  package_code, package_endtime, package_starttime,
                  student_level_code, student_package_status_code, transformed_at):
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
        if package_endtime is not None:
            text_concat += str(package_endtime)
        if package_starttime is not None:
            text_concat += str(package_starttime)
        if student_level_code is not None:
            text_concat += str(student_level_code)
        if student_package_status_code is not None:
            text_concat += str(student_package_status_code)
        if transformed_at is not None:
            text_concat += str(transformed_at)
        return text_concat

    concaText = udf(concaText, StringType())
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    dyf_av_care_call = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                             table_name="care_call")

    # duration: int
    # | -- call_status: string
    # | -- time_created: string
    # | -- phone1: string

    dyf_av_care_call = dyf_av_care_call.select_fields([ 'phone', 'duration', 'call_status']).rename_field('phone', 'phone1')
    dyf_av_care_call.printSchema()
    dyf_av_care_call.show(2)

    print("dyf_av_care_call: ", dyf_av_care_call.count())
    dyf_av_care_call = Filter.apply(frame = dyf_av_care_call, f = lambda x: x["call_status"] in ( 'success' , 'call_success' ) and  x["duration"] > 30)
    print("dyf_av_care_call: ", dyf_av_care_call.count())
    print("WWWWWWWWWWWW")
    dyf_av_care_call.printSchema()
    dyf_av_care_call.show(2)

    dyf_student_contact_phone = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                             table_name="student_contact_phone")

    dyf_student_contact_phone = dyf_student_contact_phone.select_fields(
        ['contact_id', 'phone'])


    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                table_name="student_contact")

    dyf_student_contact = dyf_student_contact.select_fields(
        ['student_id', 'contact_id', 'level_study'])



    dyf_log_student_level_study = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                               table_name="log_student_level_study")

    dyf_log_student_level_study = dyf_log_student_level_study.select_fields(
        ['contact_id', 'level_current', 'level_modified', 'package_code', 'time_created'])
    dyf_log_student_level_study = dyf_log_student_level_study.resolveChoice(
        specs=[('_key', 'cast:int')])

    dyf_tpe_invoice_product = glueContext.create_dynamic_frame.from_catalog(database = "tig_market",
                                                                            table_name = "tpe_invoice_product")
    dyf_tpe_invoice_product = dyf_tpe_invoice_product.select_fields(['_key', 'timecreated' ,'user_id', 'buyer_id', 'invoice_packages_price', 'invoice_price', 'invoice_code'])
    dyf_tpe_invoice_product = dyf_tpe_invoice_product.resolveChoice(
        specs=[('_key', 'cast:long')])
    dyf_tpe_invoice_product_details = glueContext.create_dynamic_frame.from_catalog(database = "tig_market",
                                                                            table_name = "tpe_invoice_product_details")

    dyf_tpe_invoice_product_details = dyf_tpe_invoice_product_details.select_fields(['cat_code', 'package_time', 'invoice_code'])

    dyf_student_package = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_student_package"
    )



    # chon cac field
    dyf_student_package = dyf_student_package.select_fields(['student_id', 'start_time', 'end_time', 'package_code']).rename_field('student_id', 'student_id1')
    # # doc flag tu s3
    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dtsodin/flag/student_behavior/flag_hoc_vien_duoc_chao_mung.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_tpe_invoice_product = Filter.apply(frame=dyf_tpe_invoice_product, f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    print('the number of new contacts: ', dyf_tpe_invoice_product.count())


    if (dyf_tpe_invoice_product.count() > 0):
        df_log_student_level_study = dyf_log_student_level_study.toDF()
        df_log_student_level_study = df_log_student_level_study.groupby('contact_id', 'level_current', 'level_modified', 'package_code').agg(f.max('time_created').alias('time_created') )

        dyf_join0 = Join.apply(dyf_tpe_invoice_product, dyf_tpe_invoice_product_details, 'invoice_code', 'invoice_code')

        dyf_log_student_level_study = DynamicFrame.fromDF(df_log_student_level_study, glueContext, "dyf_log_student_level_study")
        dyf_join1 = Join.apply(dyf_log_student_level_study, dyf_student_contact_phone, "contact_id", "contact_id")

        dyf_join01 = Join.apply(dyf_join1, dyf_av_care_call, "phone", "phone1")
        dyf_join00 = Join.apply(dyf_join0, dyf_student_contact, "user_id", "contact_id")
        dyf_join = Join.apply(dyf_join00, dyf_join01, "contact_id", "contact_id")
        dyf_join.printSchema()
        dyf_join.show(2)

        dyf_join = Filter.apply(frame = dyf_join, f= lambda x: x['time_created'] <= x['timecreated'])
        print("@@@@@@@@@@@@@@@")
        dyf_join.printSchema()
        dyf_join.show(2)
        dyf_data_join3 = Join.apply(dyf_join, dyf_student_package, "student_id", "student_id1")
        dyf_data_join3 = Filter.apply(frame = dyf_data_join3, f = lambda x: x['package_code'] == x['cat_code'])
        df_data_join3 = dyf_data_join3.toDF()
        df_data_join3 = df_data_join3.withColumn("student_level_code", checknull_(df_data_join3['level_modified'], df_data_join3['level_study']))\
        .withColumn("behavior_id", f.lit(4))\
        .withColumn("student_package_status_code", f.lit("DEACTIVED"))\
        .withColumn("student_behavior_date", from_unixtime(df_data_join3.timecreated))\
        .withColumn("package_starttime", df_data_join3['start_time'])\
        .withColumn("package_endtime", df_data_join3['end_time'])\
        .withColumn("transformed_at", f.lit(None))
        df_data_join3 = df_data_join3.withColumn('student_behavior_id',
                                                     f.md5(concaText(
                                                         df_data_join3.student_behavior_date,
                                                         df_data_join3.behavior_id,
                                                         df_data_join3.student_id,
                                                         df_data_join3.contact_id,
                                                         df_data_join3.package_code,
                                                         df_data_join3.package_endtime,
                                                         df_data_join3.package_starttime,
                                                         df_data_join3.student_level_code,
                                                         df_data_join3.student_package_status_code,
                                                         df_data_join3.transformed_at
                                                     )))
        df_data_join3 = df_data_join3.dropDuplicates()
        dyf_data_join3 = DynamicFrame.fromDF(df_data_join3, glueContext, "dyf_data_join3")
        dyf_data_join3 = dyf_data_join3.resolveChoice(specs=[('behavior_id', 'cast:int'), ('student_behavior_date', 'cast:timestamp')])
        dyf_data_join3.printSchema()
        dyf_data_join3.show(2)

        applymapping = ApplyMapping.apply(frame=dyf_data_join3,
                                          mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                                    ("contact_id", "string", "contact_id", "string"),
                                                    ("student_id", "string", "student_id", "long"),
                                                    ("student_behavior_date", "timestamp", "student_behavior_date","long"),
                                                    ("cat_code", "string", "package_code", "string"),
                                                    ("package_starttime", "int", "package_starttime", "long"),
                                                    ("package_endtime", "int", "package_endtime", "long"),
                                                    ("student_package_status_code", "string", "student_status_code", "string"),
                                                    ("behavior_id", "int", "behavior_id", "long"),
                                                    ("student_level_code", "string", "student_level_code", "string")])

        resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                            transformation_ctx="resolvechoice")

        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

        print(dropnullfields.count())
        dropnullfields.toDF().show()


        glueContext.write_dynamic_frame.from_options(
            frame=dropnullfields,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")

        applymapping1 = ApplyMapping.apply(frame=dyf_data_join3,
                                          mappings=[("invoice_packages_price", "int", "measure1", "long"),
                                                    ("behavior_id", "int", "behavior_id", "long"),
                                                    ("invoice_price", "int", "measure2 ", "long")])

        resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                            transformation_ctx="resolvechoice1")

        dropnullfields1 = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields1")

        print(dropnullfields1.count())
        dropnullfields1.toDF().show()
        glueContext.write_dynamic_frame.from_options(
            frame=dropnullfields,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_general_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")



        dyf_tpe_invoice_product = dyf_tpe_invoice_product.toDF()
        flag = dyf_tpe_invoice_product.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dtsodin/flag/student_behavior/flag_hoc_vien_duoc_chao_mung.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
