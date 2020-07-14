import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime,unix_timestamp,date_format
import pyspark.sql.functions as f

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # 1. ETL du lieu user_map
    # 2. ETL du lieu user_communication: phone va email
    # 3. ETL trang thai RL2130.29 (Trang thai da tao tai khoan LMS thanh cong)
    # START
    # doc datasource
    student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    # chon cac field
    student_contact = student_contact.select_fields(['_key', 'contact_id', 'student_id', 'time_lms_created'])
    # convert kieu du lieu
    student_contact = student_contact.resolveChoice(specs=[('_key', 'cast:long')])

    # doc moc flag tu s3
    df_flag = spark.read.parquet("s3://dtsodin/flag/flag_HV.parquet")
    max_key = df_flag.collect()[0]['flag']
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # student_contact = Filter.apply(frame=student_contact, f=lambda x: x["_key"] > max_key)

    if (student_contact.count() > 0):
        try:
            student_contact_df = student_contact.toDF()
            student_contact_df = student_contact_df.withColumn('ngay_s0', from_unixtime(student_contact_df.time_lms_created))
            student_contact_df = student_contact_df.withColumn('id_ngay_s0', from_unixtime(student_contact_df.time_lms_created, "yyyyMMdd"))
            student_contact_df.printSchema()
            student_contact = DynamicFrame.fromDF(student_contact_df, glueContext, "student_contact")
            # chon cac truong va kieu du lieu day vao db
            applyMapping = ApplyMapping.apply(frame=student_contact,
                                              mappings=[("contact_id", "string", "contact_id", "string"),
                                                        ("student_id", "string", "student_id", "string"),
                                                        ("id_ngay_s0", "string", "id_ngay_s0", "string"),
                                                        ("ngay_s0", "string", "ngay_s0", "timestamp")])

            resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols",
                                                transformation_ctx="resolvechoice")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

            # ghi dl vao db, thuc hien update data
            print("Count data student:  ", dropnullfields.count())
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={"dbtable": "temp_student_contact",
                                                                                           "database": "dts_odin",
                                                                                            "postactions": """ call proc_insert_student_contact();
                                                                                                                DROP TABLE IF EXISTS  temp_student_contact
                                                                                                                     """
                                                                                           },
                                                                       redshift_tmp_dir="s3n://dtsodin/temp/tig_advisor/user_profile_student_contact/",
                                                                       transformation_ctx="datasink4")


            # lay max key trong data source
            datasource = student_contact.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # convert kieu dl
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_HV.parquet", mode="overwrite")
        except:
            datasource = student_contact.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dtsodin/flag/flag_HV.parquet", mode="overwrite")

    ########
    # doc data email
    student_contact_email = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact_email"
    )

    # chon cac filed
    student_contact_email = student_contact_email.select_fields(['_key', 'contact_id', 'email', 'default', 'deleted'])
    # convert kieu du lieu
    student_contact_email = student_contact_email.resolveChoice(specs=[('_key', 'cast:long')])

    # doc moc flag tu s3
    df_flag = spark.read.parquet("s3://dtsodin/flag/flag_HV_Email.parquet")
    max_key = df_flag.collect()[0]['flag']
    print("max_key_email:  ", max_key)
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    student_contact_email = Filter.apply(frame=student_contact_email, f=lambda x: x["_key"] > max_key)
    print("Count data 1:  ", student_contact_email.count())
    if (student_contact_email.count() > 0):
        try:
            # chon cac truong va kieu du lieu day vao db
            applyMappingEmail = ApplyMapping.apply(frame=student_contact_email,
                                              mappings=[("contact_id", "string", "contact_id", "string"),
                                                        ("email", "string", "email", "string"),
                                                        ("default", "int", "default", "int"),
                                                        ("deleted", "int", "deleted", "int")])

            resolvechoiceEmail = ResolveChoice.apply(frame=applyMappingEmail, choice="make_cols",
                                                transformation_ctx="resolvechoiceEmail")
            dropnullfieldsEmail = DropNullFields.apply(frame=resolvechoiceEmail, transformation_ctx="dropnullfieldsEmail")

            # ghi dl vao db, thuc hien update data
            datasinkEmail = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfieldsEmail,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "temp_student_contact_email",
                                                                           "database": "dts_odin",
                                                                            "postactions": """ insert into user_communication(user_id, communication_type, comunication, is_primary, is_deleted, last_update_date)
                                                                                                select DISTINCT user_id, 2 as communication_type, email, temp_student_contact_email.default, deleted, CURRENT_DATE as last_update_date from temp_student_contact_email
                                                                                                join user_map on source_id = contact_id and source_type = 1;
                                                                                                DROP TABLE IF EXISTS  temp_student_contact_email
                                                                                                     """
                                                                       },
                                                                       redshift_tmp_dir="s3n://dtsodin/temp/tig_advisor/user_profile_student_contact/",
                                                                       transformation_ctx="datasink4")

            # lay max key trong data source
            datasource = student_contact_email.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # convert kieu dl
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_HV_Email.parquet", mode="overwrite")
        except:
            datasource = student_contact_email.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dtsodin/flag/flag_HV_Email.parquet", mode="overwrite")

    ########
    # doc data phone
    student_contact_phone = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact_phone"
    )

    # chon cac filed
    student_contact_phone = student_contact_phone.select_fields(['_key', 'contact_id', 'phone', 'default', 'deleted'])
    # convert kieu du lieu
    student_contact_phone = student_contact_phone.resolveChoice(specs=[('_key', 'cast:long')])

    # doc moc flag tu s3
    df_flag = spark.read.parquet("s3://dtsodin/flag/flag_HV_Phone.parquet")
    max_key = df_flag.collect()[0]['flag']
    print("max_key_phone:  ", max_key)
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    student_contact_phone = Filter.apply(frame=student_contact_phone, f=lambda x: x["_key"] > max_key)
    print("Count data phone:  ", student_contact_phone.count())

    if (student_contact_phone.count() > 0):
        try:
            # chon cac truong va kieu du lieu day vao db
            applyMappingPhone = ApplyMapping.apply(frame=student_contact_phone,
                                                   mappings=[("contact_id", "string", "contact_id", "string"),
                                                             ("phone", "string", "phone", "string"),
                                                             ("default", "int", "default", "int"),
                                                             ("deleted", "int", "deleted", "int")])

            resolvechoicePhone = ResolveChoice.apply(frame=applyMappingPhone, choice="make_cols",
                                                     transformation_ctx="resolvechoiceEmail")
            dropnullfieldsPhone = DropNullFields.apply(frame=resolvechoicePhone,
                                                       transformation_ctx="dropnullfieldsPhone")

            # ghi dl vao db, thuc hien update data
            datasinkPhone = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfieldsPhone,
                                                                           catalog_connection="glue_redshift",
                                                                           connection_options={
                                                                               "dbtable": "temp_student_contact_phone",
                                                                               "database": "dts_odin",
                                                                               "postactions": """insert into user_communication(user_id, communication_type, comunication, is_primary, is_deleted, last_update_date)
                                                                                                    select DISTINCT user_id, 1 as communication_type, phone, temp_student_contact_phone.default, deleted, CURRENT_DATE as last_update_date from temp_student_contact_phone
                                                                                                    join user_map on source_id = contact_id and source_type = 1;
                                                                                                    DROP TABLE IF EXISTS temp_student_contact_phone
                                                                                                     """
                                                                           },
                                                                           redshift_tmp_dir="s3n://dtsodin/temp/tig_advisor/user_profile_student_contact/",
                                                                           transformation_ctx="datasink4")

            # lay max key trong data source
            datasource = student_contact_phone.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # convert kieu dl
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_HV_Phone.parquet", mode="overwrite")
        except:
            datasource = student_contact_phone.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dtsodin/flag/flag_HV_Phone.parquet", mode="overwrite")

if __name__ == "__main__":
    main()