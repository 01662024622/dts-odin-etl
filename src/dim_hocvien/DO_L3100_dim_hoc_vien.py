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
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    # chon cac filed
    student_contact = student_contact.select_fields(['_key', 'contact_id', 'student_id', 'full_name', 'level_study',
                                                     'advisor_id', 'time_lms_created', 'address', 'gender', 'note',
                                                     'is_vip',
                                                     'pay_menthod', 'package_want_to_learn', 'sb100', 'time_handover',
                                                     'user_name', 'password', 'dwh_time_modified']).rename_field(
        'contact_id', 'id')
    # convert kieu du lieu
    student_contact = student_contact.resolveChoice(specs=[('_key', 'cast:long')])

    # doc moc flag tu s3
    # df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/dim_hoc_vien_temp.parquet")

    # # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # data = student_contact.toDF()
    # data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    # student_contact = DynamicFrame.fromDF(data, glueContext, "student_contact")

    if (student_contact.count() > 0):
        try:
            # Student_contact_email
            student_contact_email = glueContext.create_dynamic_frame.from_catalog(
                database='tig_advisor',
                table_name='student_contact_email'
            )
            student_contact_email = student_contact_email.select_fields(['contact_id', 'email', 'default'])

            # loc email co default = 1 (email dang su dung)
            student_contact_email_df = student_contact_email.toDF()
            student_contact_email_df = student_contact_email_df.where("default = 1")

            # Student_contact_phone
            student_contact_phone = glueContext.create_dynamic_frame.from_catalog(
                database='tig_advisor',
                table_name='student_contact_phone'
            )
            student_contact_phone = student_contact_phone.select_fields(
                ['contact_id', 'phone', 'default']).rename_field('contact_id', 'id_contact')

            # loc phone co default = 1 (phone dang su dung)
            student_contact_phone_df = student_contact_phone.toDF()
            student_contact_phone_df = student_contact_phone_df.where("default = 1")

            student_contact_df = student_contact.toDF()
            student_contact_df = student_contact_df.where("id is not null and id <> ''")
            # join lay email hv
            data_join_email = student_contact_df.join(student_contact_email_df,
                                                      student_contact_df.id == student_contact_email_df.contact_id,
                                                      'left_outer')
            # join lay phone hoc vien
            data_join_phone = data_join_email.join(student_contact_phone_df,
                                                   data_join_email.id == student_contact_phone_df.id_contact,
                                                   'left_outer')

            # loc va convert data
            # data_df = data_join_phone.where("student_id is not null and student_id <> ''")
            # data_df = data_df.where("id is not null and id <> ''")
            # data_df = data_df.where("(email is not null and email <> '') OR (phone is not null and phone <> '')")
            data_df = data_join_phone.withColumn('ngayvaolms', from_unixtime(data_join_phone.time_lms_created))
            data_df = data_df.withColumn('ngaybangiao', from_unixtime(data_df.time_handover))
            data_df = data_df.withColumn('gioi_tinh',
                                         when((data_df.gender == u'\u004b\u0068\u00e1\u0063'.encode("UTF-8"))
                                              & ((f.lower(data_df['full_name']).contains(
                                             u'\u0074\u0068\u1ecb'.encode("UTF-8")))
                                                 | (f.lower(data_df['full_name']).contains(
                                                     u'\u0074\u0068\u0069'.encode("UTF-8")))),
                                              u'\u004e\u1eef'.encode("UTF-8"))
                                         .when((data_df.gender == u'\u004b\u0068\u00e1\u0063'.encode("UTF-8"))
                                               & ((f.lower(data_df['full_name']).contains(
                                             u'\u0076\u0103\u006e'.encode("UTF-8")))
                                                  | (f.lower(data_df['full_name']).contains(
                                                     u'\u0076\u0061\u006e'.encode("UTF-8")))),
                                               u'\u004e\u0061\u006d'.encode("UTF-8"))
                                         .otherwise(data_df.gender))
            data_df = data_df.dropDuplicates(['id'])
            datasource = DynamicFrame.fromDF(data_df, glueContext, "datasource")

            # chon cac truong va kieu du lieu day vao db
            applyMapping = ApplyMapping.apply(frame=datasource,
                                              mappings=[("id", "string", "id", "string"),
                                                        ("student_id", "string", "id_hocvien", "string"),
                                                        ("full_name", "string", "hoten", "string"),
                                                        ("email", "string", "email", "string"),
                                                        ("phone", "string", "sdt", "string"),
                                                        ("address", "string", "dia_chi", "string"),
                                                        ("gioi_tinh", "string", "gioi_tinh", "string"),
                                                        ("note", "string", "note", "string"),
                                                        ("is_vip", "int", "is_vip", "string"),
                                                        ("pay_menthod", "string", "pt_thanh_toan", "string"),
                                                        ("package_want_to_learn", "string", "goi_muon_hoc", "string"),
                                                        ("sb100", "string", "sb100", "string"),
                                                        ("ngaybangiao", "string", "ngaybangiao", "timestamp"),
                                                        ("advisor_id", "int", "chuyenvienhuongdan", "string"),
                                                        ("level_study", "string", "trinhdohientai", "string"),
                                                        ("ngayvaolms", "string", "ngayvaolms", "timestamp"),
                                                        ("user_name", "string", "user_name", "string"),
                                                        ("password", "string", "password", "string"),
                                                        ("dwh_time_modified", "string", "ngay_tao", "timestamp")])

            resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols",
                                                transformation_ctx="resolvechoice")
            dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

            # ghi dl vao db, thuc hien update data
            # luu du lieu vao 1 bang phu -> xoa dl trung tren bang chinh -> insert data tu bang phu vao bang chinh -> xoa bang phu
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "staging_table_dim_hoc_vien",
                                                                           "database": "dts_odin"
                                                                           ,
                                                                           "postactions": """DELETE FROM public.dim_hocvien USING public.staging_table_dim_hoc_vien AS S 
                                                                                                WHERE dim_hocvien.contact_id = S.id;                                                                                                                
                                                                                            INSERT into dim_hocvien
                                                                                            SELECT up.user_id, shv.id, shv.id_hocvien, shv.hoten, shv.email, shv.sdt, shv.dia_chi, shv.gioi_tinh, 
                                                                                            shv.note, shv.is_vip, shv.pt_thanh_toan, 
                                                                                            shv.goi_muon_hoc, shv.sb100, shv.ngaybangiao, shv.chuyenvienhuongdan, shv.trinhdohientai,
                                                                                            shv.ngayvaolms, shv.user_name, shv.password, shv.ngay_tao
                                                                                            FROM public.staging_table_dim_hoc_vien shv
                                                                                            LEFT JOIN user_map up
                                                                                                ON up.source_type = 1
                                                                                                AND shv.id = up.source_id
                                                                                            WHERE up.user_id is not;
                                                                                            DROP TABLE IF EXISTS public.staging_table_dim_hoc_vien                    
                                                                                            """
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp/dim_hocvien/",
                                                                       transformation_ctx="datasink4")

            # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
            #                                                             catalog_connection="glue_redshift",
            #                                                             connection_options={"dbtable": "dim_hocvien",
            #                                                                                 "database": "datashine_dev"},
            #                                                             redshift_tmp_dir=args["TempDir"],
            #                                                             transformation_ctx="datasink4")

            datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3n://dts-odin/temp/dim_hoc_vien"},
                                                                     format="parquet", transformation_ctx="datasink6")

            # lay max key trong data source
            datasource = student_contact.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # convert kieu dl
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("3a://dts-odin/flag/student_status/dim_hoc_vien_temp.parquet", mode="overwrite")
        except:
            datasource = student_contact.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dts-odin/flag/student_status/dim_hoc_vien_temp.parquet", mode="overwrite")



if __name__ == "__main__":
    main()