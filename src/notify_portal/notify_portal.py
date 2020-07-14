import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType
from datetime import date, datetime, timedelta
import pytz
import hashlib

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second =  long(today.strftime("%s"))
    print('today_id: ', today_second)
    #------------------------------------------------------------------------------------------------------------------#

    def getSolanBaoLuu(solan_baoluu, songay_baoluu):
        if solan_baoluu is None:
            solan_baoluu = 0
        if songay_baoluu is None:
            songay_baoluu = 0
        if solan_baoluu > songay_baoluu:
            return songay_baoluu
        return solan_baoluu

    getSolanBaoLuu = udf(getSolanBaoLuu, LongType())


    def getSoNgayBaoLuu(solan_baoluu, songay_baoluu):
        if solan_baoluu is None:
            solan_baoluu = 0
        if songay_baoluu is None:
            songay_baoluu = 0
        if songay_baoluu > solan_baoluu:
            return songay_baoluu
        return solan_baoluu

    getSoNgayBaoLuu = udf(getSoNgayBaoLuu, LongType())



    def getContactId(code, contact_id_advisor):
        if code is not None:
            return code
        return contact_id_advisor

    getContactId = udf(getContactId, StringType())


    def concaText(student_behavior_date, behavior_id, student_id, contact_id,
                package_code, package_endtime,package_starttime,
                student_level_code, student_status_code, transformed_at):
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
        if package_endtime is  not None:
            text_concat += str(package_endtime)
        if package_starttime is not None:
            text_concat += str(package_starttime)
        if student_level_code is not None:
            text_concat += str(student_level_code)
        if student_status_code is not None:
            text_concat += str(student_status_code)
        if transformed_at is not None:
            text_concat += str(transformed_at)
        return text_concat

    concaText = udf(concaText, StringType())

    # ------------------------------------------------------------------------------------------------------------------#

    #------------------------------------------------------------------------------------------------------------------#
    dyf_poss_ghinhan_hp = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                            table_name='ghinhan_hp')

    dyf_poss_ghinhan_hp = dyf_poss_ghinhan_hp.select_fields(['_key', 'id',
                                                             'ngay_thanhtoan', 'so_tien',
                                                             'khoa_hoc_makh', 'trang_thai'])
    dyf_poss_ghinhan_hp = dyf_poss_ghinhan_hp.resolveChoice(specs=[('_key', 'cast:long')])


    try:
        df_flag = spark.read.parquet("s3a://dtsodin/flag/student_behavior/sb1_dong_tien.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_poss_ghinhan_hp = Filter.apply(frame=dyf_poss_ghinhan_hp,
                                      f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')

    dyf_poss_ghinhan_hp_number = dyf_poss_ghinhan_hp.count()
    print('dyf_poss_ghinhan_hp_number: ', dyf_poss_ghinhan_hp_number)
    if dyf_poss_ghinhan_hp_number < 1:
        return

    #-------------------------------------------------------------------------------------------------------------------#
    dyf_poss_khoa_hoc = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                        table_name='khoa_hoc')

    dyf_poss_khoa_hoc = dyf_poss_khoa_hoc.select_fields(['makh', 'mahv',  'goi_sanpham_id', 'trang_thai'])

    # -------------------------------------------------------------------------------------------------------------------#
    dyf_poss_hoc_vien = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                      table_name='hoc_vien')

    dyf_poss_hoc_vien = dyf_poss_hoc_vien.select_fields(['mahv', 'crm_id', 'trang_thai']).rename_field(
        'mahv', 'mahv_hv')

    # -------------------------------------------------------------------------------------------------------------------#
    dyf_poss_goi_sanpham = glueContext.create_dynamic_frame.from_catalog(database='poss',
                                                                      table_name='goi_sanpham')

    dyf_poss_goi_sanpham = dyf_poss_goi_sanpham.select_fields(['ma', 'id', 'solan_baoluu',
                                                               'songay_baoluu', 'trang_thai'])

    # -------------------------------------------------------------------------------------------------------------------#

    # -------------------------------------------------------------------------------------------------------------------#
    dyf_crm_goi_contacts = glueContext.create_dynamic_frame.from_catalog(database='crm_native',
                                                                         table_name='contacts')

    # print('dyf_crm_goi_contacts::full')
    #     # dyf_crm_goi_contacts.printSchema()


    dyf_crm_goi_contacts = dyf_crm_goi_contacts.select_fields(['Code']).rename_field('Code', 'code')
    dyf_crm_goi_contacts = Filter.apply(frame=dyf_crm_goi_contacts,
                                                  f=lambda x: x["code"] is not None and x["code"] != '')
    dy_crm_goi_contacts = dyf_crm_goi_contacts.toDF()
    dy_crm_goi_contacts = dy_crm_goi_contacts.dropDuplicates()
    # print('dy_crm_goi_contacts')
    # dy_crm_goi_contacts.printSchema()

    # -------------------------------------------------------------------------------------------------------------------#

    dyf_advisor_student_contact = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                         table_name='student_contact')

    dyf_advisor_student_contact = dyf_advisor_student_contact.select_fields(['student_id', 'contact_id'])
    dyf_advisor_student_contact = Filter.apply(frame=dyf_advisor_student_contact,
                                        f=lambda x: x["student_id"] is not None and x["student_id"] != ''
                                               and x["contact_id"] is not None and x["contact_id"] != '')\
                                        .rename_field('student_id', 'student_id_advisor')\
                                        .rename_field('contact_id', 'contact_id_advisor')

    dy_advisor_student_contact = dyf_advisor_student_contact.toDF()
    dy_advisor_student_contact = dy_advisor_student_contact.dropDuplicates(['student_id_advisor'])

    # print('dy_advisor_student_contact')
    # dy_advisor_student_contact.printSchema()

    # -------------------------------------------------------------------------------------------------------------------#

    # print('dyf_poss_ghinhan_hp')
    # dyf_poss_ghinhan_hp.printSchema()
    #
    # print('dyf_poss_khoa_hoc')
    # dyf_poss_khoa_hoc.printSchema()
    #
    # print('dyf_poss_hoc_vien')
    # dyf_poss_hoc_vien.printSchema()
    #
    # print('dyf_poss_goi_sanpham')
    # dyf_poss_goi_sanpham.printSchema()

    dy_poss_ghinhan_hp = dyf_poss_ghinhan_hp.toDF()
    dy_poss_ghinhan_hp = dy_poss_ghinhan_hp.dropDuplicates(['id'])

    dy_poss_khoa_hoc = dyf_poss_khoa_hoc.toDF()
    dy_poss_khoa_hoc = dy_poss_khoa_hoc.dropDuplicates(['makh', 'mahv'])

    dy_poss_hoc_vien = dyf_poss_hoc_vien.toDF()
    dy_poss_hoc_vien = dy_poss_hoc_vien.dropDuplicates(['mahv_hv'])


    dy_poss_goi_sanpham = dyf_poss_goi_sanpham.toDF()
    dy_poss_hoc_vien = dy_poss_hoc_vien.dropDuplicates(['crm_id'])

    poss_ghinhan_hp_number = dy_poss_ghinhan_hp.count()
    # print('poss_ghinhan_hp_number: ', poss_ghinhan_hp_number)

    if poss_ghinhan_hp_number < 1:
        return

    df_dong_tien = dy_poss_ghinhan_hp.join(dy_poss_khoa_hoc,
                                           dy_poss_ghinhan_hp.khoa_hoc_makh == dy_poss_khoa_hoc.makh, 'left')\
        .join(dy_poss_hoc_vien, dy_poss_hoc_vien.mahv_hv == dy_poss_khoa_hoc.mahv, 'left')\
        .join(dy_poss_goi_sanpham, dy_poss_goi_sanpham.id == dy_poss_khoa_hoc.goi_sanpham_id, 'left')

    df_dong_tien = df_dong_tien.select('ngay_thanhtoan', 'ma', 'crm_id',
                                       'so_tien',
                                       getSolanBaoLuu(df_dong_tien['solan_baoluu'], df_dong_tien['songay_baoluu'])
                                            .alias('solan_baoluu_t'),
                                       getSoNgayBaoLuu(df_dong_tien['solan_baoluu'], df_dong_tien['songay_baoluu'])
                                            .alias('songay_baoluu_t'))

    # print('df_dong_tien')
    # df_dong_tien.printSchema()

    #check lms_id and contact_id

    df_dong_tien_student = df_dong_tien.join(dy_crm_goi_contacts, df_dong_tien.crm_id == dy_crm_goi_contacts.code, 'left')\
        .join(dy_advisor_student_contact, df_dong_tien.crm_id == dy_advisor_student_contact.student_id_advisor, 'left')


    # print('df_dong_tien_student-----')
    # df_dong_tien_student.printSchema()

    df_dong_tien_student = df_dong_tien_student.filter(df_dong_tien_student.code.isNotNull()
                                                       | (df_dong_tien_student.contact_id_advisor.isNotNull()))

    df_dong_tien_student = df_dong_tien_student.limit(100)

    student_id_unavailable = 0L
    package_endtime_unavailable = 0L
    package_starttime_unavailable = 0L
    student_level_code_unavailable = 'UNAVAILABLE'
    student_status_code_unavailable = 'UNAVAILABLE'
    measure1_unavailable = 0
    measure2_unavailable = 0
    measure3_unavailable = 0
    measure4_unavailable = float(0.0)


    df_dong_tien_student = df_dong_tien_student.select(
        f.unix_timestamp(df_dong_tien_student.ngay_thanhtoan, 'yyyy-MM-dd').alias('student_behavior_date'),
        f.lit(1L).alias('behavior_id'),
        f.lit(student_id_unavailable).cast('long').alias('student_id'),
        getContactId(df_dong_tien_student.code, df_dong_tien_student.contact_id_advisor).alias('contact_id'),

        df_dong_tien_student.ma.alias('package_code'),
        f.lit(package_endtime_unavailable).cast('long').alias('package_endtime'),
        f.lit(package_starttime_unavailable).cast('long').alias('package_starttime'),

        f.lit(student_level_code_unavailable).cast('string').alias('student_level_code'),
        f.lit(student_status_code_unavailable).cast('string').alias('student_status_code'),

        f.lit(today_second).alias('transformed_at'),

        'so_tien',
        'solan_baoluu_t',
        'songay_baoluu_t',

        f.lit(measure4_unavailable).alias('measure4')

    )


    print('df_dong_tien_student--1')
    df_dong_tien_student.printSchema()
    df_dong_tien_student.show(1)

    df_dong_tien_student2 = df_dong_tien_student.withColumn('student_behavior_id',
                                    f.md5(concaText(df_dong_tien_student.student_behavior_date,
                                             df_dong_tien_student.behavior_id,
                                             df_dong_tien_student.student_id,
                                             df_dong_tien_student.contact_id,
                                             df_dong_tien_student.package_code,
                                             df_dong_tien_student.package_endtime,
                                             df_dong_tien_student.package_starttime,
                                             df_dong_tien_student.student_level_code,
                                             df_dong_tien_student.student_status_code,
                                             df_dong_tien_student.transformed_at)))

    print('df_dong_tien_student2--2')
    df_dong_tien_student2.printSchema()
    df_dong_tien_student2.show(5)

    dyf_dong_tien_student = DynamicFrame.fromDF(df_dong_tien_student2, glueContext, 'dyf_dong_tien_student')

    dyf_dong_tien_student = Filter.apply(frame=dyf_dong_tien_student,
                                        f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')

    apply_ouput = ApplyMapping.apply(frame=dyf_dong_tien_student,
                                     mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                               ("student_behavior_date", "long", "student_behavior_date", "long"),
                                               ("behavior_id", "long", "behavior_id", "long"),
                                               ("student_id", "long", "student_id", "long"),
                                               ("contact_id", "string", "contact_id", "string"),

                                               ("package_code", "long", "package_code", "string"),
                                               ("package_endtime", "long", "package_endtime", "long"),
                                               ("package_starttime", "long", "package_starttime", "long"),

                                               ("student_level_code", "string", "student_level_code", "string"),
                                               ("student_status_code", "string", "student_status_code", "string"),

                                               ("transformed_at", "long", "transformed_at", "long")
                                               ])

    dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")

    glueContext.write_dynamic_frame.from_options(
        frame=dfy_output,
        connection_type="s3",
        connection_options={"path": "s3://dtsodin/student_behavior/student_behavior",
                            "partitionKeys": ["behavior_id"]},
        format="parquet")



    apply_general = ApplyMapping.apply(frame=dyf_dong_tien_student,
                                     mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                               ("so_tien", "double", "measure1", "float"),
                                               ("solan_baoluu_t", "long", "measure2", "float"),
                                               ("songay_baoluu_t", "long", "measure3", "float"),
                                               ("measure4", "float", "measure4", "float"),
                                               ("behavior_id", "long", "behavior_id", "long")
                                               ])

    dfy_output2 = ResolveChoice.apply(frame=apply_general, choice="make_cols", transformation_ctx="resolvechoice2")


    print('dfy_output2::')
    dfy_output2.show(5)

    glueContext.write_dynamic_frame.from_options(
        frame=dfy_output2,
        connection_type="s3",
        connection_options={"path": "s3://dtsodin/student_behavior/student_general_behavior",
                            "partitionKeys": ["behavior_id"]},
        format="parquet")


    flag = dy_poss_ghinhan_hp.agg({"_key": "max"}).collect()[0][0]
    flag_data = [flag]
    df = spark.createDataFrame(flag_data, "long").toDF('flag')
    df.write.parquet("s3a://dtsodin/flag/student_behavior/sb1_dong_tien.parquet", mode="overwrite")


if __name__ == "__main__":
    main()