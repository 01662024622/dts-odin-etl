import sys
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import collect_list
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format
from pyspark.sql.types import StringType
import pyspark.sql.functions as f
from datetime import date, datetime, timedelta
import pytz

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # ----------------------------------------------DYF-----------------------------------------------------------------#
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                        table_name="student_contact")

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id'])
    #-----------------------------------------DYF-----------------------------------------------------------------------#

    dyf_ghinhan_hp = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                                        table_name="ghinhan_hp")

    dyf_ghinhan_hp = dyf_ghinhan_hp.select_fields(
        ['ngay_thanhtoan', 'khoa_hoc_makh', 'trang_thai']).rename_field('trang_thai', 'trang_thai_gnhp')

    dyf_ghinhan_hp = Filter.apply(frame=dyf_ghinhan_hp, f=lambda x: x["trang_thai_gnhp"] == True)
    # -----------------------------------------DYF-----------------------------------------------------------------------#

    dyf_khoa_hoc = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                                        table_name="khoa_hoc")

    dyf_khoa_hoc = dyf_khoa_hoc.select_fields(
        ['makh', 'mahv', 'trang_thai']).rename_field('trang_thai', 'trang_thai_kh')

    dyf_khoa_hoc = Filter.apply(frame=dyf_khoa_hoc, f=lambda x: x["trang_thai_kh"] == True)
    # -----------------------------------------DYF-----------------------------------------------------------------------#

    dyf_hoc_vien = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                                        table_name="hoc_vien")

    dyf_hoc_vien = dyf_hoc_vien.select_fields(
        ['mahv', 'crm_id', 'trang_thai']).rename_field('mahv', 'mahv_hv').rename_field('trang_thai', 'trang_thai_hv')

    dyf_hoc_vien = Filter.apply(frame=dyf_hoc_vien, f=lambda x: x["trang_thai_hv"] == True)
    #-------------------------------------------------------------------------------------------------------------------#

    df_student_contact_1 =dyf_student_contact.toDF()
    df_student_contact_1.drop_duplicates()
    df_student_contact = df_student_contact_1.groupby('contact_id','student_id').agg(
        f.count('contact_id').alias("contact_id_after_count"))
    dyf_student_contact = DynamicFrame.fromDF(df_student_contact, glueContext, "dyf_student_contact")
    dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x["contact_id_after_count"] > 1)

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact.drop_duplicates()
    df_student_contact.cache()
    df_student_contact.printSchema()
    df_student_contact.show(2)
    print('df_student_contact count::', df_student_contact.count())

    df_ghinhan_hp = dyf_ghinhan_hp.toDF()
    df_khoa_hoc = dyf_khoa_hoc.toDF()
    df_hoc_vien = dyf_hoc_vien.toDF()

    #------------------------------------------___JOIN___---------------------------------------------------------------#

    df_join = df_ghinhan_hp.join(df_khoa_hoc,df_ghinhan_hp.khoa_hoc_makh == df_khoa_hoc.makh)
    df_join.printSchema()
    print('df_join count::',df_join.count())

    df_join1 = df_join.join(df_hoc_vien,df_join.mahv == df_hoc_vien.mahv_hv)
    df_join1.printSchema()
    print('df_join1 count::', df_join1.count())

    df_join2 = df_join1.join(df_student_contact, df_join1.crm_id == df_student_contact.contact_id )

    df_join2 = df_join2.withColumn('change_status_date_id',
                                             from_unixtime(
                                                 unix_timestamp(df_join2.ngay_thanhtoan, "yyyy-MM-dd"),
                                                 "yyyyMMdd"))
    df_join2.drop_duplicates()
    df_join2.printSchema()
    df_join2.show(2)
    print('df_join2 count::', df_join2.count())


    # df_join2.printSchema()
    # print('df_join2 count::', df_join2.count())

    #-----------------------------------_____choose_name_field______----------------------------------------------------#
    to_status_id = 201L
    df_result = df_join2.select(
        'student_id',
        'change_status_date_id',
        f.lit(to_status_id).alias('to_status_id'),
        'contact_id'
    )

    df_result.printSchema()
    df_result.show(3)
    df_result = df_result.drop_duplicates()
    df_result.cache()
    print('count df_result::',df_result.count())
    dyf_result = DynamicFrame.fromDF(df_result, glueContext, "dyf_result")
    dyf_result = Filter.apply(frame=dyf_result, f=lambda x: x["student_id"] is not None
                                                              and x["change_status_date_id"] is not None
                                                              and x["to_status_id"] is not None
                                                              and x["contact_id"] is not None)

    apply_output = ApplyMapping.apply(frame=dyf_result,
                                       mappings=
                                       [
                                        ("student_id", "string", "student_id", "long"),
                                        # ("user_id", "long", "user_id", "long"),
                                        ("change_status_date_id", "string", "change_status_date_id", "long"),
                                        # ("from_status_id", "long", "from_status_id", "long"),
                                        ("to_status_id", "long", "to_status_id", "long"),
                                        # ("measure1", "double", "measure1", "double"),
                                        # ("measure2", "double", "measure2", "double"),
                                        # ("description", "string", "description", "string"),
                                        # ("timestamp1", "string", "timestamp1", "string"),
                                        ("contact_id", "string", "contact_id", "string"),

                                        # ("teacher_id", "long", "teacher_id", "long"),
                                        # ("contact_id1", "string", "contact_id1", "string"),
                                        # ("measure1_int", "int", "measure1_int", "int"),
                                        # ("measure2_int", "int", "measure2_int", "int"),
                                        # ("contact_id_str", "string", "contact_id_str", "string"),
                                        # ("lc", "string", "lc", "string"),
                                        # ("student_id_string", "string", "student_id_string", "string")
                                        ])
    df_apply_output = apply_output.toDF()
    df_apply_output.drop_duplicates()
    print('df_apply_output.count',df_apply_output.count())
    dyf_apply_output = DynamicFrame.fromDF(df_apply_output, glueContext, "dyf_apply_output")

    resolve_choice = ResolveChoice.apply(frame=dyf_apply_output, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dropnullfields = DropNullFields.apply(frame=resolve_choice, transformation_ctx="dropnullfields")

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "mapping_changed_status_student_v1",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://datashine-dwh/temp1/",
                                                               transformation_ctx="datasink4")

    df_result.unpersist()
    df_student_contact.unpersist()
    print('------------------------>___complete__________------------------------------>')
if __name__ == "__main__":
        main()
