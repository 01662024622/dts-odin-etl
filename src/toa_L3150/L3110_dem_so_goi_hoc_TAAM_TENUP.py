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
    dyf_tpe_enduser_used_product = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                        table_name="tpe_enduser_used_product")

    dyf_tpe_enduser_used_product = dyf_tpe_enduser_used_product.select_fields(
        ['contact_id', 'product_id','timecreated']
    )

    # -----------------------------------------DYF-----------------------------------------------------------------------#

    dyf_tpe_invoice_product_details = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                   table_name="tpe_invoice_product_details")

    dyf_tpe_invoice_product_details = dyf_tpe_invoice_product_details.select_fields(
        ['id', 'cat_code']
    )
    # ----------------------------------------------DYF-----------------------------------------------------------------#
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                        table_name="student_contact")

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id']).rename_field('contact_id','ct_id')

    # dyf_student_contact = Filter.apply(frame=dyf_student_contact,
    #                                    f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
    #                                                and x["student_id"] is not None and x["student_id"] != ''
    #                                                )



    df_student_contact = dyf_student_contact.toDF()
    print('df_student_contact')
    df_student_contact.show()

    #-------------------------------------------------------------------------------------------------------------------#
    df_tpe_invoice_product_details = dyf_tpe_invoice_product_details.toDF()
    df_tpe_invoice_product_details = df_tpe_invoice_product_details.\
        where("cat_code like 'TAAM%' OR cat_code like 'TENUP%' ")
    df_tpe_invoice_product_details = df_tpe_invoice_product_details.withColumn('to_status_id',
                                                                               f.when(df_tpe_invoice_product_details.cat_code.like('TAAM%'), f.lit(999L)).
                                                                                when(df_tpe_invoice_product_details.cat_code.like('TENUP%'), f.lit(998L)).
                                                                               otherwise(f.lit(999999999L)))

    df_tpe_invoice_product_details.show(2)

    df_tpe_enduser_used_product = dyf_tpe_enduser_used_product.toDF()

    #-----------------------------------------------____JOIN______------------------------------------------------------#

    df_join = df_tpe_invoice_product_details.join(df_tpe_enduser_used_product,
                                                  df_tpe_invoice_product_details.id == df_tpe_enduser_used_product.product_id
                                                  )
    df_join.printSchema()
    print('df_join ::', df_join.count())

    df_join1 = df_join.join(df_student_contact, df_student_contact.ct_id == df_join.contact_id)
    df_join1 = df_join1.withColumn('change_status_date_id', from_unixtime(df_join1.timecreated,"yyyyMMdd"))
    df_join1.printSchema()
    print('df_join1 ::', df_join1.count())
    #-------------------------------------------------------------------------------------------------------------------#
    df_result = df_join1.select(
        'student_id',
        'change_status_date_id',
        'to_status_id',
        'contact_id'
    )

    df_result.printSchema()
    df_result.show(3)
    df_result = df_result.drop_duplicates()
    df_result.cache()
    print('count df_result::', df_result.count())
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
    print('df_apply_output.count', df_apply_output.count())
    dyf_apply_output = DynamicFrame.fromDF(df_apply_output, glueContext, "dyf_apply_output")

    resolve_choice = ResolveChoice.apply(frame=dyf_apply_output, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dropnullfields = DropNullFields.apply(frame=resolve_choice, transformation_ctx="dropnullfields")

    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "temp_mapping_status",
                                                                   "database": "dts_odin",
                                                                   "postactions": """ insert into mapping_changed_status_student_v1(student_id, change_status_date_id, to_status_id, contact_id)
                                                                                                    select student_id, change_status_date_id, to_status_id, contact_id from temp_mapping_status;
                                                                                                    update mapping_changed_status_student_v1 set user_id = (select user_id from user_map where source_type = 2 and source_id = student_id)
                                                                                                        where user_id is null;
                                                                                                    DROP TABLE IF EXISTS temp_mapping_status
                                                                                                         """

                                                               },
                                                               redshift_tmp_dir="s3n://datashine-dwh/temp1/",
                                                               transformation_ctx="datasink4")

    df_result.unpersist()
    df_student_contact.unpersist()
    print('------------------------>___complete__________------------------------------>')
if __name__ == "__main__":
        main()