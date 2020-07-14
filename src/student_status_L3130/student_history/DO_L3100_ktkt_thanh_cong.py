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
from pyspark.sql.types import DateType


## @params: [TempDir, JOB_NAME]
# args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    # job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    # datasource = glueContext.create_dynamic_frame.from_catalog(database="myjira",
    #                                                            table_name="historystatustoa")
    #
    # # Chon cac truong can thiet
    # datasource = datasource.select_fields(
    #     ['_key', 'change_group_id', 'student_id', 'modification_time', 'old_value_code', 'new_value_code'])
    #
    # datasource = datasource.resolveChoice(specs=[('_key', 'cast:long')])
    #
    # # doc flag tu s3
    # df_flag = spark.read.parquet("s3://dts-odin/flag/flag_LS_A2.parquet")
    # # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # max_key = df_flag.collect()[0]['flag']
    # # datasource = Filter.apply(frame=datasource, f=lambda x: x["_key"] > max_key)
    # print("max_key:  ", max_key)
    # # print("Count data:  ", datasource.count())
    # datasource.printSchema()
    #
    # if (datasource.count() > 0):
    #     # datasource_df = datasource.toDF()
    #     # datasource_df = datasource_df.where("student_id IS NOT NULL")
    #     # datasource_df = datasource_df.where("modification_time IS NOT NULL")
    #     # # trang thai cu la 10520
    #     # datasource_df = datasource_df.where("old_value_code = '10520'")
    #     # # trang thai cu la 10521
    #     # datasource_df = datasource_df.where("new_value_code = '10521'")
    #
    #     datasource = Filter.apply(frame=datasource,
    #                               f=lambda x: x["student_id"] is not None and x["modification_time"] is not None and x[
    #                                   "old_value_code"] == '10520' and x["new_value_code"] == '10521')
    #
    #     # data = DynamicFrame.fromDF(datasource_df, glueContext, "data")
    #     print("Count data 1:  ", datasource.count())
    #     applymapping1 = ApplyMapping.apply(frame=datasource,
    #                                        mappings=[("student_id", "string", "student_id", "string"),
    #                                                  ("modification_time", "string", "ngay_a2", "timestamp"),
    #                                                  ("change_group_id", "string", "id", "string")])
    #     resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
    #                                          transformation_ctx="resolvechoice2")
    #     dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")
    #
    #     datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
    #                                                                catalog_connection="glue_redshift",
    #                                                                connection_options={
    #                                                                    "dbtable": "temp_ls_trang_thai_a2",
    #                                                                    "database": "dts_odin"
    #                                                                },
    #                                                                redshift_tmp_dir="s3n://dts-odin/student_technical_test_odin/",
    #                                                                transformation_ctx="datasink4")
    #     # ghi flag
    #     # lay max key trong data source
    #     datasourceTmp = datasource.toDF()
    #     flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]
    #
    #     flag_data = [flag]
    #     df = spark.createDataFrame(flag_data, "long").toDF('flag')
    #
    #     # ghi de _key vao s3
    #     df.write.parquet("s3a://dts-odin/flag/flag_LS_A2.parquet", mode="overwrite")

        ##################################################
    # Lay du lieu kiem tra ky thuat trong bang student_technical_test
    datasourceTech = glueContext.create_dynamic_frame.from_catalog(database="technical_test",
                                                                   table_name="student_technical_test")

    # Chon cac truong can thiet
    datasourceTech = datasourceTech.select_fields(['_key', 'studentid', 'thoigianhenktkt', 'ketluan', 'emailhocvien'])

    datasourceTech = datasourceTech.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flagTech = spark.read.parquet("s3://dts-odin/flag/flag_LS_A2_1.parquet")
    max_key = df_flagTech.collect()[0]['flag']
    # datasourceTech = Filter.apply(frame=datasourceTech, f=lambda x: x["_key"] > max_key)
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # dataTech = datasourceTech.toDF()
    # dataTech = dataTech.where(data['_key'] > df_flagTech.collect()[0]['flag'])
    # dataTech = dataTech.where(data['_key'] < 276961)
    # datasourceTech = DynamicFrame.fromDF(dataTech, glueContext, "datasourceTech")

    if (datasourceTech.count() > 0):
        # datasource_df = datasourceTech.toDF()
        # datasource_df = datasource_df.where("studentid IS NOT NULL")
        # datasource_df = datasource_df.where("thoigianhenktkt IS NOT NULL")
        # datasource_df = datasource_df.where("ketluan = 'Pass'")

        datasourceTech = Filter.apply(frame=datasourceTech,
                                      f=lambda x: x["studentid"] is not None and x["thoigianhenktkt"] is not None and x[
                                          "ketluan"] == 'Pass')
        # datasourceTech = Filter.apply(frame=datasourceTech, f=lambda x: x["thoigianhenktkt"] is not None)
        # datasourceTech = Filter.apply(frame=datasourceTech, f=lambda x: x["ketluan"] == 'Pass')

        # data = DynamicFrame.fromDF(datasource_df, glueContext, "data")
        print("Count data 2:  ", datasourceTech.count())
        #
        student_contact = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="student_contact"
        )

        # chon cac field
        student_contact = student_contact.select_fields(['_key', 'contact_id', 'student_id', 'user_name'])

        datasourceTech = Join.apply(datasourceTech, student_contact, 'emailhocvien', 'user_name')

        applymapping1 = ApplyMapping.apply(frame=datasourceTech,
                                           mappings=[("student_id", "string", "student_id", "string"),
                                                     ("thoigianhenktkt", "string", "ngay_a2", "timestamp"),
                                                     ("_key", "long", "id", "string")])
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "temp_ls_trang_thai_a2",
                                                                       "database": "dts_odin",
                                                                       "postactions": """ call proc_insert_ktkt_thanh_cong();
                                                                                        DROP TABLE IF EXISTS temp_ls_trang_thai_a2
                                                                                             """
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/student_technical_test_odin/",
                                                                   transformation_ctx="datasink4")
        # ghi flag
        # lay max key trong data source
        datasourceTmp = datasourceTech.toDF()
        flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_LS_A2_1.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
