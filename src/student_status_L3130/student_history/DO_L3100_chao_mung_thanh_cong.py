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

    datasource = glueContext.create_dynamic_frame.from_catalog(database="myjira",
                                                               table_name="historystatustoa")

    # Chon cac truong can thiet
    datasource = datasource.select_fields(
        ['_key', 'change_group_id', 'student_id', 'modification_time', 'old_value_code', 'new_value_code'])

    datasource = datasource.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_LS_A1.parquet")
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    max_key = df_flag.collect()[0]['flag']
    # data = datasource.toDF()
    # data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    # data = data.where(data['_key'] < 276961)
    # datasource = DynamicFrame.fromDF(data, glueContext, "datasource")
    # datasource = Filter.apply(frame=datasource, f=lambda x: x["_key"] > max_key)
    print("max_key:  ", max_key)
    print("Count data datasource:  ", datasource.count())
    if (datasource.count() > 0):
        # datasource_df = datasource.toDF()
        # datasource_df = datasource_df.where("student_id IS NOT NULL")
        # datasource_df = datasource_df.where("modification_time IS NOT NULL")
        # # trang thai cu la 10519 - S0. HV dc phan cong CVHD
        # datasource_df = datasource_df.where("old_value_code = '10519'")
        # # trang thai moi la 11013 - S1. HV dc chao mung thanh cong
        # datasource_df = datasource_df.where("new_value_code = '11013'")
        #
        # data = DynamicFrame.fromDF(datasource_df, glueContext, "data")

        datasource = Filter.apply(frame=datasource,
                                  f=lambda x: x["student_id"] is not None and x["modification_time"] is not None and x[
                                      "old_value_code"] == '10519' and x["new_value_code"] == '11013')
        # datasource = Filter.apply(frame=datasource, f=lambda x: x["modification_time"] is not None)
        # datasource = Filter.apply(frame=datasource, f=lambda x: x["old_value_code"] == '10519')
        # datasource = Filter.apply(frame=datasource, f=lambda x: x["new_value_code"] == '11013')

        print("Count data:  ", datasource.count())
        applymapping1 = ApplyMapping.apply(frame=datasource,
                                           mappings=[("student_id", "string", "student_id", "string"),
                                                     ("modification_time", "string", "ngay_a1", "timestamp"),
                                                     ("change_group_id", "string", "id", "string")])
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "temp_ls_trang_thai_a1",
                                                                       "database": "dts_odin",
                                                                       "postactions": """ call proc_insert_chao_mung_thanh_cong();
                                                                                              DROP TABLE IF EXISTS temp_ls_trang_thai_a1
                                                                                             """
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/historystatustoa/",
                                                                   transformation_ctx="datasink4")
        # ghi flag
        # lay max key trong data source
        datasourceTmp = datasource.toDF()
        flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_LS_A1.parquet", mode="overwrite")

    ###############################################################################################
    ### lay tu nguon craw_gs_care
    datasourceCraw = glueContext.create_dynamic_frame.from_catalog(database="dm_toa",
                                                                   table_name="craw_gs_care")

    # Chon cac truong can thiet
    datasourceCraw = datasourceCraw.select_fields(['_key', 'id', 'datetime'])

    datasourceCraw = datasourceCraw.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag_craw = spark.read.parquet("s3://dts-odin/flag/flag_LS_A1_1.parquet")
    max_key = df_flag_craw.collect()[0]['flag']
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # dataCraw = datasourceCraw.toDF()
    # dataCraw = dataCraw.where(data['_key'] > df_flag_craw.collect()[0]['flag'])
    # dataCraw = dataCraw.where(dataCraw['_key'] < 276961)
    # datasourceCraw = DynamicFrame.fromDF(dataCraw, glueContext, "datasourceCraw")
    # datasourceCraw = Filter.apply(frame=datasourceCraw, f=lambda x: x["_key"] > max_key)
    print("max_key:  ", max_key)
    # print "Count data datasourceCraw:  ", datasourceCraw.count()
    if (datasourceCraw.count() > 0):
        # datasource_df = datasourceCraw.toDF()
        # datasource_df = datasource_df.where("id IS NOT NULL")
        # datasource_df = datasource_df.where("datetime IS NOT NULL")

        datasourceCraw = Filter.apply(frame=datasourceCraw, f=lambda x: x["id"] is not None)
        datasourceCraw = Filter.apply(frame=datasourceCraw, f=lambda x: x["datetime"] is not None)
        # data = DynamicFrame.fromDF(datasource_df, glueContext, "data")
        print("Count data:  ", datasourceCraw.count())
        datasourceCraw.show()
        applymapping1 = ApplyMapping.apply(frame=datasourceCraw,
                                           mappings=[("id", "int", "student_id", "string"),
                                                     ("datetime", "string", "ngay_a1", "timestamp"),
                                                     ("_key", "string", "id", "string")])
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "temp_ls_trang_thai_a1",
                                                                       "database": "dts_odin",
                                                                       "postactions": """ call proc_insert_chao_mung_thanh_cong();
                                                                                              DROP TABLE IF EXISTS temp_ls_trang_thai_a1
                                                                                             """
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/craw_gs_care/",
                                                                   transformation_ctx="datasink4")
        # ghi flag
        # lay max key trong data source
        datasourceTmp = datasourceCraw.toDF()
        flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3n://dts-odin/flag/flag_LS_A1_1.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
