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


    datasource0 = glueContext.create_dynamic_frame.from_catalog(database="topicalms", table_name="mdl_tpebbb")

    datasource0 = datasource0.select_fields(
        ['_key', 'id', 'name', 'description', 'roomtype', 'calendar_code', 'vcr_type', 'vcr_class_id'])

    # convert dl
    datasource0 = datasource0.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag = spark.read.parquet("s3://dts-odin/flag/flag_lophoc.parquet")
    max = df_flag.collect()[0]['flag']
    print "max:  ", max
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #data = datasource0.toDF()
    #data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    #datasource0 = DynamicFrame.fromDF(data, glueContext, "datasource0")
    datasource0 = Filter.apply(frame=datasource0, f=lambda x: x["_key"] > max)
    print "Count data 1:  ", datasource0.count()
    if (datasource0.count() > 0):
        # try:
        datasource1 = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                    table_name="mdl_tpe_calendar_teach")

        datasource1 = datasource1.select_fields(
            ['calendar_code', 'level_class', 'teacher_type', 'student_type', 'type_class', 'teacher_id']).rename_field(
            'calendar_code', 'code_calendar')

        # loc data
        # df_lop = datasource1.toDF()
        # df_lop = df_lop.where("code_calendar <> '' and code_calendar is not null")
        # df_lop = df_lop.where("teacher_type <> 'TRAINING' AND teacher_type <> 'ORIENTATION'")
        # data_lop = DynamicFrame.fromDF(df_lop, glueContext, "data_lop")

        data_lop = Filter.apply(frame=datasource1,
                                f=lambda x: x["code_calendar"] != '' and x["code_calendar"] is not None)
        data_lop = Filter.apply(frame=data_lop,
                                f=lambda x: x["teacher_type"] != 'TRAINING' and x["teacher_type"] != 'ORIENTATION')

        # loc data
        # df_lslop = datasource0.toDF()
        # df_lslop = df_lslop.where("calendar_code is not null and calendar_code <> ''")
        # df_lslop = df_lslop.where("roomtype = 'ROOM'")
        # df_lslop = df_lslop.where("id is not null and id <> ''")
        # data_lslop = DynamicFrame.fromDF(df_lslop, glueContext, "data_lslop")

        data_lslop = Filter.apply(frame=datasource0,
                                f=lambda x: x["calendar_code"] != '' and x["calendar_code"] is not None)
        data_lslop = Filter.apply(frame=data_lslop,
                                  f=lambda x: x["id"] != '' and x["id"] is not None)
        data_lslop = Filter.apply(frame=data_lslop, f=lambda x: x["roomtype"] == 'ROOM')

        join = Join.apply(data_lslop, data_lop, 'calendar_code', 'code_calendar')

        # chon cac truong va kieu du lieu day vao db
        applyMapping = ApplyMapping.apply(frame=join,
                                          mappings=[("id", "string", "id", "string"),
                                                    ("name", "string", "tenlop", "string"),
                                                    ("description", 'string', 'mota', 'string'),
                                                    ("roomtype", "string", "kieuphong", "string"),
                                                    ("vcr_type", "string", "kieuvcr", "string"),
                                                    ("vcr_class_id", "string", "malopvcr", "string"),
                                                    ("level_class", "string", "levellop", "string"),
                                                    ("teacher_type", "string", "kieugiaovien", "string"),
                                                    ("student_type", "string", "kieuhocvien", "string"),
                                                    ("type_class", "string", "kieulop", "string"),
                                                    ("teacher_id", "string", "id_giaovien", "string")])

        resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols", transformation_ctx="resolvechoice2")

        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")
        print "Count data 2:  ", dropnullfields.count()
        # ghi du lieu vao redshift
        datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                  catalog_connection="glue_redshift",
                                                                  connection_options={"dbtable": "dim_lop_hoc",
                                                                                      "database": "dts_odin"},
                                                                  redshift_tmp_dir="s3n://dts-odin/topicalms/dim_lophoc/",
                                                                  transformation_ctx="datasink4")

        # ghi du lieu vao s3
        # datasink1 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields, connection_type="s3",
        #                                                          connection_options={
        #                                                              "path": "s3://dts-odin/dim_lophoc"},
        #                                                          format="parquet", transformation_ctx="datasink1")
        # lay max _key cua datasource
        datasource = datasource0.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        # tao data frame
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de flag vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_lophoc.parquet", mode="overwrite")

if __name__ == "__main__":
    main()