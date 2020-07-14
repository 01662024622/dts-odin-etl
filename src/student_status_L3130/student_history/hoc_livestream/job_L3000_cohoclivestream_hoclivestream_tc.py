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

# @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
sc = SparkContext()

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Xu ly dl LiveStream
# Lay data tu s3
fact_data_livestream = glueContext.create_dynamic_frame.from_catalog(database='native_livestream',
                                                                     table_name='log_in_out')
# xu ly truong hop start_read is null
try:
    # # doc moc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/fact_flag_LiveStrem.parquet")
    start_read = df_flag.collect()[0]['flag']
    print('read from index: ', start_read)

    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    fact_data_livestream = Filter.apply(frame=fact_data_livestream, f=lambda x: x['_key'] > start_read)
except:
    print('read flag file error ')

print('the number of new contacts: ', fact_data_livestream.count())
# chon cac truong can thiet
fact_data_livestream = fact_data_livestream.select_fields(
    ['_key', 'student_id', 'room_id', 'time_in', 'time_out', 'thoigianhoc', 'timecreated'])
fact_data_livestream = fact_data_livestream.resolveChoice(
    specs=[('_key', 'cast:long'), ('time_in', 'cast:long'), ('time_out', 'cast:long'),
           ('thoigianhoc', 'cast:long')])



if (fact_data_livestream.count() > 0):
    try:
        fact_data_livestream = Filter.apply(frame=fact_data_livestream, f=lambda x: x["student_id"] is not None and x["student_id"] != '')
        fact_data_livestream = Filter.apply(frame=fact_data_livestream, f=lambda x: x["time_in"] is not None)
        fact_data_livestream = Filter.apply(frame=fact_data_livestream, f=lambda x: x["time_out"] is not None)

        # convert datetime
        fact_ds_livestream = fact_data_livestream.toDF()
        fact_ds_livestream = fact_ds_livestream.withColumn('date_id',from_unixtime(fact_ds_livestream.time_in / 1000, "yyyyMMdd")) \
        .withColumn('time_in', from_unixtime(fact_ds_livestream.time_in / 1000)) \
        .withColumn('time_out', from_unixtime(fact_ds_livestream.time_out / 1000)) \
        .withColumn('time_study',fact_ds_livestream.thoigianhoc/1000)
        fact_ds_livestream = fact_ds_livestream.withColumn('class_type', f.lit('LIVESTREAM'))
        fact_data_livestream = DynamicFrame.fromDF(fact_ds_livestream, glueContext, "fact_data_livestream")

        fact_data_livestream = fact_data_livestream.resolveChoice(specs=[('time_study', 'cast:long')])
        fact_data_livestream = fact_data_livestream.resolveChoice(specs=[('class_type', 'cast:string')])
        data_livestream_tc = fact_data_livestream

        # chon cac truong va kieu du lieu day vao db
        applymapping0 = ApplyMapping.apply(frame=fact_data_livestream,
                                   mappings=[
                                             ("student_id", "string", "student_id", "string"),
                                             ("room_id", "string", "room_id", "string"),
                                             ("date_id", "string", "date_id", "long"),
                                             ("class_type", "string", "class_type", "string"),
                                             ("time_in", "string", "time_in", "timestamp"),
                                             ("time_out", "string", "time_out", "timestamp"),
                                             ("time_study", "long", "time_study", "long"),
                                             ("timecreated", "string", "created_time", "timestamp")])
        resolvechoice0 = ResolveChoice.apply(frame=applymapping0, choice="make_cols",
                                     transformation_ctx="resolvechoice0")
        dropnullfields0 = DropNullFields.apply(frame=resolvechoice0, transformation_ctx="dropnullfields0")
        #ghi dl vao db

        datasink0 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields0,
                                                           catalog_connection="glue_redshift",
                                                           connection_options={
                                                               "dbtable": "fact_lich_su_hoc",
                                                               "database": "dts_odin"},
                                                           redshift_tmp_dir="s3n://datashine-dwh/temp2/",
                                                           transformation_ctx="datasink0")


         # ghi data vao s3
        datasink1 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields0, connection_type="s3",
                                                         connection_options={
                                                             "path": "s3://datashine-dev-redshift-backup/fact_lichsulivestream"},
                                                         format="parquet", transformation_ctx="datasink1")
        # lay max _key tren datasource
        datasource = fact_data_livestream.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de flag moi vao s3
        df.write.parquet("s3a://dts-odin/flag/fact_flag_LiveStream.parquet", mode="overwrite")

        if (data_livestream_tc.count() > 0):
            ds_livestream = data_livestream_tc.toDF()
            ds_livestream = ds_livestream.groupby('room_id','student_id','date_id').agg(f.count("time_study").alias("measure1"), f.sum("time_study").alias("measure2"))
            data_livestream_tc = DynamicFrame.fromDF(ds_livestream, glueContext, "data_livestream")
            data_livestream_tc = data_livestream_tc.resolveChoice(specs=[("measure1", 'cast:long'), ("measure2", 'cast:long')])
            data_livestream = data_livestream_tc
            data_livestream_tc = Filter.apply(frame=data_livestream_tc, f=lambda x: x['measure2'] / 60 > 25)

            # # convert datetime
            if (data_livestream_tc.count() > 0):
                ds_livestream_tc = data_livestream_tc.toDF()
                ds_livestream_tc = ds_livestream_tc.withColumn('to_status_id', f.lit('15'))
                data_livestream_tc = DynamicFrame.fromDF(ds_livestream_tc, glueContext, "data_livestream_tc")
                data_livestream_tc = data_livestream_tc.resolveChoice(
                    specs=[('to_status_id', 'cast:string')])
                # chon cac truong va kieu du lieu day vao db
                applymapping1 = ApplyMapping.apply(frame=data_livestream_tc,
                                                   mappings=[("student_id", "string", "student_id", "long"),
                                                             ("to_status_id", "string", "to_status_id", "long"),
                                                             ("date_id", "string", "change_status_date_id", "long"),
                                                             ("measure1", "long", "measure1", "double"),
                                                             ("measure2", "long", "measure2", "double")])
                resolvechoice_tc = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                     transformation_ctx="resolvechoice_tc")


                dropnullfields_tc = DropNullFields.apply(frame=resolvechoice_tc, transformation_ctx="dropnullfields_tc")


                # ghi dl vao db
                datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_tc,
                                                                           catalog_connection="glue_redshift",
                                                                           connection_options={
                                                                               "dbtable": "mapping_changed_status_student",
                                                                               "database": "dts_odin"},
                                                                           redshift_tmp_dir="s3n://datashine-dwh/temp2/",
                                                                           transformation_ctx="datasink4")

                # ghi data vao s3
                datasink3 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields_tc, connection_type="s3",
                                                                         connection_options={
                                                                             "path": "s3://datashine-dev-redshift-backup/lichsulivestreamtc"},
                                                                         format="parquet", transformation_ctx="datasink3")


                if (data_livestream.count()>0) :
                    # try:
                        ds_livestream = data_livestream.toDF()
                        ds_livestream = ds_livestream.withColumn('to_status_id', f.lit('19'))
                        data_livestream = DynamicFrame.fromDF(ds_livestream, glueContext, "data_livestream")
                        data_livestream = data_livestream.resolveChoice(
                            specs=[('to_status_id', 'cast:string')])
                        # chon cac truong va kieu du lieu day vao db
                        applymapping2 = ApplyMapping.apply(frame=data_livestream,
                                                           mappings=[("student_id", "string", "student_id", "long"),
                                                                     ("to_status_id", "string", "to_status_id", "long"),
                                                                     ("date_id", "string", "change_status_date_id", "long"),
                                                                     ("measure1", "long", "measure1", "double"),
                                                                     ("measure2", "long", "measure2", "double")])
                        resolvechoice_2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                                               transformation_ctx="resolvechoice_2")


                        dropnullfields_2 = DropNullFields.apply(frame=resolvechoice_2,
                                                                 transformation_ctx="dropnullfields_2")


                        # ghi dl vao db
                        datasink10 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_2,
                                                                                   catalog_connection="glue_redshift",
                                                                                   connection_options={
                                                                                       "dbtable": "mapping_changed_status_student",
                                                                                       "database": "dts_odin"},
                                                                                   redshift_tmp_dir="s3n://datashine-dwh/temp2/",
                                                                                   transformation_ctx="datasink10")

                        # ghi data vao s3
                        datasink11 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields_2, connection_type="s3",
                                                                                 connection_options={
                                                                                     "path": "s3://datashine-dev-redshift-backup/lichsulivestream"},
                                                                                 format="parquet", transformation_ctx="datasink11")

    except:  # xu ly ngoai le(khi co datasource nhung k co gia tri thoa man dieu kien sau khi loc)

        # ghi flag
        datasource = fact_data_livestream.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://dts-odin/flag/fact_flag_LiveStrem.parquet", mode="overwrite")
job.commit()