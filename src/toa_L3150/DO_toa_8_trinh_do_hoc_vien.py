import sys
from pyspark import StorageLevel
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime,unix_timestamp,date_format
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from datetime import date, datetime, timedelta



def main():


    # # @params: [TempDir, JOB_NAME]
    # args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
    # sc = SparkContext()
    glueContext =  GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    # job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)

    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source




    dyf_log_student_level_study = glueContext.create_dynamic_frame.from_catalog(
        database = "tig_advisor",
        table_name = "log_student_level_study"
    )


    # chon cac field
    dyf_log_student_level_study = dyf_log_student_level_study.select_fields(['_key', 'contact_id', 'level_modified', 'package_code', 'time_created'])
    dyf_log_student_level_study = dyf_log_student_level_study.resolveChoice(specs=[('_key', 'cast:long')])
    dyf_log_student_level_study.printSchema()
    dyf_log_student_level_study.show(2)

    #  check bucket is not null
    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dtsodin/flag/toa_L3150/toa_8_log_student_level_study.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_log_student_level_study = Filter.apply(frame=dyf_log_student_level_study, f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    print('the number of new contacts: ', dyf_log_student_level_study.count())

    if(dyf_log_student_level_study.count() > 0):


        dyf_log_student_level_study = Filter.apply(frame=dyf_log_student_level_study,
                                                         f=lambda x: x['contact_id'] is not None
                                                                     and x['level_modified'] is not None
                                                                     and x['package_code'] is not None
                                                                     and x['time_created'] is not None)

        dyf_log_student_level_study.printSchema()


        # df_log_student_level_study = dyf_log_student_level_study.toDF()
        # df_log_student_level_study = df_log_student_level_study.withColumn('change_status_date_id',from_unixtime(df_log_student_level_study.time_created, "yyyyMMdd")) \
        #     .withColumn('to_status_id', f.lit(203))
        # df_log_student_level_study = df_log_student_level_study.dropDuplicates()
        # dyf_log_student_level_study = DynamicFrame.fromDF(df_log_student_level_study, glueContext,
        #                                                         'dyf_log_student_level_study')
        # applymapping1 = ApplyMapping.apply(frame=dyf_log_student_level_study,
        #                                    mappings=[("contact_id", "string", "contact_id", "string"),
        #                                              ("change_status_date_id", "string", "change_status_date_id","long"),
        #                                              ("time_created", "int", "timestamp1", "timestamp"),
        #                                              ('to_status_id', 'int', 'to_status_id', 'long')])
        #
        # resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
        #                                      transformation_ctx="resolvechoice2")
        #
        # dropnullfields_3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")
        #
        # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_3,
        #                                                            catalog_connection="glue_redshift",
        #                                                            connection_options={
        #                                                                "dbtable": "mapping_changed_status_student",
        #                                                                "database": "dts_odin",
        #                                                                "postactions": """UPDATE mapping_changed_status_student
        #     		                                                                                 SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 1 AND source_id = mapping_changed_status_student.contact_id LIMIT 1 )
        #     	                                                                                     WHERE user_id IS NULL and to_status_id=203"""
        #                                                            },
        #                                                            redshift_tmp_dir="s3n://dtsodin/backup/TOA/TOA8/",
        #                                                            transformation_ctx="datasink4")
        # # ghi flag
        # # lay max key trong data source
        # datasourceTmp = dyf_log_student_level_study.toDF()
        # flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]
        #
        # flag_data = [flag]
        # df = spark.createDataFrame(flag_data, "long").toDF('flag')
        #
        # # ghi de _key vao s3
        # df.write.parquet("s3a://dtsodin/flag/toa_L3150/toa_8_log_student_level_study.parquet", mode="overwrite")

if __name__ == "__main__":
        main()





