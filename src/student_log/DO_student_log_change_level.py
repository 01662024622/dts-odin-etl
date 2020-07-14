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
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import TimestampType
import boto3

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    student_id_unavailable = 0L
    package_endtime_unavailable = 99999999999L
    package_starttime_unavailable = 0L

    def do_check_endtime(val):
        if val is not None:
            return val
        return 4094592235

    check_endtime_null = udf(do_check_endtime, LongType())

    ########## dyf_student_contact
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )
    dyf_student_contact = dyf_student_contact.select_fields(
        ['_key', 'contact_id', 'student_id', 'advisor_id', 'level_study', 'time_lms_created', 'time_created'])

    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])

    dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x['student_id'] is not None
                                                                              and x['contact_id'] is not None
                                                                              and x['time_lms_created'] is not None)

    dyf_student_level = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_student_level_study"
    )

    dyf_student_level = dyf_student_level.select_fields(
        ['_key', 'contact_id', 'level_current', 'level_modified', 'time_created']) \
        .rename_field('contact_id', 'contact_id1') \
        .rename_field('time_created', 'time_level_created')

    # try:
    #     df_flag = spark.read.parquet("s3://dtsodin/flag/flag_log_student_level.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print("max_key:  ", max_key)
    #     # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    #     dyf_student_level = Filter.apply(frame=dyf_student_level, f=lambda x: x["_key"] > max_key)
    # except:
    #     print('read flag file error ')

    if dyf_student_level.count() > 0:

        dyf_mapping_class_lms = glueContext.create_dynamic_frame.from_catalog(
            database="nvn_knowledge",
            table_name="mapping_class_lms"
        )
        dyf_mapping_class_lms = dyf_mapping_class_lms.select_fields(
            ['level_lms', 'level_list_master'])

        dyf_learning_object_class = glueContext.create_dynamic_frame.from_catalog(
            database="nvn_knowledge",
            table_name="learning_object_class"
        )
        dyf_learning_object_class = dyf_learning_object_class.select_fields(
            ['class_id', 'class_name'])

        dyf_student_contact_level = Join.apply(dyf_student_contact, dyf_student_level, 'contact_id', 'contact_id1')
        df_student_contact_level = dyf_student_contact_level.toDF()

        df_student_contact_level = df_student_contact_level \
            .select('student_id', 'level_current', 'level_modified', 'time_created', 'time_level_created')

        df_count_contact_level = df_student_contact_level \
            .groupby('student_id').agg(f.count('time_level_created').alias("count").cast('long'))
        dyf_count_contact_level = DynamicFrame.fromDF(df_count_contact_level, glueContext, "dyf_count_contact_level")
        dyf_count_contact_level = Filter.apply(frame=dyf_count_contact_level,
                                               f=lambda x: x['count'] == 2L)
        if dyf_count_contact_level.count() > 0:
            df_count_contact_level = dyf_count_contact_level.toDF()
            print df_count_contact_level.count()
            df_count_contact_level = df_count_contact_level.select('student_id')
            list_filer = [list(row) for row in df_count_contact_level.collect()]
        else:
            list_filer = ['']

        dyf_student_contact_level = DynamicFrame.fromDF(df_student_contact_level, glueContext, "dyf_student_contact_level")
        dyf_student_contact_level = Filter.apply(frame=dyf_student_contact_level,
                                                 f=lambda x: x['student_id'] not in list_filer)

        # or x['student_id'] == '29439'
        dyf_student_contact_level.count()
        dyf_student_contact_level.printSchema()

        df_student_contact_level = dyf_student_contact_level.toDF()

        df_contact_level_first = df_student_contact_level.groupby('student_id').agg(
            f.min("time_level_created").alias("min_time_level_created")) \
            .withColumnRenamed('student_id', 'min_student_id')

        df_contact_level_last = df_student_contact_level.groupby('student_id').agg(
            f.max("time_level_created").alias("max_time_level_created")) \
            .withColumnRenamed('student_id', 'max_student_id')

        df_contact_level_first = df_contact_level_first \
            .join(df_student_contact_level,
                  (df_contact_level_first.min_student_id == df_student_contact_level.student_id)
                  & (df_contact_level_first.min_time_level_created == df_student_contact_level.time_level_created), "left")

        df_contact_level_last = df_contact_level_last \
            .join(df_student_contact_level,
                  (df_contact_level_last.max_student_id == df_student_contact_level.student_id)
                  & (df_contact_level_last.max_time_level_created == df_student_contact_level.time_level_created), "left")

        df_contact_level_last = df_contact_level_last \
            .select('student_id', 'level_modified', 'time_level_created')

        df_contact_level_last = df_contact_level_last \
            .withColumnRenamed('time_level_created', 'start_date') \
            .withColumnRenamed('level_modified', 'level_study') \
            .withColumn('end_date', f.lit(4094592235).cast('long'))

        df_contact_level_last = df_contact_level_last \
            .select('student_id', 'level_study', 'start_date', 'end_date')

        df_contact_level_first = df_contact_level_first \
            .select('student_id', 'level_current', 'time_created', 'time_level_created')

        df_contact_level_first = df_contact_level_first \
            .withColumnRenamed('level_current', 'level_study') \
            .withColumnRenamed('time_created', 'start_date') \
            .withColumnRenamed('time_level_created', 'end_date')

        df_contact_level_first = df_contact_level_first \
            .select('student_id', 'level_study', 'start_date', 'end_date')

        df_contact_level_first.show()
        df_contact_level_last.show(),
        print "END FIRST_LAST"

        df_student_contact_level01 = df_student_contact_level \
            .select('student_id', 'level_current', 'level_modified', 'time_level_created')
        print "df_student_contact_level"
        df_student_contact_level01.show(20)
        df_student_contact_level02 = df_student_contact_level01

        df_student_contact_level02 = df_student_contact_level02 \
            .withColumnRenamed('student_id', 'student_id_temp') \
            .withColumnRenamed('level_current', 'level_current_temp') \
            .withColumnRenamed('level_modified', 'level_modified_temp') \
            .withColumnRenamed('time_level_created', 'time_level_created_temp')
        df_student_contact_level02.show(20)

        df_join_student_contact_level = df_student_contact_level01 \
            .join(df_student_contact_level02,
                  (df_student_contact_level01.student_id == df_student_contact_level02.student_id_temp)
                  & (df_student_contact_level01.level_current == df_student_contact_level02.level_modified_temp)
                  & (df_student_contact_level01.time_level_created > df_student_contact_level02.time_level_created_temp),
                  "left")
        df_join_student_contact_level.show(100)
        df_join_student_contact_level = df_join_student_contact_level \
            .groupby('student_id', 'level_current', 'time_level_created_temp') \
            .agg(f.min("time_level_created").alias("time_level_created"))

        df_join_student_contact_level = df_join_student_contact_level \
            .withColumnRenamed('time_level_created_temp', 'start_date') \
            .withColumnRenamed('time_level_created', 'end_date') \
            .withColumnRenamed('level_current', 'level_study')

        df_join_student_contact_level = df_join_student_contact_level.where('start_date is not null')
        df_join_student_contact_level.count()
        df_join_student_contact_level.show()

        df_union_first_and_middle_contact = df_contact_level_first.union(df_join_student_contact_level)
        df_union_all_contact = df_union_first_and_middle_contact.union(df_contact_level_last)
        print df_union_all_contact.count()
        df_union_all_contact.printSchema()
        df_union_all_contact.show()
        df_union_all_contact = df_union_all_contact.withColumn("user_id", f.lit(None).cast('long'))

        dyf_student_contact_level = DynamicFrame.fromDF(df_union_all_contact, glueContext,
                                                        "dyf_student_contact_level")
        dyf_join_all0 = Join.apply(dyf_mapping_class_lms, dyf_student_contact_level, "level_lms", "level_study")
        dyf_join_all = Join.apply(dyf_join_all0, dyf_learning_object_class, "level_list_master", "class_name")

        df_join_all = dyf_join_all.toDF()
        print "df_join_all ", df_join_all.count()

        df_join_all = df_join_all.dropDuplicates()
        dyf_join_all = DynamicFrame.fromDF(df_join_all, glueContext, "dyf_join_all")

        print "dyf_join_all ", dyf_join_all.count()
        dyf_join_all.printSchema()
        dyf_join_all.show(10)

        applymapping = ApplyMapping.apply(frame=dyf_join_all,
                                          mappings=[
                                              ("student_id", "string", "student_id", "string"),
                                              ("user_id", "long", "user_id", "long"),
                                              ("class_id", "long", "class_id", "long"),
                                              ("start_date", "int", "start_date", "long"),
                                              ("end_date", 'long', 'end_date', 'long'),
                                              ("level_study", 'string', 'level_study', 'string')])
        resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                            transformation_ctx="resolvechoice2")
        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "mapping_class_student",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3n://dtsodin/temp1/mapping_class_student/",
                                                                   transformation_ctx="datasink5")

        print('START WRITE TO S3-------------------------')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dtsodin')
        bucket.objects.filter(Prefix="student_behavior/student_level/").delete()

        s3 = boto3.client('s3')
        bucket_name = "dtsodin"
        directory_name = "student_behavior/student_level/"  # it's name of your folders
        s3.put_object(Bucket=bucket_name, Key=directory_name)

        ######
        log_student_level = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "mapping_class_student",
                "redshiftTmpDir": "s3n://dtsodin/temp1/mapping_class_student/"}
        )
        datasink6 = glueContext.write_dynamic_frame.from_options(frame=log_student_level, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dtsodin/student_behavior/student_level/"},
                                                                 format="parquet", transformation_ctx="datasink6")
        print('END WRITE TO S3-------------------------')

        # ghi flag
        # lay max key trong data source
        datasource = dyf_student_level.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dtsodin/flag/flag_log_student_level.parquet", mode="overwrite")

    ##############################################################################################
    dyf_student_advisor = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_change_assignment_advisor"
    )
    dyf_student_advisor = dyf_student_advisor.select_fields(
        ['_key', 'id', 'contact_id', 'advisor_id_old', 'advisor_id_new', 'created_at'])\
        .rename_field('contact_id', 'contact_id1')

    # try:
    #     df_flag = spark.read.parquet("s3://dtsodin/flag/flag_log_student_advisor.parquet")
    #     max_key = df_flag.collect()[0]['flag']
    #     print("max_key:  ", max_key)
    #     # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    #     dyf_student_advisor = Filter.apply(frame=dyf_student_advisor, f=lambda x: x["_key"] > max_key)
    # except:
    #     print('read flag file error ')

    if dyf_student_advisor.count() > 0:
        df_student_advisor = dyf_student_advisor.toDF()
        # df_student_advisor.printSchema()
        # df_student_advisor.show()

        df_student_advisor = df_student_advisor.withColumn(
            'created_at_1', unix_timestamp(df_student_advisor.created_at, "yyyy-MM-dd HH:mm:ss").cast(IntegerType()))

        df_student_contact = dyf_student_contact.toDF()
        df_student_contact_advisor = df_student_contact.join(df_student_advisor,
                                                             df_student_contact.contact_id == df_student_advisor.contact_id1)



        dyf_student_contact_advisor = DynamicFrame.fromDF(df_student_contact_advisor, glueContext,
                                                          "dyf_student_contact_advisor")
        dyf_student_contact_advisor = dyf_student_contact_advisor.select_fields(['student_id', 'advisor_id',
                                                                                 'advisor_id_old', 'advisor_id_new',
                                                                                 'created_at_1', 'time_created'])
        df_student_contact_advisor = dyf_student_contact_advisor.toDF()
        df_student_contact_advisor = df_student_contact_advisor.dropDuplicates()
        df_advisor_temp = df_student_contact_advisor
        df_advisor_temp = df_advisor_temp \
            .withColumnRenamed('student_id', 'student_id_temp') \
            .withColumnRenamed('advisor_id', 'advisor_id_temp') \
            .withColumnRenamed('advisor_id_old', 'advisor_id_old_temp') \
            .withColumnRenamed('advisor_id_new', 'advisor_id_new_temp') \
            .withColumnRenamed('created_at_1', 'created_at_temp') \
            .withColumnRenamed('time_created', 'time_created_temp')

        df_join_data = df_student_contact_advisor \
            .join(df_advisor_temp,
                  (df_student_contact_advisor.student_id == df_advisor_temp.student_id_temp)
                  & (df_student_contact_advisor.advisor_id_new == df_advisor_temp.advisor_id_old_temp)
                  & (df_student_contact_advisor.created_at_1 <= df_advisor_temp.created_at_temp), "left")

        df_join_data = df_join_data.groupby('student_id', 'advisor_id_new', 'created_at_1') \
            .agg(f.min("created_at_temp").alias("created_at_temp"))

        df_join_data = df_join_data.withColumn("end_time", check_endtime_null(df_join_data.created_at_temp))
        df_join_data = df_join_data.dropDuplicates()
        dyf_student_contact_advisor = DynamicFrame.fromDF(df_join_data, glueContext,
                                                          "dyf_student_contact_advisor")

        dyf_student_contact_advisor.printSchema()
        applymapping = ApplyMapping.apply(frame=dyf_student_contact_advisor,
                                          mappings=[("student_id", "string", "student_id", "string"),
                                                    ("advisor_id_new", "string", "advisor_id", "string"),
                                                    ("created_at_1", 'int', 'start_time', 'long'),
                                                    ("end_time", 'long', 'end_time', 'long')])

        resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                            transformation_ctx="resolvechoice2")
        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "fact_log_change_advisor",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3n://dtsodin/temp1/fact_log_change_advisor/",
                                                                   transformation_ctx="datasink5")

        print('START WRITE TO S3-------------------------')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dtsodin')
        bucket.objects.filter(Prefix="student_behavior/student_advisor/").delete()

        s3 = boto3.client('s3')
        bucket_name = "dtsodin"
        directory_name = "student_behavior/student_advisor/"  # it's name of your folders
        s3.put_object(Bucket=bucket_name, Key=directory_name)

        ######
        log_student_advisor = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                    "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                    "user": "dtsodin",
                    "password": "DWHDtsodin@123",
                    "dbtable": "fact_log_change_advisor",
                    "redshiftTmpDir": "s3n://dtsodin/temp1/fact_log_change_advisor/"}
        )
        datasink6 = glueContext.write_dynamic_frame.from_options(frame=log_student_advisor, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dtsodin/student_behavior/student_advisor/"},
                                                                 format="parquet", transformation_ctx="datasink6")
        print('END WRITE TO S3-------------------------')

        datasource = dyf_student_advisor.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        # convert kieu dl
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dtsodin/flag/flag_log_student_advisor.parquet", mode="overwrite")



    ##############################################################################################
    dyf_student_product = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                        table_name="tpe_enduser_used_product")

    dyf_student_product = dyf_student_product.select_fields(
        ['_key', 'id', 'contact_id', 'product_id', 'starttime', 'endtime', 'timecreated', 'balance_used', 'status',
         'timemodified']).rename_field('contact_id', 'contact_id1')

    dyf_student_product = dyf_student_product.resolveChoice(specs=[('_key', 'cast:long')])

    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3://dtsodin/flag/flag_tpe_enduser_used_product.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)
        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        # dyf_student_product = Filter.apply(frame=dyf_student_product, f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    dyf_student_product = Filter.apply(frame=dyf_student_product,
                                       f=lambda x: x["product_id"] is not None and x["product_id"] != ''
                                                   and x["contact_id1"] is not None and x["contact_id1"] != '')
    if (dyf_student_product.count() > 0):
        # try:
        dyf_product_details = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                            table_name="tpe_invoice_product_details")

        dyf_product_details = dyf_product_details.select_fields(
            ['id', 'cat_code', 'package_cost', 'actual_cost', 'package_time']).rename_field('id', 'id_goi')

        # loc data
        # df_goi = datasource1.toDF()
        # df_goi = df_goi.where("id_goi <> '' and id_goi is not null")
        # data_goi = DynamicFrame.fromDF(df_goi, glueContext, "data_goi")
        dyf_product_details = Filter.apply(frame=dyf_product_details,
                                           f=lambda x: x["id_goi"] is not None and x["id_goi"] != '')

        # df_lsmua = dyf_student_product.toDF()
        # df_lsmua = df_lsmua.where("product_id is not null and product_id <> ''")
        # df_lsmua = df_lsmua.where("contact_id is not null and contact_id <> ''")
        # data_lsmua = DynamicFrame.fromDF(df_lsmua, glueContext, "data_lsmua")

        # map ls mua goi vs thong tin chi tiet cua goi
        dyf_student_package = Join.apply(dyf_student_product, dyf_product_details, 'product_id', 'id_goi')
        dyf_join_data = Join.apply(dyf_student_package, dyf_student_contact, 'contact_id1', 'contact_id')

        print "dyf_student_package count1: ", dyf_join_data.count()
        df_join_data = dyf_join_data.toDF()
        # drop duplicate rows
        # df_join_data = df_join_data.dropDuplicates()
        df_join_data = df_join_data.groupby('id', 'student_id') \
            .agg(f.first('cat_code').alias('cat_code'),
                 f.first('starttime').alias('starttime'),
                 f.first('endtime').alias('endtime'),
                 f.first('timecreated').alias('timecreated'))
        dyf_student_package = DynamicFrame.fromDF(df_join_data, glueContext, "dyf_student_package")

        # convert data
        # df_student_package = dyf_student_package.toDF()
        # df_student_package = df_student_package.withColumn('ngay_kich_hoat', from_unixtime(df_student_package.starttime)) \
        #     .withColumn('ngay_het_han', from_unixtime(df_student_package.endtime)) \
        #     .withColumn('ngay_mua', from_unixtime(df_student_package.timecreated)) \
        #     .withColumn('id_time', from_unixtime(df_student_package.starttime, "yyyyMMdd")) \
        #     .withColumn('ngay_thay_doi', from_unixtime(df_student_package.timemodified))
        # df_student_package = df_student_package.dropDuplicates(['contact_id', 'product_id'])
        #
        # data = DynamicFrame.fromDF(df_student_package, glueContext, "data")
        # df_student_package.printSchema()
        # df_student_package.show()

        # chon cac truong va kieu du lieu day vao db
        applyMapping = ApplyMapping.apply(frame=dyf_student_package,
                                          mappings=[("id", "string", "id", "string"),
                                                    ("student_id", "string", "student_id", "string"),
                                                    ("cat_code", 'string', 'package_code', 'string'),
                                                    ("starttime", "int", "start_time", "int"),
                                                    ("endtime", "int", "end_time", "int"),
                                                    ("timecreated", "int", "timecreated", "int")])

        resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols", transformation_ctx="resolvechoice2")

        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")

        dropnullfields.show(10)
        # ghi data vao db
        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "fact_student_package",
                                                                       "database": "dts_odin"
                                                                   },
                                                                   redshift_tmp_dir="s3n://dtsodin/temp1/fact_student_package/",
                                                                   transformation_ctx="datasink4")

        print('START WRITE TO S3-------------------------')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dtsodin')
        bucket.objects.filter(Prefix="student_behavior/student_package/").delete()

        s3 = boto3.client('s3')
        bucket_name = "dtsodin"
        directory_name = "student_behavior/student_package/"
        s3.put_object(Bucket=bucket_name, Key=directory_name)

        ######
        log_student_package = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "fact_student_package",
                "redshiftTmpDir": "s3n://dtsodin/temp1/fact_student_package/"}
        )
        datasink6 = glueContext.write_dynamic_frame.from_options(frame=log_student_package, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dtsodin/student_behavior/student_package/"},
                                                                 format="parquet", transformation_ctx="datasink6")
        print('END WRITE TO S3-------------------------')

        # ghi flag
        # lay max key trong data source
        datasource = dyf_student_product.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dtsodin/flag/flag_tpe_enduser_used_product_log.parquet", mode="overwrite")



    ##############################################################################################

    def do_check_null(val):
        if val is not None:
            return val
        return 4094592235

    check_data_null = udf(do_check_null, LongType())

    dyf_tpe_enduser_used_product_history = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                        table_name="tpe_enduser_used_product_history")

    dyf_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.select_fields(
        ['_key','contact_id', 'status_old', 'status_new', 'timecreated'])

    dyf_tpe_enduser_used_product_history = dyf_tpe_enduser_used_product_history.resolveChoice(specs=[('_key', 'cast:long')])

    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dtsodin/flag/flag_tpe_enduser_used_product_history.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)
        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        # dyf_tpe_enduser_used_product_history = Filter.apply(frame=dyf_tpe_enduser_used_product_history, f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    dyf_tpe_enduser_used_product_history = Filter.apply(frame=dyf_tpe_enduser_used_product_history,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')
    if (dyf_tpe_enduser_used_product_history.count() > 0):
        df_tpe_enduser_used_product_history01 = dyf_tpe_enduser_used_product_history.toDF()
        df_tpe_enduser_used_product_history02 = dyf_tpe_enduser_used_product_history.toDF()

        df_tpe_enduser_used_product_history01 = df_tpe_enduser_used_product_history01\
            .withColumnRenamed('timecreated', 'start_date')

        df_tpe_enduser_used_product_history02 = df_tpe_enduser_used_product_history02 \
            .withColumnRenamed('timecreated', 'timecreated02') \
            .withColumnRenamed('status_old', 'status_old02') \
            .withColumnRenamed('status_new', 'status_new02') \
            .withColumnRenamed('contact_id', 'contact_id02')

        df_tpe_enduser_used_product_history_join = df_tpe_enduser_used_product_history01.join(df_tpe_enduser_used_product_history02,
            (df_tpe_enduser_used_product_history01['contact_id'] == df_tpe_enduser_used_product_history02['contact_id02'])
            & (df_tpe_enduser_used_product_history01['status_new'] == df_tpe_enduser_used_product_history02['status_old02'])
            & (df_tpe_enduser_used_product_history01['start_date'] <= df_tpe_enduser_used_product_history02['timecreated02']), "left")

        df_tpe_enduser_used_product_history_join = df_tpe_enduser_used_product_history_join \
            .withColumn("end_date", check_data_null(df_tpe_enduser_used_product_history_join.timecreated02))

        df_tpe_enduser_used_product_history_join.printSchema()
        print df_tpe_enduser_used_product_history_join.count()
        df_tpe_enduser_used_product_history_join.show(10)

        dyf_tpe_enduser_product_history = DynamicFrame.fromDF(df_tpe_enduser_used_product_history_join, glueContext,
                                                                   "dyf_tpe_enduser_product_history")
        dyf_tpe_enduser_product_history = dyf_tpe_enduser_product_history.select_fields(
            ['contact_id', 'status_old', 'status_new', 'start_date', 'end_date'])

        df_tpe_enduser_used_product_history = dyf_tpe_enduser_product_history.toDF()
        df_tpe_enduser_used_product_history_join.printSchema()
        print df_tpe_enduser_used_product_history_join.count()
        df_tpe_enduser_used_product_history_join.show(10)

        dyf_tpe_enduser_product_history = DynamicFrame.fromDF(df_tpe_enduser_used_product_history, glueContext, "dyf_tpe_enduser_product_history")
        # chon cac truong va kieu du lieu day vao db
        applyMapping = ApplyMapping.apply(frame=dyf_tpe_enduser_product_history,
                                          mappings=[("contact_id", "string", "contact_id", "string"),
                                                    ("status_new", "string", "status_code", "string"),
                                                    ("status_old", "string", "last_status_code", "string"),
                                                    ("start_date", "int", "start_date", "long"),
                                                    ("end_date", "long", "end_date", "long")])

        resolvechoice = ResolveChoice.apply(frame=applyMapping, choice="make_cols",
                                            transformation_ctx="resolvechoice2")

        dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")

        # ghi data vao db
        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "fact_log_student_status",
                                                                       "database": "dts_odin",
                                                                       },
                                                                   redshift_tmp_dir="s3n://dtsodin/temp1/tpe_enduser_used_product_history/",
                                                                   transformation_ctx="datasink4")

        print('START WRITE TO S3-------------------------')
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dtsodin')
        bucket.objects.filter(Prefix="student_behavior/student_status/").delete()

        s3 = boto3.client('s3')
        bucket_name = "dtsodin"
        directory_name = "student_behavior/student_status/"
        s3.put_object(Bucket=bucket_name, Key=directory_name)

        ######
        log_student_status = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "fact_log_student_status",
                "redshiftTmpDir": "s3n://dtsodin/temp1/fact_log_student_status/"}
        )
        datasink6 = glueContext.write_dynamic_frame.from_options(frame=log_student_status, connection_type="s3",
                                                                 connection_options={
                                                                     "path": "s3://dtsodin/student_behavior/student_status/"},
                                                                 format="parquet", transformation_ctx="datasink6")
        print('END WRITE TO S3-------------------------')

        # ghi flag
        # lay max key trong data source
        datasource = dyf_tpe_enduser_used_product_history.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dtsodin/flag/flag_tpe_enduser_used_product_history.parquet", mode="overwrite")

if __name__ == "__main__":
    main()
