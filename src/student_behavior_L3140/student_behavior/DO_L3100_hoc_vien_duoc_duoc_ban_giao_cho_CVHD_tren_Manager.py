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
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    student_id_unavailable = '0'
    package_endtime_unavailable = 99999999999L
    package_starttime_unavailable = 0L
    student_level_code_unavailable = 'UNAVAILABLE'
    student_status_code_unavailable = 'UNAVAILABLE'

    package_endtime = 'package_endtime'
    package_starttime = 'package_starttime'
    student_level_code = 'student_level_code'
    student_status_code = 'student_status_code'

    def doCheckModified(val1, val2):
        if val1 is not None:
            return val1
        return val2

    check_modified_null = udf(doCheckModified, StringType())

    def doCheckStudentID(code):
        code = str(code)
        if code is None:
            return student_id_unavailable
        return code

    check_student_id = udf(doCheckStudentID, StringType())

    def doCheckData(code, key):
        key = str(key)
        if code is None:
            if key == package_endtime:
                return package_endtime_unavailable
            else:
                return package_starttime_unavailable
        return code

    check_data = udf(doCheckData, IntegerType())

    def doCheckDataNull(code, key):
        code = str(code)
        key = str(key)
        if (code is None) & (key == student_level_code):
            return student_level_code_unavailable

        if (code is None) & (key == student_status_code):
            return student_status_code_unavailable

        return code

    check_data_null = udf(doCheckDataNull, StringType())

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

    # get dynamic frame source
    dyf_student_assignments = glueContext.create_dynamic_frame.from_catalog(
        database = "tig_advisor",
        table_name = "student_assignments"
    )

    # chon cac field
    dyf_student_assignments = dyf_student_assignments.select_fields(
        ['_key', 'contact_id', 'advisor_id', 'time_created', 'time_modified', 'status'])

    df_student_assignments = dyf_student_assignments.toDF()
    df_student_assignments = df_student_assignments\
        .withColumn("time_created", unix_timestamp(df_student_assignments.time_created).cast("long"))\
        .withColumn("time_modified", unix_timestamp(df_student_assignments.time_modified).cast("long"))\
        .withColumn("_key", unix_timestamp(df_student_assignments._key).cast("long"))

    dyf_student_assignments = DynamicFrame.fromDF(df_student_assignments, glueContext, "dyf_student_assignments")
    dyf_student_assignments.printSchema()
    dyf_student_assignments.show(2)

    #  check bucket is not null
    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dtsodin/flag/toa_L3150/toa_student_assignments.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_student_assignments = Filter.apply(frame=dyf_student_assignments, f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    print('the number of new contacts: ', dyf_student_assignments.count())

    if dyf_student_assignments.count() > 0:
        dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="student_contact"
        )
        dyf_student_contact = dyf_student_contact.select_fields(
            ['contact_id', 'student_id', 'level_study', 'time_lms_created'])\
            .rename_field('contact_id', 'contactid')


        dyf_log_student_status = glueContext.create_dynamic_frame.from_catalog(
            database="do_tig_advisor",
            table_name="log_student_status"
        )
        dyf_log_student_status = dyf_log_student_status.select_fields(
            ['contact_id', 'status_code', 'last_status_code', 'start_date', 'end_date']) \
            .rename_field('contact_id', 'contact_id_status')

        dyf_log_student_package = glueContext.create_dynamic_frame.from_catalog(
            database="do_tig_advisor",
            table_name="log_student_package"
        )
        dyf_log_student_package = dyf_log_student_package.select_fields(
            ['student_id', 'package_code', 'start_time', 'end_time']) \
            .rename_field('student_id', 'student_id_package') \
            .rename_field('start_time', 'start_time_package') \
            .rename_field('end_time', 'end_time_package')

        dyf_log_student_level_study = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="log_student_level_study"
        )
        dyf_log_student_level_study = dyf_log_student_level_study.select_fields(
            ['contact_id', 'level_current', 'level_modified', 'package_code', 'time_created']) \
            .rename_field('contact_id', 'contact_id_level')

        dyf_student_assignment = Filter.apply(frame=dyf_student_assignments,
                                                         f=lambda x: x['contact_id'] is not None
                                                                 and x['contact_id'] != '')

        try:
            dyf_assignments_contact = Join.apply(dyf_student_assignment, dyf_student_contact, "contact_id", "contactid")
            dyf_assignments_contact.printSchema()
            dyf_assignments_contact.show(2)
            df_assignments_contact = dyf_assignments_contact.toDF().dropDuplicates()
            df_log_student_level_study = dyf_log_student_level_study.toDF()
            df_temp = dyf_log_student_level_study.toDF()
            df_log_student_status = dyf_log_student_status.toDF()
            df_log_student_package = dyf_log_student_package.toDF()

            df_temp = df_temp.groupby('contact_id_level', 'level_current', 'package_code').agg(
                f.max("time_created").alias("time_created_max"))
            df_temp = df_temp.withColumnRenamed('contact_id_level', 'contact_id_join') \
                .withColumnRenamed('package_code', 'package_code_join')

            df_student_level_study = df_temp.join(df_log_student_level_study,
                                    (df_temp['contact_id_join'] == df_log_student_level_study['contact_id_level'])
                                    & (df_temp['package_code_join'] == df_log_student_level_study['package_code'])
                                    & (df_temp['time_created_max'] == df_log_student_level_study['time_created']), "left")

            print "=========== . ==========="
            df_student_level_study.printSchema()

            dyf_student_level_study = DynamicFrame.fromDF(df_student_level_study, glueContext, "dyf_student_level_study")

            dyf_student_level_study = dyf_student_level_study.select_fields(
                ['contact_id_level', 'level_current', 'level_modified', 'package_code', 'time_created'])

            df_student_level_study = dyf_student_level_study.toDF()
            df_student_level_study.printSchema()

            df_assignments_contact_level = df_student_level_study.join(df_assignments_contact,
                                       (df_student_level_study['contact_id_level'] == df_assignments_contact['contact_id'])
                                       & (df_student_level_study['time_created'] <= df_assignments_contact['time_lms_created']), "right")

            df_assignments_contact_level = df_assignments_contact_level.withColumn("level_modified_new",
                                             check_modified_null(df_assignments_contact_level.level_modified, df_assignments_contact_level.level_study))
            df_assignments_contact_level.printSchema()
            df_assignments_contact_level.count()
            df_assignments_contact_level.show(10)
            dyf_assignments_contact_level = DynamicFrame.fromDF(df_assignments_contact_level, glueContext, "dyf_assignments_contact_level")
            dyf_assignments_contact_level = dyf_assignments_contact_level.select_fields(
                ['time_created', 'contact_id', 'student_id', 'advisor_id', 'level_study', 'time_lms_created',
                 'level_current', 'level_modified', 'package_code', 'time_modified', 'level_modified_new'])
            # dyf_join_temp = Filter.apply(frame=dyf_join,
            #                              f=lambda x: x["level_modified_new"] is None)
            # print "count: ", dyf_join_temp.count()

            ############
            df_assignments_contact_level = dyf_assignments_contact_level.toDF()
            df_join_data = df_assignments_contact_level.join(df_log_student_status,
                                       (df_log_student_status['contact_id_status'] == df_assignments_contact_level['contact_id'])
                                       & (df_log_student_status['start_date'] <= df_assignments_contact_level['time_created'])
                                       & (df_log_student_status['end_date'] >= df_assignments_contact_level['time_created']), "left")

            df_join_full_data = df_join_data.join(df_log_student_package,
                                       (df_log_student_package['student_id_package'] == df_join_data['student_id'])
                                       & (df_log_student_package['start_time_package'] <= df_join_data['time_created'])
                                       & (df_log_student_package['end_time_package'] >= df_join_data['time_created']),
                                       "left")

            df_join_full_data = df_join_full_data.dropDuplicates()
            dyf_join_full_data = DynamicFrame.fromDF(df_join_full_data, glueContext, "dyf_join_full_data")
            dyf_join_full_data = Filter.apply(frame=dyf_join_full_data,
                                    f=lambda x: x["start_time_package"] is not None
                                                and x["end_time_package"] is not None)
            print "dyf_join_full_data: ", dyf_join_full_data.count()
            dyf_join_full_data.show(10)
            dyf_join_full_data = dyf_join_full_data.select_fields(
                ['time_created', 'student_id', 'contact_id', 'package_code', 'time_modified', 'advisor_id',
                 'start_time_package', 'end_time_package', 'level_modified_new', 'status_code'])

            df_join_full_data = dyf_join_full_data.toDF()
            df_join_full_data = df_join_full_data.withColumn("transformed_at", unix_timestamp(f.current_timestamp())) \
                .withColumn("student_id", check_student_id(df_join_full_data.student_id)) \
                .withColumn("package_endtime", check_data(df_join_full_data.end_time_package, f.lit(package_endtime))) \
                .withColumn("package_starttime", check_data(df_join_full_data.start_time_package, f.lit(package_starttime))) \
                .withColumn("student_level_code",
                            check_data_null(df_join_full_data.level_modified_new, f.lit(student_level_code))) \
                .withColumn("student_status_code", check_data_null(df_join_full_data.status_code, f.lit(student_status_code))) \
                .withColumn("behavior_id", f.lit(234).cast("long"))

            df_join_full_data.printSchema()
            print df_join_full_data.count()
            df_join_full_data.show(10)

            dyf_join_full_data = DynamicFrame.fromDF(df_join_full_data, glueContext, "dyf_join_full_data")
            # dyf_join.printSchema()
            # print dyf_join.count()
            # dyf_join.show(10)

            dyf_dong_tien_student = ApplyMapping.apply(frame=dyf_join_full_data,
                                                       mappings=[
                                                           ("time_created", "long", "student_behavior_date", "long"),
                                                           ("behavior_id", "long", "behavior_id", "long"),
                                                           ("student_id", "string", "student_id", "long"),
                                                           ("contact_id", "string", "contact_id", "string"),
                                                           ("package_code", "string", "package_code", "string"),
                                                           ("package_endtime", "int", "package_endtime", "long"),
                                                           ("package_starttime", "int", "package_starttime", "long"),
                                                           ("student_level_code", "string", "student_level_code", "string"),
                                                           ("student_status_code", "string", "student_status_code", "string"),
                                                           ("transformed_at", "long", "transformed_at", "long")])

            df_dong_tien_student = dyf_dong_tien_student.toDF()
            df_dong_tien_student2 = df_dong_tien_student.withColumn('student_behavior_id',
                                                                    f.md5(concaText(
                                                                        df_dong_tien_student.student_behavior_date,
                                                                        df_dong_tien_student.behavior_id,
                                                                        df_dong_tien_student.student_id,
                                                                        df_dong_tien_student.contact_id,
                                                                        df_dong_tien_student.package_code,
                                                                        df_dong_tien_student.package_endtime,
                                                                        df_dong_tien_student.package_starttime,
                                                                        df_dong_tien_student.student_level_code,
                                                                        df_dong_tien_student.student_status_code,
                                                                        df_dong_tien_student.transformed_at)))

            # df_dong_tien_student2 = df_dong_tien_student2.dropDuplicates()
            print "==============", df_dong_tien_student2.count()
            df_dong_tien_student2 = df_dong_tien_student2\
                .groupby('student_behavior_id', 'student_behavior_date', 'behavior_id',
                         'transformed_at', 'contact_id', 'student_id', 'package_code',
                         'package_endtime', 'package_starttime', 'student_level_code')\
                .agg(f.first('student_status_code'))
            print "==============", df_dong_tien_student2.count()

            dyf_dong_tien_student = DynamicFrame.fromDF(df_dong_tien_student2, glueContext, 'dyf_dong_tien_student')

            dyf_dong_tien_student = Filter.apply(frame=dyf_dong_tien_student,
                                                 f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')

            applymapping1 = ApplyMapping.apply(frame=dyf_dong_tien_student,
                                               mappings=[
                                                   ("student_behavior_id", "string", "student_behavior_id", "string"),
                                                   ("student_behavior_date", "long", "student_behavior_date", "long"),
                                                   ("behavior_id", "long", "behavior_id", "long"),
                                                   ("student_id", "long", "student_id", "long"),
                                                   ("contact_id", "string", "contact_id", "string"),
                                                   ("package_code", "string", "package_code", "string"),
                                                   ("package_endtime", "long", "package_endtime", "long"),
                                                   ("package_starttime", "long", "package_starttime", "long"),
                                                   ("student_level_code", "string", "student_level_code", "string"),
                                                   ("student_status_code", "string", "student_status_code", "string"),
                                                   ("transformed_at", "long", "transformed_at", "long")])

            resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice1")
            dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields1")
            print resolvechoice1.count()
            resolvechoice1.printSchema()
            resolvechoice1.show(10)

            print('START WRITE TO S3-------------------------')
            datasink6 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields1, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3://dtsodin/student_behavior/student_behavior/",
                                                                         "partitionKeys": ["behavior_id"]},
                                                                     format="parquet",
                                                                     transformation_ctx="datasink6")
            print('END WRITE TO S3-------------------------')

            # datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields1,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "student_behavior",
            #                                                                "database": "dts_odin"
            #                                                            },
            #                                                            redshift_tmp_dir="s3a://dtsodin/temp/student_behavior/",
            #                                                            transformation_ctx="datasink1")

            # ghi flag
            # lay max key trong data source
            datasourceTmp = dyf_student_assignments.toDF()
            flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/toa_student_assignments.parquet", mode="overwrite")
        except Exception as e:
            print e

if __name__ == "__main__":
        main()
