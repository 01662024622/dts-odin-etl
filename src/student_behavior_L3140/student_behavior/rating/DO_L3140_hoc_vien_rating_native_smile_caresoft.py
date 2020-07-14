import sys
import pytz
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
from pyspark.sql.types import ArrayType, IntegerType, LongType, StructType, StructField, StringType
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
from datetime import date, datetime, timedelta

def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)
    # f.lit(today_second).cast('long').alias('transformed_at')

    satisfaction = ['1', '2', '3', '4', '5']

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

    concaText = f.udf(concaText, StringType())


    dyf_ticket_log = glueContext.create_dynamic_frame.from_catalog(
        database="native_smile",
        table_name="ticket_log"
    )

    dyf_ticket_log = dyf_ticket_log.select_fields(
            ['_key', 'requester_email', 'satisfaction', 'satisfaction_at', 'created_at']
    )
    dyf_ticket_log = dyf_ticket_log.resolveChoice(specs=[('_key', 'cast:long')])
    # try:
    #     df_flag_1 = spark.read.parquet("s3://dtsodin/flag/flag_hoc_vien_rating_native_smile_caresoft.parquet")
    #     max_key = df_flag_1.collect()[0]['flag']
    #     print("max_key:  ", max_key)
    #     # Chi lay nhung ban ghi lon hon max_key da luu, ko load full
    #     dyf_ticket_log = Filter.apply(frame=dyf_ticket_log, f=lambda x: x["_key"] > max_key)
    # except:
    #     print('read flag file error ')

    if dyf_ticket_log.count()> 0:

        dyf_student_contact_email = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="student_contact_email"
        )
        dyf_student_contact_email = dyf_student_contact_email.select_fields(
            ['contact_id', 'email']) \
            .rename_field('contact_id', 'contact_id_email')

        dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="student_contact"
        )
        dyf_student_contact = dyf_student_contact.select_fields(
            ['contact_id', 'student_id', 'level_study', 'time_lms_created'])\

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

        dyf_ticket_log.printSchema()
        print dyf_ticket_log.count()
        dyf_rating_class = Filter.apply(frame = dyf_ticket_log,
                                            f = lambda x: x['satisfaction'] in satisfaction)
        print dyf_rating_class.count()
        try:
            df_rating_class = dyf_rating_class.toDF()
            df_rating_class = df_rating_class.limit(99999)
            df_student_contact = dyf_student_contact.toDF()
            df_student_contact_email = dyf_student_contact_email.toDF()
            df_log_student_level_study = dyf_log_student_level_study.toDF()
            df_temp = dyf_log_student_level_study.toDF()
            df_log_student_status = dyf_log_student_status.toDF()
            df_log_student_package = dyf_log_student_package.toDF()

            df_temp = df_temp.groupby('contact_id_level', 'level_current', 'package_code').agg(
                f.max("time_created").alias("time_created_max"))
            df_temp = df_temp.withColumnRenamed('contact_id_level', 'contact_id_join') \
                .withColumnRenamed('package_code', 'package_code_join')

            df_join0 = df_temp.join(df_log_student_level_study,
                                    (df_temp['contact_id_join'] == df_log_student_level_study['contact_id_level'])
                                    & (df_temp['package_code_join'] == df_log_student_level_study['package_code'])
                                    & (df_temp['time_created_max'] == df_log_student_level_study['time_created']), "left")
            print "=========== . ==========="
            df_join0.printSchema()
            dyf_join = DynamicFrame.fromDF(df_join0, glueContext, "dyf_join")
            dyf_join = dyf_join.select_fields(
                ['contact_id_level', 'level_current', 'level_modified', 'package_code', 'time_created'])
            df_join = dyf_join.toDF()
            df_join.printSchema()
            df_join.show(10)
            print "########## . ###########"
            df_join0 = df_rating_class.join(df_student_contact_email,
                                                      (df_rating_class['requester_email'] == df_student_contact_email['email']))

            df_join01 = df_join0.join(df_student_contact,
                                         (df_join0['contact_id_email'] == df_student_contact['contact_id']))
            df_join01.printSchema()
            df_join02 = df_join01.join(df_join,
                                       (df_join['contact_id_level'] == df_join01['contact_id'])
                                       & (df_join['time_created'] <= df_join01['time_lms_created']), "left")

            df_join02 = df_join02\
                .withColumn("level_modified_new", check_modified_null(df_join02.level_modified, df_join02.level_study))\
                .withColumn("timecreated", f.unix_timestamp(df_join02.created_at, "yyyy-MM-dd HH:mm:ss"))

            df_join02.printSchema()
            df_join02.show(10)
            dyf_join = DynamicFrame.fromDF(df_join02, glueContext, "dyf_join")
            dyf_join = dyf_join.select_fields(['timecreated', 'contact_id', 'student_id', 'level_study', 'time_lms_created',
                                               'level_current', 'level_modified', 'package_code', 'time_created', 'satisfaction',
                                               'level_modified_new'])
            # dyf_join_temp = Filter.apply(frame=dyf_join,
            #                              f=lambda x: x["level_modified_new"] is None)
            # print "count: ", dyf_join_temp.count()

            ############
            df_join02 = dyf_join.toDF()
            df_join03 = df_join02.join(df_log_student_status,
                                       (df_log_student_status['contact_id_status'] == df_join02['contact_id'])
                                       & (df_log_student_status['start_date'] <= df_join02['timecreated'])
                                       & (df_log_student_status['end_date'] >= df_join02['timecreated']), "left")

            df_join04 = df_join03.join(df_log_student_package,
                                       (df_log_student_package['student_id_package'] == df_join03['student_id'])
                                       & (df_log_student_package['start_time_package'] <= df_join03['timecreated'])
                                       & (df_log_student_package['end_time_package'] >= df_join03['timecreated']), "left")

            dyf_join = DynamicFrame.fromDF(df_join04, glueContext, "dyf_join")
            dyf_join = Filter.apply(frame=dyf_join,
                                    f=lambda x: x["start_time_package"] is not None
                                                and x["end_time_package"] is not None)
            print "dyf_join: ", dyf_join.count()
            dyf_join.show(10)
            dyf_join = dyf_join.select_fields(
                ['timecreated', 'student_id', 'contact_id', 'package_code', 'satisfaction',
                 'start_time_package', 'end_time_package', 'level_modified_new', 'status_code']
            )
            # dyf_join01 = Filter.apply(frame=dyf_join,
            #                           f=lambda x: x["level_current"] is not None)
            #
            # print "Check null ", dyf_join01.count()

            df_join04 = dyf_join.toDF()
            df_join04 = df_join04.withColumn("transformed_at", unix_timestamp(f.current_timestamp())) \
                .withColumn("student_id", check_student_id(df_join04.student_id)) \
                .withColumn("package_endtime", check_data(df_join04.end_time_package, f.lit(package_endtime))) \
                .withColumn("package_starttime", check_data(df_join04.start_time_package, f.lit(package_starttime))) \
                .withColumn("student_level_code", check_data_null(df_join04.level_modified_new, f.lit(student_level_code))) \
                .withColumn("student_status_code", check_data_null(df_join04.status_code, f.lit(student_status_code))) \
                .withColumn("behavior_id", f.lit(26)) \
                .withColumn("rating_type", f.lit("rating_native_smile_caresoft")) \
                .withColumn("comment", f.lit("")) \
                .withColumn("rating_about", f.lit(None)) \
                .withColumn("number_rating", f.lit(1)) \
                .withColumn("value_rating", df_join04.satisfaction)

            df_join04.printSchema()
            print df_join04.count()
            df_join04.show(10)

            dyf_join = DynamicFrame.fromDF(df_join04, glueContext, "dyf_join")
            # dyf_join.printSchema()
            # print dyf_join.count()
            # dyf_join.show(10)

            dyf_rating_cara = ApplyMapping.apply(frame=dyf_join,
                                                       mappings=[("timecreated", "int", "student_behavior_date", "long"),
                                                                 ("behavior_id", "int", "behavior_id", "long"),
                                                                 ("student_id", "string", "student_id", "long"),
                                                                 ("contact_id", "string", "contact_id", "string"),
                                                                 ("package_code", "string", "package_code", "string"),
                                                                 ("package_endtime", "int", "package_endtime", "long"),
                                                                 ("package_starttime", "int", "package_starttime", "long"),
                                                                 ("student_level_code", "string", "student_level_code", "string"),
                                                                 ("student_status_code", "string", "student_status_code", "string"),
                                                                 ("transformed_at", "long", "transformed_at", "long"),
                                                                 ("rating_type", "string", "rating_type", "string"),
                                                                 ("comment", "string", "comment", "string"),
                                                                 ("rating_about", "int", "rating_about", "long"),
                                                                 ("number_rating", "int", "number_rating", "long"),
                                                                 ("value_rating", "int", "value_rating", "long")])

            df_rating_cara = dyf_rating_cara.toDF()
            df_rating_cara2 = df_rating_cara.withColumn('student_behavior_id',
                                                                    f.md5(concaText(
                                                                        df_rating_cara.student_behavior_date,
                                                                        df_rating_cara.behavior_id,
                                                                        df_rating_cara.student_id,
                                                                        df_rating_cara.contact_id,
                                                                        df_rating_cara.package_code,
                                                                        df_rating_cara.package_endtime,
                                                                        df_rating_cara.package_starttime,
                                                                        df_rating_cara.student_level_code,
                                                                        df_rating_cara.student_status_code,
                                                                        df_rating_cara.transformed_at)))

            dyf_rating_cara = DynamicFrame.fromDF(df_rating_cara2, glueContext, 'dyf_rating_cara')

            dyf_rating_cara = Filter.apply(frame=dyf_rating_cara,
                                           f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')

            applymapping0 = ApplyMapping.apply(frame=dyf_rating_cara,
                                               mappings=[
                                                   ("student_behavior_id", "string", "student_behavior_id", "string"),
                                                   ("rating_type", "string", "rating_type", "string"),
                                                   ("comment", "string", "comment", "string"),
                                                   ("rating_about", "long", "rating_about", "long"),
                                                   ("number_rating", "long", "number_rating", "long"),
                                                   ("value_rating", "long", "value_rating", "long"),
                                                   ("behavior_id", "long", "behavior_id", "long")])

            applymapping0.printSchema()
            print applymapping0.count()
            # applymapping0.show(5)
            resolvechoice0 = ResolveChoice.apply(frame=applymapping0, choice="make_cols",
                                                 transformation_ctx="resolvechoice1")
            dropnullfields0 = DropNullFields.apply(frame=resolvechoice0, transformation_ctx="dropnullfields0")
            print resolvechoice0.count()
            # resolvechoice0.printSchema()
            resolvechoice0.show(10)

            print('START WRITE TO S3-------------------------')
            datasink0 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields0, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3://dtsodin/student_behavior/student_rating/",
                                                                         "partitionKeys": ["behavior_id"]},
                                                                     format="parquet",
                                                                     transformation_ctx="datasink0")
            print('END WRITE TO S3-------------------------')

            # datasink0 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields0,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "student_rating_temp",
            #                                                                "database": "dts_odin"
            #                                                            },
            #                                                            redshift_tmp_dir="s3a://dtsodin/temp/student_rating_temp/",
            #                                                            transformation_ctx="datasink0")

            applymapping1 = ApplyMapping.apply(frame=dyf_rating_cara,
                                               mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
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

            applymapping1.printSchema()
            print applymapping1.count()
            # applymapping1.show(10)

            resolvechoice1 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice1")
            dropnullfields1 = DropNullFields.apply(frame=resolvechoice1, transformation_ctx="dropnullfields1")
            print resolvechoice1.count()
            # resolvechoice1.printSchema()
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
            #                                                            redshift_tmp_dir="s3a://dtsodin/temp/student_behavior",
            #                                                            transformation_ctx="datasink1")

            df_temp = dyf_ticket_log.toDF()
            flag = df_temp.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            # ghi de _key vao s3
            df.write.parquet("s3a://dtsodin/flag/flag_hoc_vien_rating_native_smile_caresoft.parquet", mode="overwrite")
        except Exception as e:
            print e


if __name__ == "__main__":
    main()
