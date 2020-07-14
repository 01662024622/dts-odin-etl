import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import when
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType
from datetime import date, datetime, timedelta

import pytz


def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    # job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    is_dev = True

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = long(today.strftime("%s"))
    print('today_id: ', today_second)


    #------------------------------------------------------------------------------------------------------------------#


    # ------------------------------------------------------------------------------------------------------------------#

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

    def convertStudentIdToLong(student_id):
        try:
            student_id_long = long(student_id)
            return student_id_long
        except:
            return 0L

    convertStudentIdToLong = f.udf(convertStudentIdToLong, LongType())
    # ------------------------------------------------------------------------------------------------------------------#

        ##################################################
    # Lay du lieu kiem tra ky thuat trong bang student_technical_test
    dyf_datasourceTech = glueContext.create_dynamic_frame.from_catalog(database="dm_toa",
                                                                   table_name="student_technical_test_odin")


    # print('dyf_datasourceTech')
    # dyf_datasourceTech.printSchema()

    # Chon cac truong can thiet
    dyf_datasourceTech = dyf_datasourceTech.select_fields(['_key', 'thoigianhenktkt', 'ketluan', 'emailhocvien',
                                                           'dauthoigian', 'emailadvisor', 'nguoitiepnhan', 'trinhdohocvien'])

    dyf_datasourceTech = dyf_datasourceTech.resolveChoice(specs=[('_key', 'cast:long')])

    if (dyf_datasourceTech.count() > 0):
        dyf_datasourceTech = Filter.apply(frame=dyf_datasourceTech,
                                        f=lambda x: x["emailhocvien"] is not None and x["emailhocvien"] != ''
                                                    and x["thoigianhenktkt"] is not None and x["thoigianhenktkt"] != ''
                                                    and x["ketluan"] == 'Pass')

        dyf_datasourceTech_numeber = dyf_datasourceTech.count()
        print("Count data 2:  ", dyf_datasourceTech_numeber)

        if dyf_datasourceTech_numeber < 1:
            return

        dy_datasourceTech = dyf_datasourceTech.toDF()


        dy_datasourceTech = dy_datasourceTech.limit(100)




        print('dy_datasourceTech')
        dy_datasourceTech.printSchema()
        dy_datasourceTech = dy_datasourceTech.withColumn('thoigianhenktkt_id',
                                                         f.unix_timestamp('thoigianhenktkt', 'yyyy-MM-dd HH:mm:ss').cast('long'))

        print('dy_datasourceTech__2')
        dy_datasourceTech.printSchema()

        # lay thoi gian kich hoat dau tien
        w2 = Window.partitionBy("emailhocvien").orderBy(f.col("thoigianhenktkt_id").desc())
        dy_datasourceTech = dy_datasourceTech.withColumn("row", f.row_number().over(w2)) \
            .where(f.col('row') <= 1) \


        print('dy_datasourceTech__3')
        dy_datasourceTech.printSchema()


        #--------------------------------------------------------------------------------------------------------------#
        dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
            database="tig_advisor",
            table_name="student_contact"
        )

        # chon cac field
        dyf_student_contact = dyf_student_contact.select_fields(['_key', 'contact_id', 'student_id', 'user_name'])

        dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                          f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                      and x["student_id"] is not None and x["student_id"] != ''
                                                      and x["user_name"] is not None and x["user_name"] != '')

        dyf_student_contact_number = dyf_student_contact.count()
        print('dyf_student_contact_number::number: ', dyf_student_contact_number)
        if dyf_student_contact_number < 1:
            return

        dy_student_contact = dyf_student_contact.toDF()
        dy_student_contact.dropDuplicates(['student_id'])

        dy_join_teach_concat = dy_datasourceTech.join(dy_student_contact,
                                          dy_datasourceTech.emailhocvien == dy_student_contact.user_name)


        print('dyf_join_teach_concat::schema')
        dy_join_teach_concat.printSchema()

        join_teach_concat_number = dy_join_teach_concat.count()
        print('join_teach_concat_number::number: ', join_teach_concat_number)
        if join_teach_concat_number < 1:
            return

        #--------------------------------------------------------------------------------------------------------------#

        dyf_student_package_status = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
                                                                           table_name="student_status")

        dyf_student_package_status = dyf_student_package_status\
            .select_fields(['contact_id', 'status_code', 'start_date', 'end_date'])\
            .rename_field('contact_id', 'contact_id_ps')

        print('dyf_student_package_status::drop_duplicates')

        df_student_package_status = dyf_student_package_status.toDF()
        print('dyf_student_package_status::drop_duplicates::before: ', df_student_package_status.count())
        df_student_package_status = df_student_package_status.drop_duplicates()
        print('dyf_student_package_status::drop_duplicates::after: ', df_student_package_status.count())

        print('dy_student_package_status')
        df_student_package_status.printSchema()
        # --------------------------------------------------------------------------------------------------------------#
        dyf_student_package = glueContext.create_dynamic_frame.from_catalog(database="od_student_behavior",
                                                                                   table_name="student_package")


        print('dyf_student_package__0')
        dyf_student_package.printSchema()


        dyf_student_package = dyf_student_package \
            .select_fields(['student_id', 'package_code', 'start_time', 'end_time'])\
            .rename_field('student_id', 'student_id_pk')

        # --------------------------------------------------------------------------------------------------------------#



        print('dyf_student_package__1')
        dyf_student_package.printSchema()


        dyf_student_package = dyf_student_package.resolveChoice(specs=[('start_time', 'cast:long'), ('end_time', 'cast:long')])

        print('dyf_student_package__2')
        dyf_student_package.printSchema()

        df_student_package = dyf_student_package.toDF()
        print('df_student_package::drop_duplicates::before: ', df_student_package.count())
        df_student_package = df_student_package.drop_duplicates()
        print('df_student_package::drop_duplicates::after: ', df_student_package.count())

        print('df_student_package')
        df_student_package.printSchema()
        df_student_package.show(3)

        df_student_package_number = df_student_package.count()
        print('df_student_package_number: ', df_student_package_number)



        # --------------------------------------------------------------------------------------------------------------#


        # --------------------------------------------------------------------------------------------------------------#

        dy_join_teach_concat_number = dy_join_teach_concat.count()
        print('dy_join_teach_concat_number: ', dy_join_teach_concat_number)

        join_result = dy_join_teach_concat\
            .join(df_student_package_status,
                 (dy_join_teach_concat.contact_id == df_student_package_status.contact_id_ps)
                 & (dy_join_teach_concat.thoigianhenktkt_id >= df_student_package_status.start_date)
                 & (dy_join_teach_concat.thoigianhenktkt_id < df_student_package_status.end_date),
                 'left'
                  )\
            .join(df_student_package,
                  (dy_join_teach_concat.student_id == df_student_package.student_id_pk)
                  &(dy_join_teach_concat.thoigianhenktkt_id >= df_student_package.start_time)
                  &(dy_join_teach_concat.thoigianhenktkt_id < df_student_package.end_time),
                  'left'
                  )


        print('join_result')
        join_result.printSchema()

        join_result_number = join_result.count()
        print('join_result_number: ', join_result_number)
        if join_result_number < 1:
            return
        join_result.show(3)

        student_id_unavailable = 0L
        package_endtime_unavailable = 99999999999L
        package_starttime_unavailable = 0L
        package_code_unavailable = 'UNAVAILABLE'
        student_level_code_unavailable = 'UNAVAILABLE'
        student_status_code_unavailable = 'UNAVAILABLE'
        measure1_unavailable = 0
        measure2_unavailable = 0
        measure3_unavailable = 0
        measure4_unavailable = float(0.0)

        # join_result = join_result.withColumnRenamed('student_id', 'student_id_a')



        join_result = join_result.select(
            join_result.thoigianhenktkt_id.alias('student_behavior_date'),
            f.lit(5L).alias('behavior_id'),
            'student_id',
            join_result.contact_id.alias('contact_id'),

            join_result.package_code.alias('package_code'),
            join_result.end_time.cast('long').alias('package_endtime'),
            join_result.start_time.cast('long').alias('package_starttime'),

            join_result.trinhdohocvien.cast('string').alias('student_level_code'),
            join_result.status_code.cast('string').alias('student_status_code'),

            f.lit(today_second).cast('long').alias('transformed_at'),

        )

        join_result = join_result.na.fill({
            'package_code': package_code_unavailable,
            'package_endtime': package_starttime_unavailable,
            'package_starttime': package_endtime_unavailable,

            'student_level_code': student_level_code_unavailable,
            'student_status_code': student_status_code_unavailable
        })

        print('join_result--1')
        join_result.printSchema()
        join_result.show(1)

        join_result = join_result.withColumn('student_behavior_id',
                                                                f.md5(concaText(
                                                                    join_result.student_behavior_date,
                                                                    join_result.behavior_id,
                                                                    join_result.student_id,
                                                                    join_result.contact_id,
                                                                    join_result.package_code,
                                                                    join_result.package_endtime,
                                                                    join_result.package_starttime,
                                                                    join_result.student_level_code,
                                                                    join_result.student_status_code,
                                                                    join_result.transformed_at)))
        #
        print('join_result--2')
        join_result.printSchema()
        join_result.show(5)

        dyf_join_result = DynamicFrame.fromDF(join_result, glueContext, 'dyf_join_result')

        dyf_join_result = Filter.apply(frame=dyf_join_result,
                                             f=lambda x: x["contact_id"] is not None and x["contact_id"] != '')

        apply_ouput = ApplyMapping.apply(frame=dyf_join_result,
                                         mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                                                   ("student_behavior_date", "long", "student_behavior_date", "long"),
                                                   ("behavior_id", "long", "behavior_id", "long"),
                                                   ("student_id", "string", "student_id", "long"),
                                                   ("contact_id", "string", "contact_id", "string"),

                                                   ("package_code", "long", "package_code", "string"),
                                                   ("package_endtime", "long", "package_endtime", "long"),
                                                   ("package_starttime", "long", "package_starttime", "long"),

                                                   ("student_level_code", "string", "student_level_code", "string"),
                                                   ("student_status_code", "string", "student_status_code", "string"),

                                                   ("transformed_at", "long", "transformed_at", "long")
                                                   ])

        dfy_output = ResolveChoice.apply(frame=apply_ouput, choice="make_cols", transformation_ctx="resolvechoice2")
        #
        glueContext.write_dynamic_frame.from_options(
            frame=dfy_output,
            connection_type="s3",
            connection_options={"path": "s3://dtsodin/student_behavior/student_behavior",
                                "partitionKeys": ["behavior_id"]},
            format="parquet")


if __name__ == "__main__":
    main()
