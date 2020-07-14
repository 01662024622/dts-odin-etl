import sys

from pyspark import StorageLevel

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql.functions import when
from datetime import date, datetime, timedelta
import pytz
import hashlib

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

is_dev = True
REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'

is_checking = True
student_id_checking = '328202'


def get_df_student_level(glueContext):
    dyf_student_level = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_level",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_level"}
    )

    dyf_student_level = dyf_student_level.select_fields(
        ['contact_id', 'level_code', 'start_date', 'end_date']) \
        .rename_field('contact_id', 'contact_id_level')

    if is_dev:
        print('dyf_student_level__original')
        dyf_student_level.printSchema()
        dyf_student_level.show(3)

    return dyf_student_level.toDF()


def get_df_student_package(glueContext):
    dyf_student_package = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_package",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )

    dyf_student_package = dyf_student_package.select_fields(
        ['contact_id', 'package_code', 'package_status_code',
         'package_start_time', 'package_end_time']) \
        .rename_field('contact_id', 'contact_id_package')

    if is_dev:
        print('dyf_student_package__original')
        dyf_student_package.printSchema()
        dyf_student_package.show(3)

    return dyf_student_package.toDF()


def get_df_student_advisor(glueContext):
    dyf_student_advisor = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/transaction_log",
            "user": REDSHIFT_USERNAME,
            "password": REDSHIFT_PASSWORD,
            "dbtable": "ad_student_advisor",
            "redshiftTmpDir": "s3n://datashine-dev-redshift-backup/translation_log/user_advisor/ad_student_package"}
    )

    dyf_student_advisor = dyf_student_advisor.select_fields(
        ['contact_id', 'advisor_id', 'start_date',
         'end_date']) \
        .rename_field('contact_id', 'contact_id_advisor')

    if is_dev:
        print('dyf_student_advisor__original')
        dyf_student_advisor.printSchema()
        dyf_student_advisor.show(3)

    return dyf_student_advisor.toDF()


def get_df_mdl_logsservice_move_user():
    dyf_mdl_logsservice_move_user = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                                  table_name="mdl_logsservice_move_user")

    dyf_mdl_logsservice_move_user = dyf_mdl_logsservice_move_user \
        .select_fields(['id', 'device_type', 'roomidto', 'userid']) \
        .rename_field('id', 'move_use_id')\
        .rename_field('userid', 'userid_move_user')

    df_mdl_logsservice_move_user = dyf_mdl_logsservice_move_user.toDF()
    df_mdl_logsservice_move_user = df_mdl_logsservice_move_user.dropDuplicates(['roomidto', 'userid_move_user'])
    df_mdl_logsservice_move_user.cache()

    return df_mdl_logsservice_move_user

def get_df_join_room_history():
    dyf_join_room_history = glueContext\
        .create_dynamic_frame.from_catalog(database="portal_topicanative",
                                         table_name="join_room_history")

    dyf_join_room_history = dyf_join_room_history.select_fields(
        ['id', 'user_id', 'room_id', 'role', 'vcr_type'])\
        .rename_field('user_id', 'user_id_join_room')\
        .rename_field('room_id', 'room_id_join_room')\
        .rename_field('role', 'role_in_class')

    dyf_join_room_history = dyf_join_room_history.resolveChoice(specs=[('user_id_join_room', 'cast:string')])

    dyf_join_room_history = Filter.apply(frame=dyf_join_room_history,
                                          f=lambda x: x["user_id_join_room"] is not None
                                                      and x["room_id_join_room"] is not None
                                          )


    df_join_room_history = dyf_join_room_history.toDF()

    df_join_room_history = df_join_room_history.dropDuplicates(['user_id_join_room', 'room_id_join_room'])
    df_join_room_history.cache()

    return df_join_room_history


def main():

    # get dynamic frame source
    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second = int(today.strftime("%s"))
    print('today_id: ', today_second)

    # df_student_level = get_df_student_level(glueContext)
    # df_student_level.cache()
    df_student_package = get_df_student_package(glueContext)
    df_student_package.cache()
    df_student_advisor = get_df_student_advisor(glueContext)
    df_student_advisor.cache()

    BEHAVIOR_ID_LS = 11
    BEHAVIOR_ID_SC = 12
    MAX_TIME_STUDY = 45 * 60

    # ------------------------------------------------------------------------------------------------------------------#
    # test
    # ------------------------------------------------------------------------------------------------------------------#

    def concaText(student_behavior_date, behavior_id, student_id, contact_id,
                  package_code, package_status_code, student_level_code,
                  transformed_at):
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
        if student_level_code is not None:
            text_concat += str(student_level_code)
        if package_status_code is not None:
            text_concat += str(package_status_code)
        if transformed_at is not None:
            text_concat += str(transformed_at)
        return text_concat

    concaText = f.udf(concaText, StringType())
    # ------------------------------------------------------------------------------------------------------------------#

    # ------------------------------------------------------------------------------------------------------------------#
    mdl_logsservice_in_out = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                           table_name="mdl_logsservice_in_out_cutoff_2020")
    mdl_logsservice_in_out = Filter.apply(frame=mdl_logsservice_in_out,
                                          f=lambda x: x["time_in"] >= 1483203600
                                                      and x["time_out"] is not None and x["time_out"] != ''
                                                      and x["time_out"] > x["time_in"]
                                          )
    # Chon cac truong can thiet
    mdl_logsservice_in_out = mdl_logsservice_in_out.select_fields(
        ['_key', 'id', 'userid', 'roomid', 'time_in', 'time_out', 'date_in'])
    mdl_logsservice_in_out = mdl_logsservice_in_out.resolveChoice(specs=[('_key', 'cast:long')])

    if is_checking:
        print('mdl_logsservice_in_out')
        mdl_logsservice_in_out.printSchema()

    # try:
    #     df_flag = spark.read.parquet("s3a://toxd-olap/transaction_log/flag/sb_student_learning/sb_student_learning.parquet")
    #     read_from_index = df_flag.collect()[0]['flag']
    #     print('read from index: ', read_from_index)
    #     mdl_logsservice_in_out = Filter.apply(frame=mdl_logsservice_in_out,
    #                                             f=lambda x: x["_key"] > read_from_index)
    # except:
    #     print('read flag file error ')


    if is_checking:
        mdl_logsservice_in_out = Filter.apply(frame=mdl_logsservice_in_out,
                                          f=lambda x: x["userid"] == student_id_checking
                                              and x["time_in"] >= 1585674000
                                          )

        print('mdl_logsservice_in_out::checking')
        mdl_logsservice_in_out.show(10)

    number_logsservice_in_out = mdl_logsservice_in_out.count()
    print('number_logsservice_in_out: ', number_logsservice_in_out)

    if (number_logsservice_in_out > 0):

        mdl_tpebbb = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                   table_name="mdl_tpebbb")
        mdl_tpebbb = mdl_tpebbb.select_fields(['id', 'timeavailable', 'calendar_code', 'roomtype',
                                               'subject_code']) \
            .rename_field('id', 'room_id')
        mdl_tpe_calendar_teach = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                               table_name="mdl_tpe_calendar_teach")
        mdl_tpe_calendar_teach = mdl_tpe_calendar_teach.select_fields(
            ['status', 'calendar_code', 'type_class', 'hour_id', 'teacher_id', 'assistant_id',
             'level_class', 'teacher_type']).rename_field(
            'calendar_code', 'code_calendar')

        mdl_logsservice_room_start = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
                                                                                   table_name="mdl_logsservice_room_start")
        mdl_logsservice_room_start = mdl_logsservice_room_start.select_fields(
            ['roomid', 'timecreated']).rename_field(
            'roomid', 'id_room')

        # mdl_role_assignments = glueContext.create_dynamic_frame.from_catalog(database="topicalms",
        #                                                                      table_name="mdl_role_assignments")
        # mdl_role_assignments = mdl_role_assignments.select_fields(['userid', 'roleid']).rename_field('userid',
        #                                                                                              'user_id')

        # Loc du lieu
        mdl_tpe_calendar_teach = Filter.apply(frame=mdl_tpe_calendar_teach, f=lambda x: x["status"] >= 0)

        data_tpe_bbb = Filter.apply(frame=mdl_tpebbb, f=lambda x: x["roomtype"] == 'ROOM' and (
                x["calendar_code"] is not None and x["calendar_code"] != ''))

        join_calendar_teach = Join.apply(data_tpe_bbb, mdl_tpe_calendar_teach, 'calendar_code',
                                         'code_calendar').drop_fields(['calendar_code', 'code_calendar'])

        data_in_out = Filter.apply(frame=mdl_logsservice_in_out, f=lambda x: x["time_out"] is not None and (
                x["userid"] is not None and x["userid"] != '') and (x["roomid"] is not None and x["roomid"] != ''))

        # data_mdl_role_assignments = Filter.apply(frame=mdl_role_assignments,
        #                                          f=lambda x: x["roleid"] == '5' and x["user_id"] is not None)
        #
        # join_data_role = Join.apply(data_in_out, data_mdl_role_assignments, 'userid', 'user_id')

        join_data_role = data_in_out

        # map ls lssc vs thong tin lop
        join_data_tpebbb = Join.apply(join_data_role, join_calendar_teach, 'roomid', 'room_id')

        mdl_logsservice_room_start = Filter.apply(frame=mdl_logsservice_room_start,
                                                  f=lambda x: x["id_room"] is not None and x["id_room"] != '')

        df_data_roomstart = mdl_logsservice_room_start.toDF()
        df_data_tpebbb = join_data_tpebbb.toDF()

        print("Count data 222:  ", df_data_tpebbb.count())

        df_data_tpebbb.printSchema()
        # df_data_tpebbb.show()
        # map ls lssc vs thong tin mo lop
        join_bbb = df_data_tpebbb.join(df_data_roomstart, df_data_tpebbb.roomid == df_data_roomstart.id_room,
                                       'left_outer')

        data_bbb = DynamicFrame.fromDF(join_bbb, glueContext, "data_bbb")

        # convert data
        df_bbb = data_bbb.toDF()
        df_bbb = df_bbb.drop_duplicates(["id"])
        df_bbb.cache()
        df_bbb = df_bbb.withColumn('time_start',
                                   when(f.col("timecreated").isNull(), df_bbb['timeavailable']).otherwise(
                                       df_bbb['timecreated']))
        df_bbb = df_bbb.withColumn('timein',
                                   when(df_bbb.time_in < df_bbb.time_start, df_bbb['time_start']).otherwise(
                                       df_bbb['time_in']))
        df_bbb = df_bbb.withColumn('time_study',
                                   when((df_bbb.time_out < df_bbb.time_in) | (df_bbb.time_out < df_bbb.time_start),
                                        f.lit(0)).otherwise(df_bbb.time_out - df_bbb.timein))

        print('df_bbb__1')
        df_bbb.printSchema()
        df_bbb.show(3)

        df_lssc_log = df_bbb.groupBy('userid', 'roomid').agg(
            f.first('type_class').alias('type_class'),
            f.first('assistant_id').alias('assistant_id'),
            f.first('hour_id').alias('hour_id'),
            f.first('roomtype').alias('roomtype'),
            f.first('subject_code').alias('subject_code'),
            f.first('status').alias('status'),
            f.first('level_class').alias('level_class'),

            f.first('teacher_type').alias('teacher_type'),
            f.first('teacher_id').cast('long').alias('teacher_id'),
            f.when(f.sum('time_study') >= MAX_TIME_STUDY, MAX_TIME_STUDY).otherwise(f.sum('time_study')).cast(
                'long').alias('time_study'),
            f.min('time_in').cast('long').alias('student_behavior_date'),  # student_behavior_date == time_in
            f.max('time_out').cast('long').alias('time_out'),
            f.count('time_in').cast('long').alias('number_in_out')
        )
        df_lssc_log = df_lssc_log \
            .withColumn('behavior_id',
                        when(df_lssc_log.type_class == 'LS', f.lit(BEHAVIOR_ID_LS)).otherwise(f.lit(BEHAVIOR_ID_SC)).cast('long'))

        print('df_lssc_log')
        df_lssc_log.printSchema()
        df_lssc_log.show(1)

        # --------------------------------------------------------------------------------------------------------------#
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
        dy_student_contact = dy_student_contact.dropDuplicates(['student_id'])
        # --------------------------------------------------------------------------------------------------------------#

        #get role_in_class
        df_join_room_history = get_df_join_room_history()
        df_mdl_logsservice_move_user = get_df_mdl_logsservice_move_user()


        df_lssc_log = df_lssc_log\
            .join(dy_student_contact,
                   on = df_lssc_log.userid == dy_student_contact.student_id,
                   how='left') \
            .join(df_mdl_logsservice_move_user,
                    on=(df_lssc_log.roomid == df_mdl_logsservice_move_user.roomidto)
                    &(df_lssc_log.userid == df_mdl_logsservice_move_user.move_use_id),
                    how='left')\
            .join(df_join_room_history,
                  on=(df_lssc_log.roomid == df_join_room_history.room_id_join_room)
                  & (df_lssc_log.userid == df_join_room_history.user_id_join_room),
                  how='left'
                  )

        # --------------------------------------------------------------------------------------------------------------#

        # if is_dev:
        #     print ('join_result::before::join package, status, level')
        #     print('join_result::number: ', df_lssc_log.count())
        #     df_lssc_log.show(3)

        join_result = df_lssc_log

        join_result = join_result \
            .join(df_student_advisor,
                  (join_result.contact_id == df_student_advisor.contact_id_advisor)
                  & (join_result.student_behavior_date >= df_student_advisor.start_date)
                  & (join_result.student_behavior_date < df_student_advisor.end_date),
                  'left'
                  ) \
            .join(df_student_package,
                  (join_result.contact_id == df_student_package.contact_id_package)
                  & (join_result.student_behavior_date >= df_student_package.package_start_time)
                  & (join_result.student_behavior_date < df_student_package.package_end_time),
                  'left'
                  ) \

        if is_dev:
            print ('join_result::after::join package, status, level')
            print('join_result::number: ', join_result.count())

            join_result.printSchema()
            join_result.show(3)

        contact_id_unavailable = 0
        student_id_unavailable = 0
        package_endtime_unavailable = 99999999999
        package_starttime_unavailable = 0
        student_level_code_unavailable = 'UNAVAILABLE'
        student_status_code_unavailable = 'UNAVAILABLE'
        package_code_unavailable = 'UNAVAILABLE'
        class_type_unavailable = 'UNAVAILABLE'
        teacher_type_unavailable = 'UNAVAILABLE'
        advisor_unavailable = 0
        measure1_unavailable = float(0.0)
        measure2_unavailable = float(0.0)
        measure3_unavailable = float(0.0)
        measure4_unavailable = float(0.0)
        role_in_class_unavailable = 'UNAVAILABLE'


        join_result = join_result.select(
            'student_behavior_date',
            'behavior_id',
            join_result.userid.cast('long').alias('student_id'),
            join_result.contact_id.cast('string').alias('contact_id'),
            'package_code',
            'package_status_code',

            f.col('level_class').alias('student_level_code'),

            'advisor_id',

            f.lit(today_second).cast('long').alias('transformed_at'),

            join_result.type_class.alias('class_type'),
            f.when(join_result.device_type == 'MOBILE', 'MOBILE').otherwise('WEB').alias('platform'),
            'subject_code',
            join_result.teacher_id.cast('long').alias('teacher_id'),
            'teacher_type',

            join_result.assistant_id.cast('long').alias('assistant_id'),
            'hour_id',  # int

            join_result.student_behavior_date.cast('long').alias('start_learning_time'),
            join_result.time_out.cast('long').alias('end_learning_time'),
            join_result.time_study.cast('long').alias('duration'),

            f.from_unixtime('student_behavior_date', format="yyyyMM").alias('year_month_id'),

            'role_in_class',

            'number_in_out',

            'vcr_type'
        )

        join_result = join_result.na.fill({
            'package_code': package_code_unavailable,

            'student_level_code': student_level_code_unavailable,
            'package_status_code': student_status_code_unavailable,
            'class_type': class_type_unavailable,

            'teacher_type': teacher_type_unavailable,

            'advisor_id': advisor_unavailable,

            'role_in_class': role_in_class_unavailable,

            'number_in_out': 0,

            'vcr_type': 'UNAVAILABLE'
        })

        print('join_result::2')
        join_result.printSchema()
        join_result = join_result.withColumn('student_behavior_id',
                                             f.md5(concaText(
                                                 join_result.student_behavior_date,
                                                 join_result.behavior_id,
                                                 join_result.student_id,
                                                 join_result.contact_id,
                                                 join_result.package_code,
                                                 join_result.package_status_code,
                                                 join_result.student_level_code,
                                                 join_result.transformed_at)))

        join_result = join_result.dropDuplicates(['student_behavior_id'])

        join_result.persist(StorageLevel.DISK_ONLY_2)
        df_bbb.unpersist()

        #
        print('join_result--2')
        join_result.printSchema()
        join_result.show(5)

        dyf_join_result = DynamicFrame.fromDF(join_result, glueContext, 'dyf_join_result')

        apply_ouput_behavior = ApplyMapping\
            .apply(frame=dyf_join_result,
                  mappings=[("student_behavior_id", "string", "student_behavior_id", "string"),
                            ("student_behavior_date", "long", "student_behavior_date", "long"),
                            ("behavior_id", "long", "behavior_id", "int"),
                            ("student_id", "long", "student_id", "long"),
                            ("contact_id", "string", "contact_id", "string"),

                            ("package_code", "string", "package_code", "string"),


                            ("student_level_code", "string", "student_level_code", "string"),
                            ("package_status_code", "string", "package_status_code", "string"),

                            ("advisor_id", "long", "advisor_id", "long"),

                            ("transformed_at", "long", "transformed_at", "long"),

                            ("year_month_id", "string", "year_month_id", "long")
                            ])

        dfy_output_behavior = ResolveChoice.apply(frame=apply_ouput_behavior, choice="make_cols",
                                                  transformation_ctx="resolvechoice2")
        #

        # save to s3
        glueContext.write_dynamic_frame.from_options(
            frame=dfy_output_behavior,
            connection_type="s3",
            connection_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_behavior",
                                "partitionKeys": ["behavior_id", "year_month_id"]},
            format="parquet")

        # save to redshift

        # save student_learinng
        apply_ouput_behavior = ApplyMapping.apply(frame=dyf_join_result,
                                                  mappings=[("student_behavior_id", "string", "student_behavior_id",
                                                             "string"),
                                                            ("class_type", "string", "class_type", "string"),
                                                            ("platform", "string", "platform", "string"),

                                                            ("teacher_id", "long", "teacher_id", "long"),
                                                            ("teacher_type", "string", "teacher_type", "string"),

                                                            ("assistant_id", "long", "assistant_id", "long"),

                                                            ("hour_id", "int", "hour_id", "int"),

                                                            ("start_learning_time", "long", "start_learning_time",
                                                             "long"),
                                                            ("end_learning_time", "long", "end_learning_time", "long"),
                                                            ("duration", "long", "duration", "long"),

                                                            ("behavior_id", "long", "behavior_id", "int"),

                                                            ("year_month_id", "string", "year_month_id", "long"),

                                                            ("role_in_class", "string", "role_in_class", "string"),

                                                            ("number_in_out", "long", "number_in_out", "long"),

                                                            ("vcr_type", "string", "vcr_type", "string")

                                                            ])


        dfy_output_behavior = ResolveChoice.apply(frame=apply_ouput_behavior, choice="make_cols",
                                                  transformation_ctx="resolvechoice2")
        # save to s3
        glueContext.write_dynamic_frame.from_options(
            frame=dfy_output_behavior,
            connection_type="s3",
            connection_options={"path": "s3://toxd-olap/transaction_log/student_behavior/sb_student_learning",
                                "partitionKeys": ["behavior_id",  "year_month_id"]},
            format="parquet")

        df_mdl_logsservice_in_out = mdl_logsservice_in_out.toDF()
        flag = df_mdl_logsservice_in_out.agg({"_key": "max"}).collect()[0][0]
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')
        df.write.parquet("s3a://toxd-olap/transaction_log/flag/sb_student_learning/sb_student_learning.parquet", mode="overwrite")

        join_result.unpersist()

if __name__ == "__main__":
    main()

# job.commit()



