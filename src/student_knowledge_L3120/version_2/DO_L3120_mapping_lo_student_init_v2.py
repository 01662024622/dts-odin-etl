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
from pyspark.sql.types import StringType, LongType
from pyspark.sql.types import ArrayType


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    is_dev = True
    score_pass = 50

    arr_level = [["basic100", 9], ["inter100", 12], ["basic300", 11], ["basic", 9], ["basic200", 10]
        , ["sbasic", 8], ["inter300", 14], ["inter200", 13], ["inter", 12]
        , ["starter", 7], ["advan", 15], ["advan100", 15], ["advan300", 15]]

    def get_score(value, score):
        if value is None:
            return 0L
        return value * score

    get_score = udf(get_score, LongType())

    # Custom function
    def do_add_pass_date(status, date):
        if status == 1:
            return date
        return None

    # print('test:', doSplitWord('abcacd'))
    # add_pass_date = udf(lambda x: do_add_pass_date(x))
    add_pass_date = udf(do_add_pass_date, StringType())

    # ------------------------------------------------------------------------------------------------------------------#
    dyf_mapping_lo_class = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_class"
    )

    dyf_mapping_lo_class = dyf_mapping_lo_class \
        .select_fields(
        ['class_id', 'learning_object_id', 'knowledge', 'comprehension', 'application', 'analysis', 'synthesis',
         'evaluation'])

    # 'knowledge > 0 or comprehension > 0 or application > 0 or analysis > 0 or synthesis > 0 or evaluation > 0')
    dyf_mapping_lo_class = Filter.apply(frame=dyf_mapping_lo_class,
                                        f=lambda x: x["class_id"] is not None \
                                                    and x["class_id"] != '' \
                                                    and (x["knowledge"] > 0
                                                         or x["comprehension"] > 0
                                                         or x["application"] > 0
                                                         or x["analysis"] > 0
                                                         or x["synthesis"] > 0
                                                         or x["evaluation"] > 0
                                                         )
                                        )

    dyf_mapping_lo_class = dyf_mapping_lo_class.rename_field('class_id', 'class_id_lo_class')

    df_mapping_lo_class = dyf_mapping_lo_class.toDF()

    if is_dev:
        print ('df_mapping_lo_class')
        df_mapping_lo_class.printSchema()
        df_mapping_lo_class.show(3)

    # ------------------------------------------------------------------------------------------------------------------#
    # dyf_learning_object_class = glueContext.create_dynamic_frame.from_catalog(
    #     database="nvn_knowledge",
    #     table_name="learning_object_class"
    # )

    # Loc chi lay nhung ttrinh do cua topica
    # dyf_learning_object_class_top = Filter.apply(frame=dyf_learning_object_class, f=lambda x: x['class_parent_id'] == 1)
    #
    # dyf_learning_object_class_top = dyf_learning_object_class_top.select_fields(
    #     ['class_id', 'class_name', 'display_order'])
    #
    # df_learning_object_class_top = dyf_learning_object_class_top.toDF()
    #
    # if is_dev:
    #     print ('df_learning_object_class_top')
    #     df_learning_object_class_top.printSchema()
    #     df_learning_object_class_top.show()

    # ------------------------------------------------------------------------------------------------------------------#
    ########## dyf_learning_object_class
    # dyf_mapping_class_lms = glueContext.create_dynamic_frame.from_catalog(
    #     database="nvn_knowledge",
    #     table_name="mapping_class_lms"
    # )

    dyf_mapping_class_lms = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "mapping_class_lms",
            "redshiftTmpDir": "s3://dts-odin/temp1/dyf_mapping_lo_student_current/v9"}
    )

    df_mapping_class_lms = dyf_mapping_class_lms.toDF()
    if is_dev:
        print ('df_mapping_class_lms')
        df_mapping_class_lms.printSchema()
        df_mapping_class_lms.show(3)
    # ------------------------------------------------------------------------------------------------------------------#



    # -----------------------------------------------------------------------------------------------------------------#
    dyf_log_student_level_study = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="log_student_level_study"
    )

    if is_dev:
        print ('dyf_log_student_level_study')
        dyf_log_student_level_study.printSchema()
        dyf_log_student_level_study.show(3)

    dyf_log_student_level_study = Filter.apply(frame=dyf_log_student_level_study,
                                               f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                           and x["level_modified"] is not None and x[
                                                               "level_modified"] != ''
                                                           and x["time_created"] is not None)

    dyf_log_student_level_study = dyf_log_student_level_study.select_fields(
        ['contact_id', 'level_modified', 'time_created'])

    df_log_student_level_study = dyf_log_student_level_study.toDF()
    df_log_student_level_study = df_log_student_level_study.dropDuplicates(['contact_id'])

    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )

    dyf_student_contact = Filter.apply(frame=dyf_student_contact,
                                       f=lambda x: x["contact_id"] is not None and x["contact_id"] != ''
                                                   and x["student_id"] is not None and x["student_id"] != '')

    dyf_student_contact = dyf_student_contact.select_fields(
        ['contact_id', 'student_id'])

    df_student_contact = dyf_student_contact.toDF()
    df_student_contact = df_student_contact.dropDuplicates(['student_id'])

    if is_dev:
        print ('df_student_contact')
        df_student_contact.printSchema()
        df_student_contact.show(3)

    df_log_student_level_study_student = df_log_student_level_study.join(df_student_contact, 'contact_id',
                                                                         'inner')
    df_log_student_level_study_student = df_log_student_level_study_student.drop('contact_id')

    df_log_student_level_study_student = df_log_student_level_study_student \
        .orderBy(f.asc('student_id'), f.desc('time_created'))

    if is_dev:
        print ('df_log_student_level_study_student')
        df_log_student_level_study_student.printSchema()
        df_log_student_level_study_student.show(3)

    df_student_current_level = df_log_student_level_study_student.groupBy('student_id').agg(
        f.from_unixtime(f.first('time_created'), format="yyyyMMdd").alias('level_changed_date'),
        f.first('level_modified').alias('level')
    )

    df_student_current_level.cache()

    if is_dev:
        print ('df_student_current_level')
        df_student_current_level.printSchema()
        df_student_current_level.show(10)

    # root
    # | -- student_id: string(nullable=true)
    # | -- level_changed_date: integer(nullable=true)
    # | -- level: string(nullable=true)

    # dyf_student_current_level = DynamicFrame.fromDF(df_student_current_level, glueContext, "dyf_student_current_level")
    #
    # datasink10 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_current_level,
    #                                                             catalog_connection="glue_redshift",
    #                                                             connection_options={
    #                                                                 "dbtable": "dyf_student_current_level",
    #                                                                 "database": "dts_odin"},
    #                                                             redshift_tmp_dir="s3n://dtsodin/temp3/test/df_student_current_level",
    #                                                             transformation_ctx="datasink10")

    # ------------------------------------------------------------------------------------------------------------------#

    # dyf_student_current_level = DynamicFrame.fromDF(df_student_current_level, glueContext,
    #                                                 "dyf_student_current_level")
    #
    # dyf_level_student_contact = Join.apply(dyf_student_current_level, dyf_mapping_lo_class, '', '')

    df_level_student_contact = df_student_current_level \
        .join(df_mapping_class_lms, df_student_current_level.level == df_mapping_class_lms.level_lms, 'inner') \
        .join(df_mapping_lo_class, df_mapping_class_lms.class_id == df_mapping_lo_class.class_id_lo_class, 'inner')

    df_level_student_contact.cache()

    if is_dev:
        print ('df_level_student_contact')
        df_level_student_contact.printSchema()
        print('df_level_student_contact::number: ', df_level_student_contact.count())
        # df_level_student_contact.show(10)

    # df_level_student_contact = df_level_student_contact.select(
    #     'student_id',
    #     'class_id',
    #     'display_order',
    #     df_level_student_contact.level_changed_date.alias('modified_date_id')
    # )

    # get student_user list
    # df_student_id_list = df_level_student_contact.select('student_id')

    if is_dev:
        print ('df_level_student_contact')
        df_level_student_contact.printSchema()
        df_level_student_contact.show(10)

    # df_mapping_lo_class = df_mapping_lo_class.where(
    #     'knowledge > 0 or comprehension > 0 or application > 0 or analysis > 0 or synthesis > 0 or evaluation > 0')

    # dyf_level_student_contact = DynamicFrame.fromDF(df_level_student_contact, glueContext,
    #                                                   "dyf_level_student_contact")
    #
    # dyf_level_student_contact_join = Join.apply(dyf_level_student_contact, dyf_mapping_lo_class, 'class_id', 'class_id')
    #
    # df_level_student_contact_lo = dyf_level_student_contact_join.toDF()

    # df_mapping_lo_class = dyf_mapping_lo_class.toDF()
    #
    # df_level_student_contact_lo = df_level_student_contact.join(df_mapping_lo_class, 'class_id', 'inner')

    # if is_dev:
    #     print ('df_level_student_contact_lo')
    #     df_level_student_contact_lo.printSchema()
    #     df_level_student_contact_lo.show(10)

    df_level_student_contact_lo = df_level_student_contact.select(
        'student_id', 'learning_object_id', 'level_changed_date',
        get_score(df_level_student_contact.knowledge, f.lit(score_pass)).alias('knowledge_t'),
        get_score(df_level_student_contact.comprehension, f.lit(score_pass)).alias('comprehension_t'),
        get_score(df_level_student_contact.application, f.lit(score_pass)).alias('application_t'),
        get_score(df_level_student_contact.analysis, f.lit(score_pass)).alias('analysis_t'),
        get_score(df_level_student_contact.synthesis, f.lit(score_pass)).alias('synthesis_t'),
        get_score(df_level_student_contact.evaluation, f.lit(score_pass)).alias('evaluation_t'),

        add_pass_date(df_level_student_contact.knowledge, df_level_student_contact.level_changed_date)
            .alias('knowledge_pass_date_id'),
        add_pass_date(df_level_student_contact.comprehension, df_level_student_contact.level_changed_date)
            .alias('comprehension_pass_date_id'),
        add_pass_date(df_level_student_contact.application, df_level_student_contact.level_changed_date)
            .alias('application_pass_date_id'),
        add_pass_date(df_level_student_contact.analysis, df_level_student_contact.level_changed_date)
            .alias('analysis_pass_date_id'),
        add_pass_date(df_level_student_contact.synthesis, df_level_student_contact.level_changed_date)
            .alias('synthesis_pass_date_id'),
        add_pass_date(df_level_student_contact.evaluation, df_level_student_contact.level_changed_date)
            .alias('evaluation_pass_date_id')
    )

    if is_dev:
        print ('df_level_student_contact_lo')
        df_level_student_contact_lo.printSchema()
        df_level_student_contact_lo.show(10)

    dyf_level_student_contact_lo = DynamicFrame.fromDF(df_level_student_contact_lo, glueContext,
                                                       "dyf_level_student_contact_lo")

    applymapping = ApplyMapping.apply(frame=dyf_level_student_contact_lo,
                                      mappings=[("student_id", "string", "student_id", "long"),
                                                ("learning_object_id", "long", "learning_object_id", "long"),
                                                ("level_changed_date", "string", "modified_date_id", "long"),
                                                ("level_changed_date", "string", "created_date_id", "long"),
                                                ("knowledge_t", 'long', 'knowledge', 'long'),
                                                ("comprehension_t", 'long', 'comprehension', 'long'),
                                                ("comprehension_t", 'long', 'application', 'long'),
                                                ("analysis_t", 'long', 'analysis', 'long'),
                                                ("synthesis_t", 'long', 'synthesis', 'long'),
                                                ("evaluation_t", 'long', 'evaluation', 'long'),
                                                ("knowledge_pass_date_id", 'string', 'knowledge_pass_date_id', 'long'),
                                                ("comprehension_pass_date_id", 'string', 'comprehension_pass_date_id',
                                                 'long'),
                                                ("application_pass_date_id", 'string', 'application_pass_date_id',
                                                 'long'),
                                                ("analysis_pass_date_id", 'string', 'analysis_pass_date_id', 'long'),
                                                ("synthesis_pass_date_id", 'string', 'synthesis_pass_date_id', 'long'),
                                                (
                                                    "evaluation_pass_date_id", 'string', 'evaluation_pass_date_id',
                                                    'long'),
                                                ("user_id", 'string', 'user_id', 'long')])
    resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                        transformation_ctx="resolvechoice2")
    dyf_student_lo_init = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dyf_student_lo_init")
    datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_lo_init,
                                                               catalog_connection="glue_redshift",
                                                               connection_options={
                                                                   "dbtable": "mapping_lo_student_init_v2",
                                                                   "database": "dts_odin"
                                                               },
                                                               redshift_tmp_dir="s3n://dtsodin/temp1/mapping_lo_student_init_v2",
                                                               transformation_ctx="datasink5")

    # ------------------------------------------------------------------------------------------------------------------#

    df_student_current_level.unpersist()
    df_level_student_contact.unpersist()


if __name__ == "__main__":
    main()
