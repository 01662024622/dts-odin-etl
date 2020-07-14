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
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    # learning_object_class = glueContext.create_dynamic_frame.from_options(
    #     connection_type="redshift",
    #     connection_options={
    #             "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
    #             "user": "datashine",
    #             "password": "TOXnative75dataredshift",
    #             "dbtable": "learning_object_class",
    #             "redshiftTmpDir": "s3n://dts-odin/temp1/learning_object_class/"}
    # )
    # Chi loc lay cac level cua topica
    # learning_object_class = Filter.apply(frame=learning_object_class, f=lambda x: x['class_parent_id'] == 54)
    # learning_object_class.printSchema()
    # learning_object_class.show()

    arr_level = [["basic100", 9], ["inter100", 12], ["basic300", 11], ["basic", 9], ["basic200", 10]
        , ["sbasic", 8], ["inter300", 14], ["inter200", 13], ["inter", 12]
        , ["starter", 7], ["advan", 15], ["advan100", 15], ["advan300", 15]]

    # Custom function
    def do_add_pass_date(status, date):
        if status == 1:
            return date
        return None

    # print('test:', doSplitWord('abcacd'))
    # add_pass_date = udf(lambda x: do_add_pass_date(x))
    add_pass_date = udf(do_add_pass_date, StringType())

    ########## dyf_learning_object_class
    dyf_learning_object_class = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="learning_object_class"
    )

    # Loc chi lay nhung ttrinh do cua topica
    dyf_learning_object_class_top = Filter.apply(frame=dyf_learning_object_class, f=lambda x: x['class_parent_id'] == 1)

    dyf_learning_object_class = dyf_learning_object_class.select_fields(
        ['class_id', 'class_name', 'display_order'])
    ########## dyf_learning_object_class
    dyf_mapping_class_lms = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_class_lms"
    )

    # dyf_learning_object_class_top.printSchema()

    dyf_learning_object_class_top = dyf_learning_object_class_top.rename_field('class_id', 'class_id_top') \
        .rename_field('display_order', 'display_order_top')
    # dyf_learning_object_class_top.printSchema()
    df_learning_object_class_top = dyf_learning_object_class_top.toDF()

    # df_learning_object_class_top = df_learning_object_class_top.where('class_parent_id = 1')
    df_learning_object_class_top = df_learning_object_class_top.select('class_id_top', 'display_order_top')

    ########## dyf_student_contact
    dyf_student_contact = glueContext.create_dynamic_frame.from_catalog(
        database="tig_advisor",
        table_name="student_contact"
    )
    dyf_student_contact = dyf_student_contact.select_fields(
        ['_key', 'contact_id', 'student_id', 'level_study', 'time_lms_created'])
    dyf_student_contact = dyf_student_contact.resolveChoice(specs=[('time_lms_created', 'cast:long')])

    # Doc cac ban ghi co time_lms_created_from = 01/10/2019
    time_lms_created_from = 1569891600

    # Khi chay lan dau: Filter lay nhung hoc vien trong nam 2019: Tu 01/01/2019 den 01/09/2019

    dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x['student_id'] is not None
                                                                              and x['contact_id'] is not None
                                                                              and x['time_lms_created'] is not None
                                                                              and x['time_lms_created'] >= time_lms_created_from)

    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3://dts-odin/flag/flag_log_time_lms_created.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)
        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x['time_lms_created'] > start_read)
        # dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x['student_id'] == '267896')
    except:
        print('read flag file error ')
    print('df_student_contact count 1:', dyf_student_contact.count())

    if dyf_student_contact.count() > 0:
        try:
            print("START......................")
            ### Check lay ban ghi moi nhat cua dyf_student_contact
            df_student_contact = dyf_student_contact.toDF()

            # dyf_student_contact.show()
            df_student_contact_max = df_student_contact.groupby('student_id').agg(f.max('_key').alias('max_key')) \
                .withColumnRenamed("student_id", "student_id_max")

            join_student_contact = df_student_contact_max.join(df_student_contact,
                                                               (df_student_contact_max['student_id_max']
                                                                == df_student_contact['student_id']) & (
                                                                       df_student_contact_max['max_key']
                                                                       == df_student_contact['_key']))
            # join_student_contact.show()
            dyf_student_contact = DynamicFrame.fromDF(join_student_contact, glueContext, 'dyf_student_contact')

            ### END Check lay ban ghi moi nhat cua dyf_student_contact
            dyf_mapping_lo_class = glueContext.create_dynamic_frame.from_catalog(
                database="nvn_knowledge",
                table_name="mapping_lo_class"
            )
            # dyf_student_level = glueContext.create_dynamic_frame.from_catalog(
            #     database="tig_advisor",
            #     table_name="log_student_level_study"
            # )
            # dyf_student_level = dyf_student_level.select_fields(
            #     ['_key', 'contact_id', 'level_current', 'level_modified', 'time_created']).rename_field('contact_id',
            #                                                                                             'contact_id1')

            # date_id = '20190901'
            # modified_time = 01/09/2019
            # modified_time = 1567299600
            # print ('Show 1')
            # dyf_student_contact.show()
            dyf_student_contact = Join.apply(dyf_student_contact, dyf_mapping_class_lms, 'level_study', 'level_lms')
            # print ('Show 2')
            # dyf_student_contact.show()
            # dyf_student_contact.show()
            dyf_student_contact = Join.apply(dyf_student_contact, dyf_learning_object_class, 'level_list_master',
                                             'class_name')
            # print ('Show 3')
            # dyf_student_contact.show()
            dyf_student_contact = dyf_student_contact.rename_field('class_id', 'level_study_id')
            # print ('Show 4')
            # dyf_student_contact.show()
            # dyf_student_contact.show()
            # JOIN
            df_student_contact = dyf_student_contact.toDF()

            df_student_contact = df_student_contact.withColumn('modified_date_id', from_unixtime(df_student_contact['time_lms_created'], 'yyyyMMdd')) \
                .withColumn('modified_time', df_student_contact.time_lms_created) \
                .withColumn('created_date_id', from_unixtime(df_student_contact['time_lms_created'], 'yyyyMMdd'))

            # .withColumn('date_id', from_unixtime(df_student_contact.time_lms_created, "yyyyMMdd"))
            df_student_contact = df_student_contact.select('student_id', 'level_study_id', 'display_order',
                                                           'modified_date_id', 'modified_time')

            # Join lay nhung trinh do nho hon trinh do hien tai
            df_student_contact = df_student_contact.join(df_learning_object_class_top,
                                                         df_student_contact.display_order >= df_learning_object_class_top.display_order_top)

            df_student_contact = df_student_contact.select('student_id', 'class_id_top',
                                                           'modified_date_id', 'modified_time') \
                .withColumnRenamed("class_id_top", "level_study_id")
            df_student_contact.cache()
            # df_student_contact = df_student_contact.drop_duplicates()
            # df_student_contact.printSchema()
            # df_student_contact.show(10)
            # print('df_student_contact count:', df_student_contact.count())

            df_mapping_lo_class = dyf_mapping_lo_class.toDF()
            df_mapping_lo_class = df_mapping_lo_class.where(
                'knowledge > 0 or comprehension > 0 or application > 0 or analysis > 0 or synthesis > 0 or evaluation > 0')


            # df_mapping_lo_class.cache()
            #
            # df_mapping_lo_class.printSchema()
            # df_mapping_lo_class.count()
            # df_mapping_lo_class.show()
            #

            ###################################################################
            ###################################################################

            df_student_lo_init = df_student_contact.join(df_mapping_lo_class, df_student_contact.level_study_id
                                                         == df_mapping_lo_class.class_id)
            score_pass = 50
            df_student_lo_init = df_student_lo_init.withColumn("knowledge_1",
                                                                 df_mapping_lo_class.knowledge * f.lit(score_pass)) \
                .withColumn("comprehension_1", df_mapping_lo_class.comprehension * f.lit(score_pass)) \
                .withColumn("application_1", df_mapping_lo_class.application * f.lit(score_pass)) \
                .withColumn("analysis_1", df_mapping_lo_class.analysis * f.lit(score_pass)) \
                .withColumn("synthesis_1", df_mapping_lo_class.synthesis * f.lit(score_pass)) \
                .withColumn("evaluation_1", df_mapping_lo_class.evaluation * f.lit(score_pass)) \
                .withColumn("knowledge_pass_date_id", add_pass_date(df_mapping_lo_class.knowledge, df_student_lo_init.modified_date_id)) \
                .withColumn("comprehension_pass_date_id",
                            add_pass_date(df_mapping_lo_class.comprehension, df_student_lo_init.modified_date_id)) \
                .withColumn("application_pass_date_id", add_pass_date(df_mapping_lo_class.application, df_student_lo_init.modified_date_id)) \
                .withColumn("analysis_pass_date_id", add_pass_date(df_mapping_lo_class.analysis, df_student_lo_init.modified_date_id)) \
                .withColumn("synthesis_pass_date_id", add_pass_date(df_mapping_lo_class.synthesis, df_student_lo_init.modified_date_id)) \
                .withColumn("evaluation_pass_date_id", add_pass_date(df_mapping_lo_class.evaluation, df_student_lo_init.modified_date_id))

            df_student_lo_init = df_student_lo_init.groupby('student_id', 'learning_object_id', 'modified_date_id').agg(
                f.max('knowledge_1').alias("max_knowledge_1"),
                f.max('comprehension_1').alias("max_comprehension_1"),
                f.max('application_1').alias("max_application_1"),
                f.max('analysis_1').alias("max_analysis_1"),
                f.max('synthesis_1').alias("max_synthesis_1"),
                f.max('evaluation_1').alias("max_evaluation_1"),
                f.max('knowledge_pass_date_id').alias("max_knowledge_pass_date_id"),
                f.max('comprehension_pass_date_id').alias("max_comprehension_pass_date_id"),
                f.max('application_pass_date_id').alias("max_application_pass_date_id"),
                f.max('analysis_pass_date_id').alias("max_analysis_pass_date_id"),
                f.max('synthesis_pass_date_id').alias("max_synthesis_pass_date_id"),
                f.max('evaluation_pass_date_id').alias("max_evaluation_pass_date_id")
            )
            df_student_lo_init = df_student_lo_init.drop_duplicates()
            # #
            # # df_student_lo_init.printSchema()
            # # df_student_lo_init.count()
            # # df_student_lo_init.show()
            dyf_student_lo_init = DynamicFrame.fromDF(df_student_lo_init, glueContext,
                                                      "dyf_student_lo_init")
            applymapping = ApplyMapping.apply(frame=dyf_student_lo_init,
                                              mappings=[("student_id", "string", "student_id", "long"),
                                                        ("learning_object_id", "long", "learning_object_id", "long"),
                                                        ("modified_date_id", "string", "modified_date_id", "long"),
                                                        ("created_date_id", "string", "created_date_id", "long"),
                                                        ("max_knowledge_1", 'int', 'knowledge', 'long'),
                                                        ("max_comprehension_1", 'int', 'comprehension', 'long'),
                                                        ("max_application_1", 'int', 'application', 'long'),
                                                        ("max_analysis_1", 'int', 'analysis', 'long'),
                                                        ("max_synthesis_1", 'int', 'synthesis', 'long'),
                                                        ("max_evaluation_1", 'int', 'evaluation', 'long'),
                                                        ("max_knowledge_pass_date_id", 'string',
                                                         'knowledge_pass_date_id',
                                                         'long'),
                                                        ("max_comprehension_pass_date_id", 'string',
                                                         'comprehension_pass_date_id',
                                                         'long'),
                                                        ("max_application_pass_date_id", 'string',
                                                         'application_pass_date_id',
                                                         'long'),
                                                        ("max_analysis_pass_date_id", 'string', 'analysis_pass_date_id',
                                                         'long'),
                                                        ("max_synthesis_pass_date_id", 'string',
                                                         'synthesis_pass_date_id',
                                                         'long'),
                                                        ("max_evaluation_pass_date_id", 'string',
                                                         'evaluation_pass_date_id',
                                                         'long'),
                                                        ("user_id", 'string', 'user_id', 'long')])
            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dyf_student_lo_init = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dyf_student_lo_init")
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_lo_init,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp1/dyf_student_lo_init",
                                                                       transformation_ctx="datasink5")

            # Luu data vao S3
            datasink2 = glueContext.write_dynamic_frame.from_options(frame=dyf_student_lo_init, connection_type="s3",
                                                                     connection_options={
                                                                         "path": "s3://dts-odin/nvn_knowledge/mapping_lo_student/"},
                                                                     format="parquet",
                                                                     transformation_ctx="datasink2")

            # dyf_mapping_lo_class_bloom = DynamicFrame.fromDF(df_student_lo_init, glueContext,
            #                                                  "dyf_mapping_lo_class_bloom")
            # dyf_mapping_lo_class_bloom.printSchema()
            # print('COUNT:', dyf_student_lo_init.count())
            # dyf_student_lo_init.printSchema()
            # dyf_student_lo_init.show()

            # applymapping = ApplyMapping.apply(frame=dyf_mapping_lo_class_bloom,
            #                                   mappings=[("student_id", "string", "student_id", "long"),
            #                                             ("learning_object_id", "long", "learning_object_id", "long"),
            #                                             ("date_id", "string", "date_id", "long"),
            #                                             ("knowledge", 'int', 'knowledge', 'long'),
            #                                             ("comprehension", 'int', 'comprehension', 'long'),
            #                                             ("application", 'int', 'application', 'long'),
            #                                             ("analysis", 'int', 'analysis', 'long'),
            #                                             ("synthesis", 'int', 'synthesis', 'long'),
            #                                             ("evaluation", 'int', 'evaluation', 'long')])
            # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
            #                                     transformation_ctx="resolvechoice2")
            # dyf_student_lo_init = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dyf_student_lo_init")
            # datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_lo_init,
            #                                                            catalog_connection="glue_redshift",
            #                                                            connection_options={
            #                                                                "dbtable": "mapping_lo_student_bloom",
            #                                                                "database": "dts_odin"
            #                                                            },
            #                                                            redshift_tmp_dir="s3n://dts-odin/temp1/dyf_student_lo_init",
            #                                                            transformation_ctx="datasink5")
            #
            # dyf_student_contact_level = Join.apply(dyf_student_contact, dyf_student_level, 'contact_id', 'contact_id1')

            # JOIN lay diem khoi tao
            # dyf_student_contact_level ; mapping_lo_class
            # dyf_mapping_lo_init = Join.apply(dyf_student_contact_level, dyf_mapping_lo_class, 'level_study_id', 'class_id')
            # print('COUNT dyf_mapping_lo_init:', dyf_mapping_lo_init.count())
            # dyf_mapping_lo_init.printSchema()
            # dyf_mapping_lo_init.show()
            #
            # applymapping = ApplyMapping.apply(frame=dyf_mapping_lo_init,
            #                                   mappings=[("student_id", "string", "student_id", "string"),
            #                                             ("learning_object_id", "long", "learning_object_id", "long"),
            #                                             ("level_study", "string", "level_study", "string"),
            #                                             ("level_study_id", 'string', 'level_study_id', 'long')])
            # resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
            #                                     transformation_ctx="resolvechoice2")
            # dyf_student_contact_level = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields")


        except Exception as e:
            print("###################### Exception ##########################")
            print(e)

        # ghi flag
        # lay max key trong data source
        datasourceTmp = dyf_student_contact.toDF()
        flag = datasourceTmp.agg({"time_lms_created": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        # df.write.parquet("s3a://dts-odin/flag/flag_log_time_lms_created.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
