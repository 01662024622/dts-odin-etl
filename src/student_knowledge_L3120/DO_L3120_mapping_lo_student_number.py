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
from datetime import datetime, timedelta, date


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    class_topica_id = 1

    now = datetime.now()  # current date and time
    year = now.strftime("%Y%m%d")
    year = '20190901'
    print("year:", int(year))
    cur_date = int(year)
    pre_date = cur_date - 1
    print("year:", pre_date)
    ########## dyf_mapping_lo_student
    dyf_mapping_lo_student = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_student"
    )

    # try:
    #     # # doc moc flag tu s3
    #     df_flag = spark.read.parquet("s3://dts-odin/flag/flag_mapping_lo_student.parquet")
    #     start_read = df_flag.collect()[0]['flag']
    #     print('read from index: ', start_read)
    #     # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    #     # dyf_student_contact = Filter.apply(frame=dyf_student_contact, f=lambda x: x['time_lms_created'] > start_read)
    # except:
    #     print('read flag file error ')
    # dyf_mapping_lo_student = Filter.apply(frame=dyf_mapping_lo_student, f=lambda x: x['knowledge_pass_date_id'] >= f.lit(int(year)))

    print('df_student_contact count 1:', dyf_mapping_lo_student.count())
    if dyf_mapping_lo_student.count() > 0:
        try:
            print("START......................")
            ########## dyf_mapping_lo_student
            dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
                database="nvn_knowledge",
                table_name="learning_object"
            )

            ########## dyf_learning_object_class
            dyf_learning_object_class = glueContext.create_dynamic_frame.from_catalog(
                database="nvn_knowledge",
                table_name="learning_object_class"
            )
            dyf_learning_object_class = dyf_learning_object_class.select_fields(['class_id', 'class_parent_id'])
            dyf_learning_object_class = Filter.apply(frame=dyf_learning_object_class, f=lambda x: x["class_parent_id"] == class_topica_id)

            ########## dyf_mapping_lo_class
            dyf_mapping_lo_class = glueContext.create_dynamic_frame.from_catalog(
                database="nvn_knowledge",
                table_name="mapping_lo_class"
            )
            dyf_mapping_lo_class = dyf_mapping_lo_class.select_fields(['class_id', 'learning_object_id'])\
                .rename_field('class_id', 'map_class_id').rename_field('learning_object_id', 'map_lo_id')
            ## JOIN chi lay nhung trinh do cua TOPICA
            dyf_mapping_lo_class = Join.apply(dyf_mapping_lo_class, dyf_learning_object_class, 'map_class_id', 'class_id')


            dyf_learning_object = dyf_learning_object.select_fields(
                ['learning_object_id', 'learning_object_type']).rename_field('learning_object_id', 'lo_id')
            dyf_mapping_lo_student = Join.apply(dyf_mapping_lo_student, dyf_learning_object, 'learning_object_id',
                                                'lo_id')
            dyf_mapping_lo_student = Join.apply(dyf_mapping_lo_student, dyf_mapping_lo_class, 'learning_object_id',
                                                'map_lo_id')

            # dyf_mapping_lo_student.printSchema()
            # dyf_mapping_lo_student.show()

            df_mapping_lo_student = dyf_mapping_lo_student.toDF()

            df_mapping_lo_student = df_mapping_lo_student.groupby('student_id', 'learning_object_type', 'class_id').agg(
                f.count('knowledge_pass_date_id').alias("knowledge_number"),
                f.count('comprehension_pass_date_id').alias("comprehension_number"),
                f.count('application_pass_date_id').alias("application_number"),
                f.count('analysis_pass_date_id').alias("analysis_number"),
                f.count('synthesis_pass_date_id').alias("synthesis_number"),
                f.count('evaluation_pass_date_id').alias("evaluation_number"))
            df_mapping_lo_student = df_mapping_lo_student.withColumn("created_date_id", f.lit(str(year)))
            # print('Count:' , df_mapping_lo_student.count())
            # df_mapping_lo_student.printSchema()
            # df_mapping_lo_student.show(5)

            dyf_mapping_lo_student = DynamicFrame.fromDF(df_mapping_lo_student, glueContext,
                                                         "dyf_mapping_lo_student")
            applymapping = ApplyMapping.apply(frame=dyf_mapping_lo_student,
                                              mappings=[("student_id", "long", "student_id", "long"),
                                                        ("user_id", "long", "user_id", "long"),
                                                        ("class_id", "long", "class_id", "long"),
                                                        ("knowledge_number", "long", "knowledge_number", "long"),
                                                        (
                                                        "comprehension_number", 'long', 'comprehension_number', 'long'),
                                                        ("application_number", 'long', 'application_number', 'long'),
                                                        ("analysis_number", 'long', 'analysis_number', 'long'),
                                                        ("synthesis_number", 'long', 'synthesis_number', 'long'),
                                                        ("evaluation_number", 'long', 'evaluation_number', 'long'),
                                                        ("created_date_id", 'string', 'created_date_id', 'long'),
                                                        ("learning_object_type", 'string', 'learning_object_type',
                                                         'string')])
            resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                                transformation_ctx="resolvechoice2")
            dyf_student_lo_init = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dyf_student_lo_init")
            datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_student_lo_init,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "mapping_lo_student_number",
                                                                           "database": "dts_odin"
                                                                       },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp1/dyf_student_lo_number",
                                                                       transformation_ctx="datasink5")

            print("END......................")
        except Exception as e:
            print("###################### Exception ##########################")
            print(e)

        # ghi flag
        # lay max key trong data source
        # datasourceTmp = dyf_student_contact.toDF()
        # flag = datasourceTmp.agg({"time_lms_created": "max"}).collect()[0][0]
        #
        # flag_data = [flag]
        # df = spark.createDataFrame(flag_data, "long").toDF('flag')
        #
        # # ghi de _key vao s3
        # df.write.parquet("s3a://dts-odin/flag/flag_mapping_lo_student.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
