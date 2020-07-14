import sys
from awsglue.transforms import *
from awsglue.transforms.apply_mapping import ApplyMapping
from awsglue.transforms.drop_nulls import DropNullFields
from awsglue.transforms.dynamicframe_filter import Filter
from awsglue.transforms.resolve_choice import ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import when
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format

import pyspark.sql.functions as f


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    mdl_ticket_log = glueContext.create_dynamic_frame.from_catalog(database="native_smile",
                                                                   table_name="ticket_log")

    mdl_ticket_log = mdl_ticket_log.select_fields(
        ['_key', 'ticket_id', 'requester_email', 'satisfaction', 'satisfaction_at', 'created_at'])

    mdl_ticket_log = mdl_ticket_log.resolveChoice(specs=[('_key', 'cast:long')])

    # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_rating_class_caresoft.parquet")
    max_key = df_flag.collect()[0]['flag']

    print("max_key: ", max_key)

    mdl_ticket_log = Filter.apply(frame=mdl_ticket_log,
                                    f=lambda x: x["_key"] > max_key)
    if (mdl_ticket_log.count() > 0):
        print(mdl_ticket_log.count())
        try:
            mdl_ticket_log = Filter.apply(frame=mdl_ticket_log,
                                          f=lambda x: x["requester_email"] != None and x["requester_email"] != '' and x['satisfaction'] in ('1','2','3','4','5'))

            mdl_ticket_log = mdl_ticket_log.resolveChoice(specs=[('satisfaction', 'cast:int')])

            mdl_student_contact = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor",
                                                                                table_name="student_contact")
            mdl_student_contact = mdl_student_contact.select_fields(['student_id', 'user_name'])

            df_mdl_ticket_log = mdl_ticket_log.toDF()

            df_mdl_student_contact = mdl_student_contact.toDF()

            df_join_ticket_student = df_mdl_ticket_log.join(df_mdl_student_contact, (
                    df_mdl_ticket_log['requester_email'] == df_mdl_student_contact['user_name']), 'inner')

            df_join_ticket_student = df_join_ticket_student.withColumn("mapping_id_time",
                                                                       from_unixtime(unix_timestamp(
                                                                           df_join_ticket_student['satisfaction_at'],
                                                                           "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))

            # So lan rating 1 2 3 4 5* cua tung hoc vien theo tung ngay
            # start

            df_join_ticket_student = df_join_ticket_student.groupby('student_id', 'mapping_id_time',
                                                                    'satisfaction').agg(
                f.count('satisfaction'))

            df_join_ticket_student.printSchema()

            df_join_ticket_student = df_join_ticket_student.withColumn('to_status_id', f.lit(506))

            dyf_join_ticket_student = DynamicFrame.fromDF(df_join_ticket_student,
                                                               glueContext, "datasource0")

            applymapping_count_rating = ApplyMapping.apply(frame=dyf_join_ticket_student,
                                                           mappings=[("student_id", "string", "student_id", "bigint"),
                                                                     ("mapping_id_time", "string",
                                                                      "change_status_date_id",
                                                                      "int"),
                                                                     ("satisfaction", "int", "points", "int"),
                                                                     ("to_status_id", "int", "to_status_id", "int"),
                                                                     ("count(satisfaction)", "long", "measure", "int")])

            resolvechoice_count_rating = ResolveChoice.apply(frame=applymapping_count_rating, choice="make_cols",
                                                             transformation_ctx="resolvechoice_count_rating")

            dropnullfields_count_rating_star = DropNullFields.apply(frame=resolvechoice_count_rating,
                                                                    transformation_ctx="dropnullfields3")

            print(dropnullfields_count_rating_star.count())
            dropnullfields_count_rating_star.toDF().show()

            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_count_rating_star,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "test_rating_star_caresoft_temp",
                                                                           "database": "dts_odin",
                                                                           "postactions": """DROP TABLE IF EXISTS temp_rating_star_caresoft;
                                                                                           CREATE TABLE temp_rating_star_caresoft AS
                                                                                           SELECT mcss.id as "id", trs.student_id, trs.change_status_date_id as "change_status_date_id", trs.to_status_id as "to_status_id", trs.points as "points",
                                                                                           COALESCE(mcss.measure2 + trs.measure, trs.measure) as "measure2_a"
                                                                                           FROM test_rating_star_caresoft_temp trs
                                                                                           LEFT JOIN mapping_changed_status_student mcss ON mcss.student_id = trs.student_id
                                                                                           AND mcss.change_status_date_id = trs.change_status_date_id
                                                                                           AND mcss.to_status_id = 506;
                                                                                           DELETE mapping_changed_status_student
                                                                                           WHERE mapping_changed_status_student.id in (SELECT id FROM temp_rating_star_caresoft WHERE id is not null);
                                                                                           INSERT INTO mapping_changed_status_student(student_id, change_status_date_id, to_status_id, measure1, measure2)
                                                                                           SELECT student_id, change_status_date_id, to_status_id, points, measure2_a
                                                                                           FROM temp_rating_star_caresoft;
                                                                                           DROP TABLE IF EXISTS temp_rating_star_caresoft, test_rating_star_caresoft_temp;"""},
                                                                       redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
                                                                       transformation_ctx="datasink4")

            # end

            # ghi flag
            # lay max key trong data source
            mdl_ticket_log = mdl_ticket_log.toDF()
            flag = mdl_ticket_log.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de _key vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_rating_class_caresoft.parquet", mode="overwrite")

        except Exception as e:
            print("No new data")
            print(e)


if __name__ == "__main__":
    main()
