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

    # date_now = datetime.now()
    # preday = date_now + timedelta(days=-1)
    # d1 = preday.strftime("%Y%m%d")
    # print("d1 =", d1)
    #
    # now = datetime.now()  # current date and time
    # year = now.strftime("%Y%m%d")
    # print("year:", year)

    dyf_mapping_lo_student_history = glueContext.create_dynamic_frame.from_catalog(
        database="nvn_knowledge",
        table_name="mapping_lo_student_history"
    )
    print('Count:', dyf_mapping_lo_student_history.count())
    # # Filter nhung ban ghi cua ngay hom truoc, filter nhung ban ghi co diem != 0
    # dyf_mapping_lo_student_history = Filter.apply(frame=dyf_mapping_lo_student_history, f=lambda x: x['date_id'] is not None)
    dyf_mapping_lo_student_history = Filter.apply(frame=dyf_mapping_lo_student_history,
                                                  f=lambda x: x['date_id'] is not None and
                                                              (x['knowledge'] != 0 or x['comprehension'] != 0 or x[
                                                                  'application'] != 0 or x['analysis'] != 0 or x[
                                                                   'synthesis'] != 0 or x['evaluation'] != 0))
    if dyf_mapping_lo_student_history.count() > 0:
        print('START JOB---------------')
        df_mapping_lo_student_history = dyf_mapping_lo_student_history.toDF()
        df_mapping_lo_student_history = df_mapping_lo_student_history.groupby('date_id', 'student_id',
                                                                              'learning_object_id').agg(
            f.sum("knowledge").alias("knowledge"),
            f.sum("comprehension").alias("comprehension"), f.sum("application").alias("application"),
            f.sum("analysis").alias("analysis"), f.sum("synthesis").alias("synthesis"),
            f.sum("evaluation").alias("evaluation"))
        df_mapping_lo_student_history.printSchema()
        df_mapping_lo_student_history.show()
        print('END JOB---------------')

        dyf_mapping_lo_student_used = DynamicFrame.fromDF(df_mapping_lo_student_history, glueContext,
                                                          "dyf_student_lo_init")
        # print('COUNT:', dyf_student_lo_init.count())
        # dyf_student_lo_init.printSchema()
        # dyf_student_lo_init.show()

        dyf_mapping_lo_student_used = ApplyMapping.apply(frame=dyf_mapping_lo_student_used,
                                                         mappings=[("student_id", "long", "student_id", "long"),
                                                                   ("learning_object_id", "long", "learning_object_id",
                                                                    "long"),
                                                                   ("date_id", "int", "date_id", "long"),
                                                                   ("knowledge", 'long', 'knowledge', 'long'),
                                                                   ("comprehension", 'long', 'comprehension', 'long'),
                                                                   ("application", 'long', 'application', 'long'),
                                                                   ("analysis", 'long', 'analysis', 'long'),
                                                                   ("synthesis", 'long', 'synthesis', 'long'),
                                                                   ("evaluation", 'long', 'evaluation', 'long')])
        dyf_mapping_lo_student_used = ResolveChoice.apply(frame=dyf_mapping_lo_student_used, choice="make_cols",
                                                          transformation_ctx="resolvechoice2")
        dyf_mapping_lo_student_used = DropNullFields.apply(frame=dyf_mapping_lo_student_used,
                                                           transformation_ctx="dyf_mapping_lo_student_used")
        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_mapping_lo_student_used,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "mapping_lo_student_used",
                                                                       "database": "dts_odin",
                                                                       "postactions": """ call proc_insert_tbhv();
                                                                       INSERT INTO mapping_lo_student_history SELECT * FROM mapping_lo_student_used;
                                                                       DROP TABLE IF EXISTS mapping_lo_student_used """
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/temp1/dyf_student_lo_init",
                                                                   transformation_ctx="datasink5")


if __name__ == "__main__":
    main()
