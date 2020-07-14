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

    ########## dyf_learning_object
    dyf_learning_object = glueContext.create_dynamic_frame.from_catalog(
                                database="nvn_knowledge",
                                table_name="nvn_knowledge_learning_object"
                            )
    dyf_learning_object = dyf_learning_object.select_fields(
        ['_key', 'learning_object_id', 'learning_object_name', 'phone_tic'])
    # convert kieu du lieu
    dyf_learning_object = dyf_learning_object.resolveChoice(specs=[('_key', 'cast:long')])


    # Doc ra max key moi nhat, va chi load du lieuj lon hon key nay (tranh viec load full du lieu do cac ban ghi truoc da thuc hien chay etl roi)
    df_flag = spark.read.parquet("s3://dts-odin/flag/flag_LO.parquet")
    max_key = df_flag.collect()[0]['flag']
    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    dyf_learning_object = Filter.apply(frame=dyf_learning_object, f=lambda x: x["_key"] > max_key)
    # Show schema
    dyf_learning_object.printSchema()
    # Show data cua dynamicframe
    dyf_learning_object.show()
    # Check neu co ban ghi thi thuc hien tiep
    if (dyf_learning_object.count() > 0):
        apply_mapping_learning_object = ApplyMapping.apply(frame=dyf_learning_object,
                                          mappings=[("learning_object_id", "int", "learning_object_id", "int"),
                                                    ("learning_object_name", "string", "learning_object_name", "string"),
                                                    ("phone_tic", "string", "phone_tic", "string")])
        resolve_choice_learning_object = ResolveChoice.apply(frame=apply_mapping_learning_object, choice="make_cols",
                                                transformation_ctx="resolve_choice_learning_object")
        dropnullfields_learning_object = DropNullFields.apply(frame=resolve_choice_learning_object, transformation_ctx="dropnullfields_learning_object")
        #
        # save_learning_object = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields_learning_object,
        #                                                            catalog_connection="glue_redshift",
        #                                                            connection_options={
        #                                                                "dbtable": "learning_object",
        #                                                                "database": "dts_odin"
        #                                                                },
        #                                                            redshift_tmp_dir="s3n://dts-odin/temp/tig_advisor/user_profile_student_contact/",
        #                                                            transformation_ctx="datasink4")

        # lay max key trong data source
        datasource = dyf_learning_object.toDF()
        flag = datasource.agg({"_key": "max"}).collect()[0][0]

        # convert kieu dl
        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/flag_LO.parquet", mode="overwrite")
if __name__ == "__main__":
    main()
