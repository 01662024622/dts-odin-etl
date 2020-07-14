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

    # dyf_learning_object = glueContext.create_dynamic_frame.from_options(
    #     connection_type="redshift",
    #     connection_options={
    #             "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
    #             "user": "datashine",
    #             "password": "TOXnative75dataredshift",
    #             "dbtable": "learning_object",
    #             "redshiftTmpDir": "s3n://dts-odin/temp1/dyf_learning_object"}
    # )
    # #
    # resolvechoice = ResolveChoice.apply(frame=dyf_learning_object, choice="make_cols", transformation_ctx="resolvechoice2")
    #
    # dropnullfields = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")
    # print('count', dropnullfields.count())
    # dropnullfields.printSchema()
    # dropnullfields.show()
    # datasink5 = glueContext.write_dynamic_frame.from_options(frame=dropnullfields, connection_type="s3",
    #                                                          connection_options={
    #                                                              "path": "s3://dts-odin/nvn_knowledge/learning_object/"},
    #                                                          format="parquet", transformation_ctx="datasink6")

    # dyf_mapping_class_lms = glueContext.create_dynamic_frame.from_options(
    #     connection_type="redshift",
    #     connection_options={
    #             "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
    #             "user": "dtsodin",
    #             "password": "DWHDtsodin@123",
    #             "dbtable": "mapping_class_lms",
    #             "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_class_lms"}
    # )
    # #
    # dyf_mapping_class_lms = ResolveChoice.apply(frame=dyf_mapping_class_lms, choice="make_cols", transformation_ctx="resolvechoice2")
    # datasink5 = glueContext.write_dynamic_frame.from_options(frame=dyf_mapping_class_lms, connection_type="s3",
    #                                                          connection_options={
    #                                                              "path": "s3://dts-odin/nvn_knowledge/mapping_class_lms/"},
    #                                                          format="parquet", transformation_ctx="datasink6")

    # dyf_learning_object_class = glueContext.create_dynamic_frame.from_options(
    #     connection_type="redshift",
    #     connection_options={
    #         "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
    #         "user": "dtsodin",
    #         "password": "DWHDtsodin@123",
    #         "dbtable": "learning_object_class",
    #         "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_class_lms"}
    # )
    # #
    # dyf_learning_object_class = ResolveChoice.apply(frame=dyf_learning_object_class, choice="make_cols",
    #                                             transformation_ctx="resolvechoice2")
    # dyf_learning_object_class.printSchema()
    # dyf_learning_object_class.show()
    # datasink5 = glueContext.write_dynamic_frame.from_options(frame=dyf_learning_object_class, connection_type="s3",
    #                                                          connection_options={
    #                                                              "path": "s3://dts-odin/nvn_knowledge/learning_object_class/"},
    #                                                          format="parquet", transformation_ctx="datasink6")

    dyf_mapping_lo_class = glueContext.create_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options={
            "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
            "user": "dtsodin",
            "password": "DWHDtsodin@123",
            "dbtable": "mapping_lo_class",
            "redshiftTmpDir": "s3n://dts-odin/temp1/mapping_class_lms"}
    )
    #
    dyf_mapping_lo_class = ResolveChoice.apply(frame=dyf_mapping_lo_class, choice="make_cols",
                                                    transformation_ctx="resolvechoice2")
    # dyf_learning_object_class.printSchema()
    # dyf_learning_object_class.show()
    datasink5 = glueContext.write_dynamic_frame.from_options(frame=dyf_mapping_lo_class, connection_type="s3",
                                                             connection_options={
                                                                 "path": "s3://dts-odin/nvn_knowledge/mapping_lo_class/"},
                                                             format="parquet", transformation_ctx="datasink6")


if __name__ == "__main__":
    main()
