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

    ########## top_quiz_attempts
    dyf_phonemic = glueContext.create_dynamic_frame.from_catalog(
                                database="nvn_knowledge",
                                table_name="phonemic"
                            )
    dyf_phonemic = dyf_phonemic.select_fields(
        ['_key', 'id', 'timestart', 'quiz'])
    # dyf_top_quiz_attempts = dyf_top_quiz_attempts.resolveChoice(specs=[('_key', 'cast:long')])


if __name__ == "__main__":
    main()
