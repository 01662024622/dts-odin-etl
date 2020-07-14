import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice, DropNullFields, RenameField, Join
from datetime import date

# sparkcontext
sc = SparkContext()
# glue with sparkcontext env
glueContext = GlueContext(sc)
# create new spark session working
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

today = date.today()
d4 = today.strftime("%Y-%m-%d").replace("-", "")


def main():
    x = spark.createDataFrame([(1, 'Thang'),
                               (2, 'Name')], ['name', 'age']).collect()
    x.show(3)


if __name__ == "__main__":
    main()
