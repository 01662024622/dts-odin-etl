

from pyspark.sql import Row, SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import udf,first
from pyspark.sql.types import StringType


def main():
    spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
    # sc = SparkContext()
    l = [(None, 1),('Aliceaa', 3),('Alices',None),('Alicesssss', 1),('Alices', 3)]
    x=spark.createDataFrame(l,['name', 'age'])

    def myFunc(data_list):
        for val in data_list:
            if val is not None and val !='':
                return val
        return None

    myUdf = udf(myFunc, StringType())

    x=x.groupBy('age')\
        .agg(first('name').alias('name'))
    # dropping duplicates from the dataframe
    x.dropDuplicates().show()

def xxx():
    spark = SparkSession.builder.master("local").appName("Word Count").config("spark.some.config.option", "some-value").getOrCreate()
    # sc = SparkContext()
    l = [('Alicesssss', 1,3),('Alice', 3,4),('Alices',None,3),('Alicesssss', 1,2),('Alices', 3,3)]
    x=spark.createDataFrame(l,['name', 'age','test'])

    # dropping duplicates from the dataframe
    x.dropDuplicates(['name','test']).show()
main()