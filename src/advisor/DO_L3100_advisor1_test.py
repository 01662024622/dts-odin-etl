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


def main():
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    datasource0 = glueContext.create_dynamic_frame.from_catalog(database="tig_advisor", table_name="advisor_account",
                                                                transformation_ctx="datasource0")

    datasource0 = datasource0.select_fields(
        ['_key', 'user_id', 'user_name', 'user_display_name', 'user_email', 'user_phone', 'ip_phone_number',
         'level', 'advisor_deleted']).rename_field('user_id', 'id').rename_field('user_name', 'ten').rename_field('advisor_deleted', 'advisor_deleted_tmp')

    # doc flag tu s3
    df_flag = spark.read.parquet("s3://dts-odin/flag/flag_CVHD.parquet")

    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    data = datasource0.toDF()
    data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    data = data.withColumn('type_eg', f.lit(None))
    data = data.withColumn('advisor_type', f.lit(None))
    data = data.withColumn('advisor_deleted', when(data.advisor_deleted_tmp, f.lit(1)).otherwise(f.lit(0)))
    data.printSchema()


    datasource0 = DynamicFrame.fromDF(data, glueContext, "datasource0")
    # datasource0.show()
    if (datasource0.count() > 0):
        try:
            # chon field mong muon
            applymapping1 = ApplyMapping.apply(frame=datasource0,
                                               mappings=[("id", "int", "id", "bigint"),
                                                         ("ten", "string", "username", "string"),
                                                         ("user_display_name", "string", "name", "string"),
                                                         ("user_email", "string", "email", "string"),
                                                         ("level", "int", "level", "int"),
                                                         ("advisor_deleted", "int", "advisor_deleted", "int"),
                                                         ("type_eg", "int", "type_eg", "string"),
                                                         ("advisor_type", "int", "advisor_type", "string")
                                                         ],
                                               transformation_ctx="applymapping1")

            resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice2")

            dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

            # ghi data vao redshift
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={
                                                                           "dbtable": "dim_advisor",
                                                                           "database": "dts_odin"},
                                                                       redshift_tmp_dir="s3n://dts-odin/backup/advisor_account/",
                                                                       transformation_ctx="datasink4")

            # lay max _key tren datasource
            datasource = datasource0.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            # tao data frame
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            # ghi de flag moi vao s3
            df.write.parquet("s3a://dts-odin/flag/flag_CVHD.parquet", mode="overwrite")
        except:  # xu ly ngoai le(khi co datasource nhung k co gia tri thoa man dieu kien sau khi loc)
            # ghi flag
            datasource = datasource0.toDF()
            flag = datasource.agg({"_key": "max"}).collect()[0][0]

            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')

            df.write.parquet("s3a://dts-odin/flag/flag_CVHD.parquet", mode="overwrite")

    # EG
    datasource = glueContext.create_dynamic_frame.from_catalog(database="dm_toa",
                                                             table_name="advisor_eg")

    # Chon cac truong can thiet
    datasource = datasource.select_fields(['_key', 'advisor_id', 'bo_phan', 'eg'])

    datasource = datasource.resolveChoice(specs=[('_key', 'cast:long')])


    data = datasource.toDF()
    # data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    # data = data.where(data['_key'] < 276961)
    datasource = DynamicFrame.fromDF(data, glueContext, "datasource")

    if (datasource.count() > 0):
        applymapping1 = ApplyMapping.apply(frame=datasource,
                                           mappings=[("advisor_id", "string", "advisor_id", "string"),
                                                     ("bo_phan", "string", "bo_phan", "string"),
                                                     ("eg", "string", "eg", "string")])
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "dim_advisor_eg",
                                                                       "database": "dts_odin",
                                                                       "postactions": """update dim_advisor set type_eg = eg, advisor_type = bo_phan
                                                                                            from dim_advisor_eg
                                                                                            where id=advisor_id;
                                                                                            DROP TABLE IF EXISTS public.dim_advisor_eg"""
                                                                   },
                                                                   redshift_tmp_dir="s3n://dts-odin/backup/advisor_account/",
                                                                   transformation_ctx="datasink4")
if __name__ == "__main__":
    main()
