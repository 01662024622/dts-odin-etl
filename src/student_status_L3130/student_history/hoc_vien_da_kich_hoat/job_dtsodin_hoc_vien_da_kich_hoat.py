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

    tpe_enduser_used_product_history = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                                     table_name="tpe_enduser_used_product_history")
    tpe_enduser_used_product_history = tpe_enduser_used_product_history.select_fields(
        ['_key', 'id', 'used_product_id', 'contact_id', 'status_new', 'status_old', 'timecreated'])

    tpe_enduser_used_product_history = tpe_enduser_used_product_history.resolveChoice(
        specs=[('_key', 'cast:long')])

    df_flag = spark.read.parquet("s3a://datashine-dev-redshift-backup/flag/flag_hvdkh_LS_A3.parquet")

    max_key = df_flag.collect()[0]['flag']

    tpe_enduser_used_product_history = Filter.apply(frame=tpe_enduser_used_product_history,
                                                        f=lambda x: x["_key"] > max_key)
    print(tpe_enduser_used_product_history.count())
    if (tpe_enduser_used_product_history.count() > 0):
        tpe_enduser_used_product_history = Filter.apply(
            frame=tpe_enduser_used_product_history,
            f=lambda x: x["timecreated"] is not None
                        and x["contact_id"] is not None
                        and x["used_product_id"] is not None
                        and x["status_new"] is not None
                        and x["status_new"] == 'ACTIVED'
                        and (x["status_old"] == 'SUSPENDED'
                             or x["status_old"] == 'EXPIRED'
                             or x["status_old"] == 'EXPRIED'))

        if (tpe_enduser_used_product_history.count() > 0):
            try:
                tpe_enduser_used_product_history = tpe_enduser_used_product_history.resolveChoice(
                    specs=[('timecreated', 'cast:long')])
                df_tpe_enduser_used_product_history = tpe_enduser_used_product_history.toDF()

                df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.withColumn(
                    'ngay_kich_hoat', from_unixtime(df_tpe_enduser_used_product_history['timecreated'], "yyyyMMdd"))

                df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.withColumn(
                    'timestemp', df_tpe_enduser_used_product_history['timecreated'] * f.lit(1000))

                df_tpe_enduser_used_product_history = df_tpe_enduser_used_product_history.withColumn(
                    'to_status_id', f.lit(107))

                data_tpe_enduser_used_product_history = DynamicFrame.fromDF(
                    df_tpe_enduser_used_product_history, glueContext, "data_tpe_enduser_used_product_history")

                data_tpe_enduser_used_product_history = data_tpe_enduser_used_product_history.resolveChoice(
                    specs=[('timestemp', 'cast:long')])

                data_tpe_enduser_used_product_history.printSchema()
                data_tpe_enduser_used_product_history.show(3)

                applymapping1 = ApplyMapping.apply(
                    frame=data_tpe_enduser_used_product_history,
                    mappings=[("ngay_kich_hoat", "string", "change_status_date_id", "long"),
                              ("to_status_id", "int", "to_status_id", "long"),
                              ("timestemp", "long", "timestamp1", "timestamp"),
                              ("contact_id", "string", "contact_id1", "string")])

                applymapping1.printSchema()
                applymapping1.show(20)

                resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                     transformation_ctx="resolvechoice2")

                dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

                datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dropnullfields3,
                    catalog_connection="glue_redshift",
                    connection_options={
                        "dbtable": "mapping_changed_status_student",
                        "database": "dts_odin",
                        "postactions": """UPDATE mapping_changed_status_student 
                                            SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 1 AND source_id = mapping_changed_status_student.contact_id1 LIMIT 1 ) 
                                            WHERE user_id IS NULL AND to_status_id = 107"""},
                    redshift_tmp_dir="s3n://datashine-dwh/temp1/",
                    transformation_ctx="datasink4")

                # ghi flag
                # lay max key trong data source
                datasourceTmp = tpe_enduser_used_product_history.toDF()
                flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

                flag_data = [flag]
                df = spark.createDataFrame(flag_data, "long").toDF('flag')

                # ghi de _key vao s3
                df.write.parquet("s3a://datashine-dev-redshift-backup/flag/flag_hvdkh_LS_A3.parquet",
                                 mode="overwrite")
            except Exception as e:
                print("No new data")
                print(e)

if __name__ == "__main__":
    main()
