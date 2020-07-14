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

    mdl_tpe_enduser_used_product_history = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                                         table_name="tpe_enduser_used_product_history")
    # xu ly truong hop start_read is null
    try:
        # # doc moc flag tu s3
        df_flag = spark.read.parquet("s3a://dts-odin/flag/fact_flag_expired.parquet")
        start_read = df_flag.collect()[0]['flag']
        print('read from index: ', start_read)

        # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
        mdl_tpe_enduser_used_product_history = Filter.apply(
            frame=mdl_tpe_enduser_used_product_history,
            f=lambda x: x['_key'] > start_read)
    except:
        print('read flag file error ')

    print('the number of new contacts: ', mdl_tpe_enduser_used_product_history.count())
    mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.select_fields(
        ['_key', 'id', 'used_product_id', 'contact_id', 'status_new', 'status_old', 'timecreated'])

    mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.resolveChoice(
        specs=[('_key', 'cast:long')])

    # df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_LS_A3.parquet")
    #
    # max_key = df_flag.collect()[0]['flag']
    #
    # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
    #                                                     f=lambda x: x["_key"] > max_key)

    if (mdl_tpe_enduser_used_product_history.count() > 0):
        mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
                                                            f=lambda x: x["timecreated"] is not None
                                                                        and x["contact_id"] is not None
                                                                        and x["used_product_id"] is not None
                                                                        and x["status_old"] == "ACTIVED"
                                                                        and x["status_new"] in ["EXPIRED", "EXPRIED"])

        # print(mdl_tpe_enduser_used_product_history.count())

        mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.resolveChoice(
            specs=[('timecreated', 'cast:long')])
        df_mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.toDF()

        df_mdl_tpe_enduser_used_product_history = df_mdl_tpe_enduser_used_product_history.withColumn(
            'change_status_date_id', from_unixtime(df_mdl_tpe_enduser_used_product_history['timecreated'], "yyyyMMdd")) \
            .withColumn('to_status_id', f.lit(108)) \
            .withColumn('timestamp1', df_mdl_tpe_enduser_used_product_history['timecreated'] * f.lit(
            1000))

        # df_mdl_tpe_enduser_used_product_history = df_mdl_tpe_enduser_used_product_history.select('used_product_id',
        #                                                                                    'contact_id',
        #                                                                                    'ngay_kich_hoat',
        #                                                                                    'id').withColumnRenamed(
        #     'used_product_id', 'id_product_buy')
        data_mdl_tpe_enduser_used_product_history = DynamicFrame.fromDF(df_mdl_tpe_enduser_used_product_history,
                                                                        glueContext,
                                                                        "data_mdl_tpe_enduser_used_product_history")

        data_mdl_tpe_enduser_used_product_history.printSchema()
        data_mdl_tpe_enduser_used_product_history.show(3)

        applymapping1 = ApplyMapping.apply(frame=data_mdl_tpe_enduser_used_product_history,
                                           mappings=[("contact_id", "string", "contact_id", "string"),
                                                     ("change_status_date_id", "string", "change_status_date_id",
                                                      "long"),
                                                     ("timestamp1", "long", "timestamp1", "timestamp"),
                                                     ('to_status_id', 'int', 'to_status_id', 'long')])

        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")

        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={
                                                                       "dbtable": "mapping_changed_status_student",
                                                                       "database": "dts_odin",
                                                                       "postactions": """UPDATE mapping_changed_status_student
		                                                                                 SET user_id = ( SELECT user_id FROM user_map WHERE source_type = 1 AND source_id = mapping_changed_status_student.contact_id LIMIT 1 )
	                                                                                     WHERE user_id IS NULL and to_status_id=108"""
                                                                   },
                                                                   redshift_tmp_dir="s3n://datashine-dwh/temp1/",
                                                                   transformation_ctx="datasink4")
        # ghi flag
        # lay max key trong data source
        datasourceTmp = mdl_tpe_enduser_used_product_history.toDF()
        flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://dts-odin/flag/fact_flag_expired.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
