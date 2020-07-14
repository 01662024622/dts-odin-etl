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


    mdl_tpe_enduser_used_product_history = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                                                         table_name="tpe_enduser_used_product_history")
    mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.select_fields(
        ['_key', 'id', 'used_product_id', 'contact_id', 'status_new', 'status_old', 'timecreated'])

    mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.resolveChoice(
        specs=[('_key', 'cast:long')])



    # df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_LS_A3.parquet")

    # so sanh _key datasource voi flag, lay nhung gia tri co key > flag
    # data = mdl_rating_class.toDF()
    # data = data.where(data['_key'] > df_flag.collect()[0]['flag'])
    # data = data.where(data['_key'] < 100)
    #
    # mdl_rating_class = DynamicFrame.fromDF(data, glueContext, "mdl_rating_class")
    # max_key = df_flag.collect()[0]['flag']

    # print("max_key: ", max_key)

    # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
    #                                                     f=lambda x: x["_key"] > max_key)

    if (mdl_tpe_enduser_used_product_history.count() > 0):
        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["_key"] > max_key)

        mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history, f=lambda x: x["timecreated"] is not None and x["contact_id"] is not None and x["used_product_id"] is not None and x["status_old"] == 'DEACTIVED' and x["status_new"] is not None)

        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["contact_id"] is not None)
        #
        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["used_product_id"] is not None)
        #
        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["status_new"] is not None)
        #
        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["status_old"] == 'DEACTIVED')
        #
        # mdl_tpe_enduser_used_product_history = Filter.apply(frame=mdl_tpe_enduser_used_product_history,
        #                                                     f=lambda x: x["status_new"] == 'ACTIVED')

        df_mdl_tpe_enduser_used_product_history = mdl_tpe_enduser_used_product_history.toDF()

        df_mdl_tpe_enduser_used_product_history = df_mdl_tpe_enduser_used_product_history.withColumn('ngay_kich_hoat',
                                                                                               from_unixtime(
                                                                                                   df_mdl_tpe_enduser_used_product_history[
                                                                                                       "timecreated"], "yyyyMMdd"))
        df_mdl_tpe_enduser_used_product_history = df_mdl_tpe_enduser_used_product_history.select('used_product_id',
                                                                                           'contact_id',
                                                                                           'ngay_kich_hoat',
                                                                                           'id').withColumnRenamed(
            'used_product_id', 'id_product_buy')
        data_mdl_tpe_enduser_used_product_history = DynamicFrame.fromDF(df_mdl_tpe_enduser_used_product_history,
                                                                        glueContext, "datasource")

        applymapping1 = ApplyMapping.apply(frame=data_mdl_tpe_enduser_used_product_history,
                                           mappings=[("contact_id", "string", "contact_id", "string"),
                                                     ("ngay_kich_hoat", "string", "ngay_kich_hoat", "timestamp"),
                                                     ("id", "string", "id", "string")])

        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                             transformation_ctx="resolvechoice2")

        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")
        print("count: ", dropnullfields3.count())
        dropnullfields3.show()
        # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
        #                                                            catalog_connection="glue_redshift",
        #                                                            connection_options={
        #                                                                "dbtable": "temp_ls_trang_thai_a3_1",
        #                                                                "database": "dts_odin",
        #                                                                "postactions": """  call tiepnv_proc_status_a3(1,'');"""},
        #                                                            redshift_tmp_dir="s3n://datashine-dwh/temp1/",
        #                                                            transformation_ctx="datasink4")
        # # ghi flag
        # lay max key trong data source
        datasourceTmp = mdl_tpe_enduser_used_product_history.toDF()
        flag = datasourceTmp.agg({"_key": "max"}).collect()[0][0]

        flag_data = [flag]
        df = spark.createDataFrame(flag_data, "long").toDF('flag')

        # ghi de _key vao s3
        df.write.parquet("s3a://datashine-dev-redshift-backup/flag/flag_LS_A3.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
