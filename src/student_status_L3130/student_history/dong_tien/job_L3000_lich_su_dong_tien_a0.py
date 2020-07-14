import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import from_unixtime, unix_timestamp, date_format


def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    datasource = glueContext.create_dynamic_frame.from_catalog(database="tig_market",
                                                         table_name="tpe_enduser_used_product_history")

    datasource = datasource.resolveChoice(specs=[('_key', 'cast:long')])
    # doc moc flag tu s3
    try:
        df_flag = spark.read.parquet("s3a://dts-odin/flag/student_status/lich_su_dong_tien.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        datasource = Filter.apply(frame=datasource,
                                      f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')
    print('the number of new contacts: ', datasource.count())

    # Chon cac truong can thiet
    datasource = datasource.select_fields(['_key', 'used_product_id', 'contact_id', 'status_new', 'status_old', 'timecreated'])

    if (datasource.count() > 0):
        datasource_df = datasource.toDF()
        datasource_df = datasource_df.where("timecreated IS NOT NULL")
        datasource_df = datasource_df.where("contact_id IS NOT NULL and used_product_id IS NOT NULL")
        datasource_df = datasource_df.where("status_new IS NOT NULL and status_new <> ''")
        datasource_buy = datasource_df.where("(status_old is null and status_new = 'DEACTIVED')")
        datasource_buy = datasource_buy.withColumn('id_time', from_unixtime(datasource_buy.timecreated, "yyyyMMdd"))
        datasource_buy = datasource_buy.withColumn("ngay_mua", from_unixtime(datasource_df.timecreated))
        datasource_buy = datasource_buy.select('used_product_id', 'contact_id', 'ngay_mua', 'id_time') \
          .withColumnRenamed('used_product_id', 'id_product_buy')

        data = DynamicFrame.fromDF(datasource_buy, glueContext, "data")

        # print('datasource_buy:;datasource_buy::schime')
        # data.printSchema()
        # print('the number of data: ', data.count())
        # print('Show data')
        # data.show(10)

        # data = data.select_fields(['id_dim_hocvien', 'ngay_mua'])
        df_active = data.toDF()
        # df_active = df_active.dropDuplicates(['id_lichsumuagoi'])
        data_active = DynamicFrame.fromDF(df_active, glueContext, "data_active")

        applymapping1 = ApplyMapping.apply(frame=data_active,
                                         mappings=[("id_time", "string", "change_status_date_id", "long"),
                                                   ("contact_id", "string", "contact_id", "string"),
                                                   ("id_product_buy", "string", "measure1", "long"),
                                                   ("ngay_mua", "string", "timestamp1", "timestamp")])
        resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                           transformation_ctx="resolvechoice2")
        dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")

        # datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
        #                                                          catalog_connection="glue_redshift",
        #                                                          connection_options={
        #                                                              "dbtable": "temp_lich_su_trang_thai_a0",
        #                                                              "database": "dts_odin",
        #                                                                          "postactions": """INSERT INTO mapping_changed_status_student(user_id, change_status_date_id, measure1, timestamp1, description, to_status_id)
        #                                                                              SELECT up.user_id, temp_a0.change_status_date_id, temp_a0.measure1, temp_a0.timestamp1,
        #                                                                              'measure1: id_product_buy, timestamp1: ngay_mua', 1
        #                                                                              FROM temp_lich_su_trang_thai_a0 temp_a0
        #                                                                              LEFT JOIN user_map up
        #                                                                              ON up.source_type = 1
        #                                                                              AND up.user_id is not null
        #                                                                              AND temp_a0.contact_id = up.source_id
        #                                                                              WHERE up.user_id is not null;
        #                                                                              DROP TABLE IF EXISTS public.temp_lich_su_trang_thai_a0"""
        #                                                                           },
        #                                                          redshift_tmp_dir="s3n://dts-odin/temp/lich_su_dong_tien/",
        #                                                          transformation_ctx="datasink4")
        # ghi flag
        # lay max key trong data source
        # df_datasource = datasource.toDF()
        # flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
        # flag_data = [flag]
        # df = spark.createDataFrame(flag_data, "long").toDF('flag')
        # df.write.parquet("s3a://dts-odin/flag/student_status/lich_su_dong_tien.parquet", mode="overwrite")


if __name__ == "__main__":
    main()