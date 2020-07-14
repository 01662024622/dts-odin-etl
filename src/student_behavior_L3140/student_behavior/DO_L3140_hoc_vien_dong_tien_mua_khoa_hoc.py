import sys
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

    khoa_hoc = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                             table_name="khoa_hoc")

    khoa_hoc.printSchema()

    khoa_hoc.show(2)

    khoa_hoc = khoa_hoc.select_fields(
        ['_key', 'makh', 'mahv', 'goi_sanpham_id', 'trang_thai'])

    khoa_hoc = khoa_hoc.resolveChoice(specs=[('_key', 'cast:long')])

    print("khoa_hoc: ", khoa_hoc.count())

    hoc_vien = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                             table_name="hoc_vien")

    hoc_vien = hoc_vien.select_fields(
        ['_key', 'mahv', 'crm_id', 'trang_thai'])

    hoc_vien = hoc_vien.resolveChoice(specs=[('_key', 'cast:long')])

    print("hoc_vien: ", hoc_vien.count())

    goi_sanpham = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                                table_name="goi_sanpham")

    goi_sanpham = goi_sanpham.select_fields(
        ['_key', 'id', 'ma', 'solan_baoluu', 'songay_baoluu', 'trang_thai'])

    goi_sanpham = goi_sanpham.resolveChoice(specs=[('_key', 'cast:long')])

    print("goi_sanpham: ", goi_sanpham.count())

    ghinhan_hp = glueContext.create_dynamic_frame.from_catalog(database="poss",
                                                               table_name="ghinhan_hp")

    ghinhan_hp = ghinhan_hp.select_fields(
        ['_key', 'id', 'ngay_thanhtoan', 'so_tien', 'khoa_hoc_makh', 'trang_thai'])

    ghinhan_hp = ghinhan_hp.resolveChoice(specs=[('_key', 'cast:long')])

    print("ghinhan_hp: ", ghinhan_hp.count())

    # # doc flag tu s3
    df_flag = spark.read.parquet("s3a://dts-odin/flag/flag_rating_class.parquet")
    # max_key = df_flag.collect()[0]['flag']
    # print("max_key: ", max_key)
    # mdl_rating_class = Filter.apply(frame=mdl_rating_class,
    #                                 f=lambda x: x["_key"] > max_key)

    if (ghinhan_hp.count() > 0):
        ghinhan_hp = ghinhan_hp.toDF()

        khoa_hoc = khoa_hoc.toDF()

        hoc_vien = hoc_vien.toDF()

        goi_sanpham = goi_sanpham.toDF()

        data_join1 = ghinhan_hp.join(khoa_hoc, (ghinhan_hp['khoa_hoc_makh'] == khoa_hoc['makh']))
        data_join2 = data_join1.join(hoc_vien, (data_join1['mahv'] == hoc_vien['mahv']))
        data_join3 = data_join2.join(goi_sanpham, (data_join2['goi_sanpham_id'] == goi_sanpham['id']))

        data_join3 = data_join3.withColumn('BehaviorId', f.lit(1))

        data_data_join3 = DynamicFrame.fromDF(data_join3,
                                              glueContext, "datasource0")

        applymapping = ApplyMapping.apply(frame=data_data_join3,
                                          mappings=[("ngay_thanhtoan", "string", "StudentBehaviorDate", "timestamp"),
                                                    ("ma", "string", "PackagePOSSCode", "string"),
                                                    ("so_tien", "double", "measure1", "int"),
                                                    ("songay_baoluu", "int", "songay_baoluu", "int"),
                                                    ("solan_baoluu", "int", "solan_baoluu", "int"),
                                                    ("BehaviorId", "int", "BehaviorId", "int")])

        resolvechoice = ResolveChoice.apply(frame=applymapping, choice="make_cols",
                                            transformation_ctx="resolvechoice2")

        dropnullfields3 = DropNullFields.apply(frame=resolvechoice, transformation_ctx="dropnullfields3")

        print(dropnullfields3.count())
        dropnullfields3.toDF().show()

        datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                   catalog_connection="glue_redshift",
                                                                   connection_options={"dbtable": "student_behavior",
                                                                                       "database": "dts_odin"},
                                                                   redshift_tmp_dir="s3n://dts-odin/topicalms/mdl_toannt_rating_class/",
                                                                   transformation_ctx="datasink4")

        # # ghi flag
        # # lay max key trong data source
        # mdl_rating_class_tmp = mdl_rating_class.toDF()
        # flag = mdl_rating_class_tmp.agg({"_key": "max"}).collect()[0][0]
        #
        # flag_data = [flag]
        # df = spark.createDataFrame(flag_data, "long").toDF('flag')
        #
        # # ghi de _key vao s3
        # df.write.parquet("s3a://dts-odin/flag/flag_rating_class.parquet", mode="overwrite")


if __name__ == "__main__":
    main()
