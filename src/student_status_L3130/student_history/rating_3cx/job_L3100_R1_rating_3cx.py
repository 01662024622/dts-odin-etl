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
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    # get dynamic frame source
    dyf_3cx_advisor_call = glueContext.create_dynamic_frame.from_catalog(database='callcenter',
                                                                            table_name='advisorcall')

    dyf_3cx_advisor_call = dyf_3cx_advisor_call.resolveChoice(specs=[('_key', 'cast:long')])
    # print schema and select fields
    print('original schema')
    dyf_3cx_advisor_call.printSchema()

    try:
        df_flag = spark.read.parquet("s3a://dtsodin/flag/student_status/temp_ls_rating_3cx_v1.parquet")
        read_from_index = df_flag.collect()[0]['flag']
        print('read from index: ', read_from_index)
        dyf_3cx_advisor_call = Filter.apply(frame=dyf_3cx_advisor_call,
                                      f=lambda x: x["_key"] > read_from_index)
    except:
        print('read flag file error ')
    print('the number of new contacts: ', dyf_3cx_advisor_call.count())

    dyf_3cx_advisor_call = dyf_3cx_advisor_call.select_fields(
        ['_key', 'device', 'hanguptvts', 'status','phonenumber', 'rating', 'calldate'])
        # .rename_field('statuss', 'status')

    dy_source_3cx_cache = dyf_3cx_advisor_call.toDF()
    dy_source_3cx_cache = dy_source_3cx_cache.dropDuplicates(['_key'])
    dy_source_3cx_cache = dy_source_3cx_cache.cache()
    dyf_3cx_advisor_call = DynamicFrame.fromDF(dy_source_3cx_cache, glueContext, 'dyf_3cx_advisor_call')

    if ( dyf_3cx_advisor_call.count() > 0 ):
        dyf_3cx_advisor_call = Filter.apply(frame=dyf_3cx_advisor_call,
                                                  f=lambda x: x["device"] == '3CX' and x["status"] == 'ANSWER' and x["hanguptvts"] == 1
                                                             and x["phonenumber"] is not None and x["phonenumber"] != ''
                                                             and x["calldate"] is not None and x["calldate"] != ''
                                                             and x["rating"] is not None and x["rating"] > 0
                                                             and x["rating"] < 6)

        print('dyf_3cx_advisor_call::corrcect')
        print('dyf_3cx_advisor_call number', dyf_3cx_advisor_call.count())
        if(dyf_3cx_advisor_call.count() > 0):


            dyf_3cx_advisor_call = dyf_3cx_advisor_call.resolveChoice(
                specs=[('phonenumber', 'cast:string')])
            dyf_3cx_advisor_call.printSchema()
            #convert data
            df_advisor_call = dyf_3cx_advisor_call.toDF()
            df_advisor_call = df_advisor_call.withColumn('id_time',
                                                   from_unixtime(unix_timestamp(df_advisor_call.calldate, "yyyy-MM-dd HH:mm:ss"),
                                                                 "yyyyMMdd"))

            df_advisor_call = df_advisor_call.groupby('phonenumber', 'id_time', 'rating').agg(f.count('_key').alias("so_lan"))

            df_advisor_call = df_advisor_call.withColumn('phonenumber_correct', f.concat(f.lit('0'), df_advisor_call.phonenumber))

            df_advisor_call = df_advisor_call.withColumn('rating_status', f.lit(60) + df_advisor_call.rating)

            dyf_3cx_advisor_call_rating_number = DynamicFrame.fromDF(df_advisor_call, glueContext, 'dyf_3cx_advisor_call_rating_number')

            dyf_3cx_advisor_call_rating_number = dyf_3cx_advisor_call_rating_number.resolveChoice(
                specs=[('so_lan', 'cast:int')])

            print('dyf_3cx_advisor_call::after::group::schema')
            dyf_3cx_advisor_call_rating_number.printSchema()
            dyf_3cx_advisor_call_rating_number.show(10)
            print('dyf_3cx_advisor_call after::group: ', dyf_3cx_advisor_call_rating_number.count())

            dyf_ad_contact_phone = glueContext.create_dynamic_frame.from_catalog(database='tig_advisor',
                                                                                 table_name='student_contact_phone')

            dyf_ad_contact_phone = dyf_ad_contact_phone.select_fields(
                ['phone', 'contact_id'])

            dyf_ad_contact_phone = Filter.apply(frame=dyf_ad_contact_phone,
                                                f=lambda x: x["phone"] is not None and x["phone"] != ''
                                                            and x["contact_id"] is not None and x["contact_id"] != ''
                                                )

            print('dyf_ad_contact_phone::schema')
            dyf_ad_contact_phone.printSchema()


            #-----------------------------------------------------------------------------------------------------------#
            join = Join.apply(dyf_3cx_advisor_call_rating_number, dyf_ad_contact_phone, 'phonenumber_correct', 'phone')


            print('join::schema------------')
            join.printSchema()
            join.show(2)
            print('join: ', join.count())


            # # chon field
            applymapping1 = ApplyMapping.apply(frame=join,
                                               mappings=[("contact_id", "string", "contact_id", "string"),
                                                         ("id_time", "string", "id_time", "bigint"),
                                                         ("phone", "string", "phone", "string"),
                                                         ("rating_status", "int", "rating_status", "int"),
                                                         ("rating", "int", "rating", "int"),
                                                         ("so_lan", "int", "so_lan", "int")])

            resolvechoice2 = ResolveChoice.apply(frame=applymapping1, choice="make_cols",
                                                 transformation_ctx="resolvechoice2")
            dropnullfields3 = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields3")


            print('dropnullfields3::printSchema')
            dropnullfields3.printSchema()
            dropnullfields3.show(2)


            # ghi data vao redshift
            datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3,
                                                                       catalog_connection="glue_redshift",
                                                                       connection_options={"dbtable": "temp_ls_rating_3cx_v1",
                                                                                           "database": "dts_odin",
                                                                                           "postactions": """
                                                                                           INSERT into mapping_changed_status_student(contact_id, change_status_date_id, user_id,to_status_id, measure1)
                                                                                                SELECT t3cx.contact_id, t3cx.id_time, um.user_id, t3cx.rating_status, t3cx.so_lan 
                                                                                                FROM temp_ls_rating_3cx_v1 t3cx
                                                                                                LEFT JOIN user_map um
                                                                                                     ON um.source_type = 1
                                                                                                     AND um.source_id = t3cx.contact_id	 
                                                                                                WHERE len(t3cx.contact_id) < 33
                                                                                                ;
                                                                                                DROP TABLE IF EXISTS public.temp_ls_rating_3cx_v1
                                                                                           """
                                                                                           },
                                                                       redshift_tmp_dir="s3n://dts-odin/temp/temp_ls_rating_3cx_v1",
                                                                       transformation_ctx="datasink4")
            df_datasource = dyf_3cx_advisor_call.toDF()
            flag = df_datasource.agg({"_key": "max"}).collect()[0][0]
            flag_data = [flag]
            df = spark.createDataFrame(flag_data, "long").toDF('flag')
            df.write.parquet("s3a://dtsodin/flag/student_status/temp_ls_rating_3cx_v1.parquet", mode="overwrite")
            dy_source_3cx_cache.unpersist()


if __name__ == "__main__":
    main()