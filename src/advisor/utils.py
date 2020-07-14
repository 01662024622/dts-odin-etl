from awsglue.transforms import Filter
from awsglue.dynamicframe import DynamicFrame


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame


def save_flag(data_frame, flag_path):
    data_frame.write.parquet(flag_path, mode="overwrite")


def get_flag(spark, data_frame):
    flag = data_frame.agg({"_key": "max"}).collect()[0][0]
    flag_data = [flag]
    return spark.createDataFrame(flag_data, "string").toDF('flag')


def filter_latest(spark, dynamic_frame, config_file):
    try:
        df_flag = spark.read.parquet(config_file)
        start_read = df_flag.collect()[0]['flag']
        print 'read from index: ', start_read

        result = Filter.apply(frame=dynamic_frame, f=lambda x: x['_key'] > start_read)
        return result
    except():
        print 'read flag file error '
        return dynamic_frame


def data_frame_filter_not_null(data_frame, fields=[]):
    string = ""
    for i in fields:
        string = string + ' ' + i + ' != "" and'
    string = string[:-3]
    return data_frame.where(string)


def display(data_frame, message):
    print "log_data_frame:", message, data_frame.count()
    data_frame.printSchema()
    data_frame.show(10)


def from_data_frame(data_frame, glue_context, name):
    return DynamicFrame.fromDF(data_frame, glue_context, name)


def save_data(glue_context, frame, path, partition_keys=[]):
    glue_context.write_dynamic_frame.from_options(frame=frame, connection_type="s3",
                                                  connection_options={
                                                      "path": path,
                                                      "partitionKeys": partition_keys},
                                                  format="parquet")
