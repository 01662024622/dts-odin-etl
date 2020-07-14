from pyspark.context import SparkContext
from pyspark.sql.functions import udf, lit, from_unixtime, count, col, sum, when
from pyspark.sql.types import LongType, BooleanType

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import ApplyMapping, ResolveChoice

STUDENT_LEVEL_DATA = [
    ['starter', 1],
    ['sbasic', 2],
    ['basic', 3],
    ['preinter', 4],
    ['inter', 5]
]
PACKAGE_DATA = [
    ['TAAM-TT', 1],
    ['TENUP', 2],
    ['TAAM-TC', 3],
    ['VIP3-TT', 4],
    ['DEMO', 5],
    ['TRAINING', 6],
    ['UNAVAILABLE', 7]
]

PACKAGE_STATUS_DATA = [
    ['SUSPENDED', 1],
    ['DEACTIVED', 2],
    ['ACTIVED', 3],
    ['CANCELLED', 4],
    ['EXPIRED', 5],
    ['UNAVAILABLE', 6]
]
CLASS_TYPE_DATA = [
    ['LS', 1],
    ['SC', 2],
    ['LIVESTREAM', 3]
]
PLATFORM_DATA = [
    ['WEB', 1],
    ['MOBILE', 2]
]

STUDENT_BEHAVIOR_DATABASE = 'olap_student_behavior'
STUDENT_BEHAVIOR_TABLE = 'sb_student_behavior'
STUDENT_BEHAVIOR_FIELDS = ['student_behavior_id',
                           'student_behavior_date',
                           'student_level_code',
                           'advisor_id',
                           'package_code',
                           'package_status_code']

STUDENT_LEARNING_DATABASE = 'olap_student_behavior'
STUDENT_LEARNING_TABLE = 'sb_student_learning'
STUDENT_LEARNING_FIELDS = ['student_behavior_id',
                           'class_type',
                           'platform',
                           'hour_id',
                           'teacher_id',
                           'duration']

STUDENT_LEARNING_FACT_DATABASE = 'student_learning_fact'
STUDENT_LEARNING_FACT_TABLE = 'student_learning_fact'
STUDENT_LEARNING_FACT_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/student_learning_fact'
STUDENT_LEARNING_FACT_TRANSFORM_CTX = 'olap'


def find(lists, key):
    if key is None:
        return 0

    if len(key) < 1:
        return 0

    for items in lists:
        if key.startswith(items[0]):
            return items[1]

    return 0


def get_student_level_id(student_level):
    return find(STUDENT_LEVEL_DATA, student_level)


def get_package_id(package_code):
    return find(PACKAGE_DATA, package_code)


def get_package_status_id(package_status_code):
    return find(PACKAGE_STATUS_DATA, package_status_code)


def get_class_type_id(class_type):
    return find(CLASS_TYPE_DATA, class_type)


def get_platform_id(platform_data):
    return find(PLATFORM_DATA, platform_data)


def is_learning_success(class_type, duration):
    if class_type == 3:
        if duration >= 1500:
            return 1
    else:
        if duration >= 2160:
            return 1
    return 0


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame


def display(data_frame, message):
    print "log_data_frame:", message, data_frame.count()
    data_frame.printSchema()
    data_frame.show(10)


def save_data_to_redshift(glue_context, dynamic_frame, database, table, redshift_tmp_dir, transformation_ctx):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir=redshift_tmp_dir,
                                                    transformation_ctx=transformation_ctx)


def main():
    # ========== init
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    # ========== define function
    udf_get_student_level_id = udf(get_student_level_id, LongType())
    udf_get_package_id = udf(get_package_id, LongType())
    udf_get_package_status_id = udf(get_package_status_id, LongType())
    udf_get_class_type_id = udf(get_class_type_id, LongType())
    udf_get_platform_id = udf(get_platform_id, LongType())
    udf_is_learning_success = udf(is_learning_success, LongType())

    # ========== retrieve student_behavior dynamic frame
    dyf_student_behavior = retrieve_dynamic_frame(glue_context,
                                                  STUDENT_BEHAVIOR_DATABASE,
                                                  STUDENT_BEHAVIOR_TABLE,
                                                  STUDENT_BEHAVIOR_FIELDS)

    dyf_student_learning = retrieve_dynamic_frame(glue_context,
                                                  STUDENT_LEARNING_DATABASE,
                                                  STUDENT_LEARNING_TABLE,
                                                  STUDENT_LEARNING_FIELDS)

    display(dyf_student_behavior, "dyf_student_behavior")
    display(dyf_student_learning, "dyf_student_learning")

    # ========== convert to DF
    df_student_behavior = dyf_student_behavior.toDF()
    df_student_learning = dyf_student_learning.toDF()

    # ========== convert student_behavior
    df_student_behavior = df_student_behavior \
        .withColumn('student_level_id',
                    lit(udf_get_student_level_id(df_student_behavior['student_level_code'])).cast('long')) \
        .withColumn('package_id', lit(udf_get_package_id(df_student_behavior['package_code'])).cast('long')) \
        .withColumn('package_status_id',
                    lit(udf_get_package_status_id(df_student_behavior['package_status_code'])).cast('long')) \
        .withColumn('date_id',
                    lit(from_unixtime(df_student_behavior['student_behavior_date'], format="yyyyMMdd")).cast('long'))

    df_student_learning = df_student_learning \
        .withColumn('class_type_id', lit(udf_get_class_type_id(df_student_learning['class_type'])).cast('long')) \
        .withColumn('platform_id', lit(udf_get_platform_id(df_student_learning['platform'])).cast('long')) \
        .withColumn('learning_success',
                    lit(udf_is_learning_success(df_student_learning['class_type'],
                                                df_student_learning['duration'])).cast('long'))

    display(df_student_behavior, "df_student_behavior")
    display(df_student_learning, "df_student_learning")

    # ========== join
    df_student_learning_fact = df_student_behavior.join(
        df_student_learning,
        on=['student_behavior_id'],
        how='left'
    )

    display(df_student_learning_fact, "df_student_learning_fact")

    # ========== group
    df_student_learning_fact_group = df_student_learning_fact \
        .groupBy('date_id',
                 'student_level_id',
                 'advisor_id',
                 'package_id',
                 'package_status_id',
                 'class_type_id',
                 'platform_id',
                 'hour_id',
                 'teacher_id') \
        .agg(count('duration').alias('number_learning'),
             sum('duration').alias('total_duration'),
             sum('learning_success').alias('number_learning_success'))

    display(df_student_learning_fact_group, "df_student_learning_fact_group")

    dyf_student_learning_fact_group = DynamicFrame.fromDF(df_student_learning_fact_group,
                                                          glue_context,
                                                          "dyf_student_learning_fact_group")

    apply_dyf_student_learning_fact_group = ApplyMapping \
        .apply(frame=dyf_student_learning_fact_group,

               mappings=[("date_id", "long", "date_id", "long"),
                         ("student_level_id", "long", "student_level_id", "long"),
                         ("advisor_id", "long", "advisor_id", "long"),
                         ("package_id", "long", "package_id", "long"),
                         ("package_status_id", "long", "package_status_id", "long"),
                         ("class_type_id", "long", "class_type_id", "long"),
                         ("platform_id", "long", "platform_id", "long"),
                         ("hour_id", "int", "hour_id", "long"),
                         ("teacher_id", "long", "teacher_id", "long"),
                         ("number_learning", "long", "number_learning", "long"),
                         ("total_duration", "long", "total_duration", "long"),
                         ("number_learning_success", "long", "number_learning_success", "long")])

    dyf_student_learning_fact_group = ResolveChoice.apply(frame=apply_dyf_student_learning_fact_group,
                                                          choice="make_cols",
                                                          transformation_ctx="resolvechoice2")

    display(dyf_student_learning_fact_group, "dyf_student_learning_fact_group")

    save_data_to_redshift(glue_context,
                          dyf_student_learning_fact_group,
                          STUDENT_LEARNING_FACT_DATABASE,
                          STUDENT_LEARNING_FACT_TABLE,
                          STUDENT_LEARNING_FACT_TMP_DIR,
                          STUDENT_LEARNING_FACT_TRANSFORM_CTX)


def test():
    print is_learning_success(3, 140)
    print is_learning_success(3, 1500)
    print is_learning_success(3, 1540)
    print is_learning_success(2, 2150)
    print is_learning_success(2, 2160)
    print is_learning_success(2, 2180)
    print is_learning_success(1, 2150)
    print is_learning_success(1, 2160)
    print is_learning_success(1, 2180)


if __name__ == '__main__':
    main()
    # test()
