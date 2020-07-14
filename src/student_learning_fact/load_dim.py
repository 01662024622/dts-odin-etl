from pyspark.context import SparkContext

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import ApplyMapping, ResolveChoice

STUDENT_LEVEL_HEADER = ['student_level_code', 'student_level_id']
STUDENT_LEVEL_DATA = [
    ('starter', 1),
    ('sbasic', 2),
    ('basic', 3),
    ('preinter', 4),
    ('inter', 5)
]
STUDENT_LEVEL_MAPPING = [
    ("student_level_id", "long", "student_level_id", "long"),
    ("student_level_code", "string", "student_level_code", "string")
]
STUDENT_LEVEL_DATABASE = 'student_learning_fact'
STUDENT_LEVEL_TABLE = 'student_level_dim'
STUDENT_LEVEL_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/student_level_dim'

PACKAGE_HEADER = ['package_code', 'package_id']
PACKAGE_DATA = [
    ('TAAM-TT', 1),
    ('TENUP', 2),
    ('TAAM-TC', 3),
    ('VIP3-TT', 4),
    ('DEMO', 5),
    ('TRAINING', 6),
    ('UNAVAILABLE', 7)
]
PACKAGE_MAPPING = [
    ("package_id", "long", "package_id", "long"),
    ("package_code", "string", "package_code", "string")
]
PACKAGE_DATABASE = 'student_learning_fact'
PACKAGE_TABLE = 'package_dim'
PACKAGE_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/package_dim'

PACKAGE_STATUS_HEADER = ['package_status_code', 'package_status_id']
PACKAGE_STATUS_DATA = [
    ('SUSPENDED', 1),
    ('DEACTIVED', 2),
    ('ACTIVED', 3),
    ('CANCELLED', 4),
    ('EXPIRED', 5),
    ('UNAVAILABLE', 6)
]
PACKAGE_STATUS_MAPPING = [
    ("package_status_id", "long", "package_status_id", "long"),
    ("package_status_code", "string", "package_status_code", "string")
]
PACKAGE_STATUS_DATABASE = 'student_learning_fact'
PACKAGE_STATUS_TABLE = 'package_status_dim'
PACKAGE_STATUS_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/package_status_dim'

CLASS_TYPE_HEADER = ['class_type_code', 'class_type_id']
CLASS_TYPE_DATA = [
    ('LS', 1),
    ('SC', 2),
    ('LIVESTREAM', 3)
]
CLASS_TYPE_MAPPING = [
    ("class_type_id", "long", "class_type_id", "long"),
    ("class_type_code", "string", "class_type_code", "string")
]
CLASS_TYPE_DATABASE = 'student_learning_fact'
CLASS_TYPE_TABLE = 'class_type_dim'
CLASS_TYPE_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/class_type_dim'

PLATFORM_HEADER = ['platform_code', 'platform_id']
PLATFORM_DATA = [
    ('WEB', 1),
    ('MOBILE', 2)
]
PLATFORM_MAPPING = [
    ("platform_id", "long", "platform_id", "long"),
    ("platform_code", "string", "platform_code", "string")
]
PLATFORM_DATABASE = 'student_learning_fact'
PLATFORM_TABLE = 'platform_dim'
PLATFORM_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/platform_dim'

HOUR_HEADER = ['hour_id', 'hour_code']
HOUR_DATA = [
    (1, '8h'),
    (2, '9h'),
    (3, '10h'),
    (4, '11h'),
    (5, '12h'),
    (6, '13h'),
    (7, '14h'),
    (8, '15h'),
    (9, '16h'),
    (10, '17h'),
    (11, '18h'),
    (12, '19h'),
    (13, '20h'),
    (14, '21h'),
    (15, '22h'),
    (16, '23h'),
]
HOUR_MAPPING = [
    ("hour_id", "long", "hour_id", "long"),
    ("hour_code", "string", "hour_code", "string")
]
HOUR_DATABASE = 'student_learning_fact'
HOUR_TABLE = 'hour_dim'
HOUR_TMP_DIR = 's3://datashine-dev-redshift-backup/student_learning_fact/hour_dim'


def retrieve_dynamic_frame(glue_context, spark, data, header, name, mapping):
    data_frame = spark.createDataFrame(data, header)
    dynamic_frame = DynamicFrame.fromDF(data_frame, glue_context, name)

    display(dynamic_frame, "retrieve" + name)

    apply_dynamic_frame = ApplyMapping.apply(frame=dynamic_frame, mappings=mapping)
    dyf_result = ResolveChoice.apply(frame=apply_dynamic_frame,
                                     choice="make_cols",
                                     transformation_ctx="resolvechoice2")

    display(dynamic_frame, "apply" + name)

    return dyf_result


def save_data_to_redshift(glue_context, dynamic_frame, database, table, redshift_tmp_dir, transformation_ctx):
    glue_context.write_dynamic_frame.from_jdbc_conf(frame=dynamic_frame,
                                                    catalog_connection="glue_redshift",
                                                    connection_options={
                                                        "dbtable": table,
                                                        "database": database
                                                    },
                                                    redshift_tmp_dir=redshift_tmp_dir,
                                                    transformation_ctx=transformation_ctx)


def display(data_frame, message):
    print "log_data_frame:", message, data_frame.count()
    data_frame.printSchema()
    data_frame.show(10)


def main():
    # ========== init
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    # ========== retrieve dynamic frame
    dyf_student_level = retrieve_dynamic_frame(glue_context,
                                               spark,
                                               STUDENT_LEVEL_DATA,
                                               STUDENT_LEVEL_HEADER,
                                               "dyf_student_level",
                                               STUDENT_LEVEL_MAPPING)

    dyf_package = retrieve_dynamic_frame(glue_context,
                                         spark,
                                         PACKAGE_DATA,
                                         PACKAGE_HEADER,
                                         "dyf_package",
                                         PACKAGE_MAPPING)

    dyf_package_status = retrieve_dynamic_frame(glue_context,
                                                spark,
                                                PACKAGE_STATUS_DATA,
                                                PACKAGE_STATUS_HEADER,
                                                "dyf_package_status",
                                                PACKAGE_STATUS_MAPPING)

    dyf_class_type = retrieve_dynamic_frame(glue_context,
                                            spark,
                                            CLASS_TYPE_DATA,
                                            CLASS_TYPE_HEADER,
                                            "dyf_class_type",
                                            CLASS_TYPE_MAPPING)

    dyf_platform = retrieve_dynamic_frame(glue_context,
                                          spark,
                                          PLATFORM_DATA,
                                          PLATFORM_HEADER,
                                          "dyf_platform",
                                          PLATFORM_MAPPING)

    dyf_hour = retrieve_dynamic_frame(glue_context,
                                      spark,
                                      HOUR_DATA,
                                      HOUR_HEADER,
                                      "dyf_hour",
                                      HOUR_MAPPING)

    display(dyf_student_level, "dyf_platform")
    display(dyf_package, "dyf_package")
    display(dyf_package_status, "dyf_package_status")
    display(dyf_class_type, "dyf_class_type")
    display(dyf_platform, "dyf_platform")
    display(dyf_hour, "dyf_hour")

    # ========== save to redshift
    save_data_to_redshift(glue_context,
                          dyf_student_level,
                          STUDENT_LEVEL_DATABASE,
                          STUDENT_LEVEL_TABLE,
                          STUDENT_LEVEL_TMP_DIR,
                          'student_learning_dim')
    save_data_to_redshift(glue_context,
                          dyf_package,
                          PACKAGE_DATABASE,
                          PACKAGE_TABLE,
                          PACKAGE_TMP_DIR,
                          'student_learning_dim')
    save_data_to_redshift(glue_context,
                          dyf_package_status,
                          PACKAGE_STATUS_DATABASE,
                          PACKAGE_STATUS_TABLE,
                          PACKAGE_STATUS_TMP_DIR,
                          'student_learning_dim')
    save_data_to_redshift(glue_context,
                          dyf_class_type,
                          CLASS_TYPE_DATABASE,
                          CLASS_TYPE_TABLE,
                          CLASS_TYPE_TMP_DIR,
                          'student_learning_dim')
    save_data_to_redshift(glue_context,
                          dyf_platform,
                          PLATFORM_DATABASE,
                          PLATFORM_TABLE,
                          PLATFORM_TMP_DIR,
                          'student_learning_dim')
    save_data_to_redshift(glue_context,
                          dyf_hour,
                          HOUR_DATABASE,
                          HOUR_TABLE,
                          HOUR_TMP_DIR,
                          'student_learning_dim')


if __name__ == '__main__':
    main()
