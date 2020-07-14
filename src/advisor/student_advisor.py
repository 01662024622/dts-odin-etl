from pyspark.context import SparkContext
from pyspark.sql.functions import create_map, collect_list, array

from awsglue.context import GlueContext
from utils import *


# flag_file = 's3://dtsodin/flag/student_behavior/student_behavior_livestream.parquet'
#
# student_behavior_s3_path = "s3://dtsodin/student_behavior/student_behavior/"
# student_behavior_s3_partition = ["behavior_id"]
#
# student_learning_s3_path = "s3://dtsodin/student_behavior/student_learning/"
# student_learning_s3_partition = ["behavior_id"]


def init():
    return GlueContext(SparkContext.getOrCreate())


def convert_to_start_end(arr=[]):
    result = []
    for i in range(0, len(arr) - 1):
        item = ""
        item = item + arr[i]


def main():
    # ========== init
    glue_context = init()
    spark = glue_context.spark_session

    # =========== create_dynamic_frame
    dyf_log_change_assignment_advisor = retrieve_dynamic_frame(glue_context, 'tig_advisor',
                                                               'log_change_assignment_advisor',
                                                               ['id', 'contact_id', 'advisor_id_new',
                                                                'advisor_id_old', 'created_at', 'updated_at'])
    display(data_frame=dyf_log_change_assignment_advisor, message="dyf_log_change_assignment_advisort")

    # =========== advisor_id_creaded_at = (advisor_id_new,created_at)
    df_log_change_assignment_advisor = dyf_log_change_assignment_advisor.toDF()

    df_log_change_assignment_advisor = df_log_change_assignment_advisor.withColumn(
        'advisor_id_creaded_at', array('advisor_id_new', 'created_at').alias(
            "advisor_id_creaded_at"))

    display(df_log_change_assignment_advisor, "df_log_change_assignment_advisor")

    # ===========
    df_log_change_assignment_advisor = data_frame_filter_not_null(df_log_change_assignment_advisor,
                                                                  ['id', 'contact_id', 'advisor_id_new', 'created_at'])

    df_log_change_assignment_advisor = df_log_change_assignment_advisor.groupby('contact_id').agg(
        collect_list('advisor_id_creaded_at').alias('advisor_id_creaded_at_array'))

    display(df_log_change_assignment_advisor, "df_log_change_assignment_advisor")

    print df_log_change_assignment_advisor.limit(100).collect()


if __name__ == '__main__':
    main()
