from pyspark.context import SparkContext

from awsglue import DynamicFrame
from awsglue.context import GlueContext

REDSHIFT_USERNAME = 'dtsodin'
REDSHIFT_PASSWORD = 'DWHDtsodin@123'


def retrieve_dynamic_frame(glue_context, database, table_name, fields=[], casts=[]):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database=database, table_name=table_name)
    if len(fields) > 0:
        dynamic_frame = dynamic_frame.select_fields(fields)
    if len(casts) > 0:
        dynamic_frame = dynamic_frame.resolveChoice(casts)
    return dynamic_frame.toDF()


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

    # ========== retrieve dynamic frame
    df_advisor = retrieve_dynamic_frame(
        glue_context,
        'tig_advisor',
        'advisor_account',
        ['user_id', 'user_name', 'user_email']
    )
    display(df_advisor, "df_advisor")

    df_advisor = df_advisor.withColumnRenamed('user_id', 'advisor_id').withColumnRenamed('user_email', 'email')
    display(df_advisor, "df_advisor renamed")

    dyf_advisor = DynamicFrame.fromDF(
        df_advisor,
        glue_context,
        "dyf_advisor"
    )
    display(dyf_advisor, "dyf_advisor")

    # ========== save dynamic frame to redshift
    save_data_to_redshift(
        glue_context,
        dyf_advisor,
        'student_learning_fact',
        'advisor_dim',
        's3://datashine-dev-redshift-backup/student_learning_fact/advisor_dim',
        'student_learning_dim'
    )


if __name__ == '__main__':
    main()
