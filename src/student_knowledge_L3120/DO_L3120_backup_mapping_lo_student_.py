import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame
from datetime import date, timedelta
import pyspark.sql.functions as f
import boto3

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")

    today = date.today()
    today_id = today.strftime("%Y%m%d")
    today_id = long(today_id)
    backup_day_number = 1


    #properties
    is_backup = False

    def get_active_backup_date(backup_day_number):
        active_backup_date = []
        today = date.today()
        today_id = today.strftime("%Y%m%d")
        for i in range(0, backup_day_number):
            date_id = (date.today() - timedelta(days=i)).strftime("%Y%m%d")
            active_backup_date.append(long(date_id))
        return active_backup_date

    active_backup_dates = get_active_backup_date(backup_day_number)
    print ('active_backup_dates')
    print (active_backup_dates)

    #delete voi nhung bac qua lau
    print ('start---------------')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('dts-odin')
    root_folder = "nvn_knowledge/backup_mapping_lo_student/backup_date="
    root_folder_length = len(root_folder)
    print('root_folder_length: ', root_folder_length)
    backup_mapping_lo_student_object = bucket.objects.filter(Prefix=root_folder,
                                                             Delimiter='/')

    backed_up_dates = []

    for key in backup_mapping_lo_student_object:
        backup_date = key.key[root_folder_length : root_folder_length + 8]
        backed_up_dates.append(long(backup_date))
    print ('backed_up_dates: ')
    print (backed_up_dates)


    # check is back_up_today
    print ('today_id: ', today_id)
    print ('backed_up_dates: ')
    print (backed_up_dates)
    if today_id in backed_up_dates:
        is_backup = True

    #Clear all old backup
    backup_clear_date = backed_up_dates
    print ('backup_clear_date')
    print (backup_clear_date)
    for active_backup_date in active_backup_dates:
        print ('active_backup_date: ', active_backup_date)
        if active_backup_date in backup_clear_date:
            backup_clear_date.remove(active_backup_date)
    print ('backup_clear_date')
    print (backup_clear_date)


    #Clear
    for backup_date_id in backup_clear_date:
        print('backup_date_id: ', backup_date_id)
        # bucket.objects.filter(Prefix=root_folder + backup_date_id).delete()

    #backup

    print ('check :: is_backup:: final: ', is_backup)

    if is_backup == False:
        # dyf_mapping_lo_student = glueContext.create_dynamic_frame.from_catalog(database="nvn_knowledge",
        #                                                             table_name="mapping_lo_student")

        dyf_mapping_lo_student = glueContext.create_dynamic_frame.from_options(
            connection_type="redshift",
            connection_options={
                "url": "jdbc:redshift://datashine-dev.c4wxydftpsto.ap-southeast-1.redshift.amazonaws.com:5439/dts_odin",
                "user": "dtsodin",
                "password": "DWHDtsodin@123",
                "dbtable": "mapping_lo_student",
                "redshiftTmpDir": "s3://dts-odin/temp1/thanhtv3/temp_v1_mapping_lo_student/v8"}
        )

        df_mapping_lo_student = dyf_mapping_lo_student.toDF()
        df_mapping_lo_student = df_mapping_lo_student.withColumn('backup_date', f.lit(today_id))

        dyf_mapping_lo_student_bk = DynamicFrame.fromDF(df_mapping_lo_student, glueContext, 'dyf_mapping_lo_student_bk')


        print('dyf_mapping_lo_student_bk')
        dyf_mapping_lo_student_bk.printSchema()
        dyf_mapping_lo_student_bk.show(1)

        glueContext.write_dynamic_frame.from_options(
            frame=dyf_mapping_lo_student_bk,
            connection_type="s3",
            connection_options={"path": "s3://dts-odin/nvn_knowledge/backup_mapping_lo_student",
                                "partitionKeys": ["backup_date"]},
            format="parquet")




if __name__ == "__main__":
    main()
