# -*- coding: utf-8 -*-
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, SQLContext
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from datetime import date, datetime, timedelta
import pytz

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")
    sqlContext = SQLContext(sc)
    # get dynamic frame source

    ho_chi_minh_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    today = datetime.now(ho_chi_minh_timezone)
    today_second =  long(today.strftime("%s"))
    print('today_id: ', today_second)
    #------------------------------------------------------------------------------------------------------------------#

    behavior_category_dir = [
        {'behavior_id': '1', 'behavior_code': 'SB1', 'behavior_name': u'Học viên đóng tiền mua khóa học'},
        {'behavior_id': '2', 'behavior_code': 'SB2', 'behavior_name': u'Học viên được tạo tài khoản'},
        {'behavior_id': '3', 'behavior_code': 'SB3', 'behavior_name': u'Học viên được mua gói, nạp tiền'},
        {'behavior_id': '4', 'behavior_code': 'SB4', 'behavior_name': u'Học viên được chào mừng'},
        {'behavior_id': '5', 'behavior_code': 'SB5', 'behavior_name': u'Học viên đã được kiểm tra kỹ thuật'},
        {'behavior_id': '6', 'behavior_code': 'SB6', 'behavior_name': u'Học viên kích hoạt gói học'},
        {'behavior_id': '7', 'behavior_code': 'SB7', 'behavior_name': u'Học viên thay đổi trạng thái tài khoản'},
        {'behavior_id': '8', 'behavior_code': 'SB8', 'behavior_name': u'Học viên kích hoạt tài khoản'},
        {'behavior_id': '9', 'behavior_code': 'SB29', 'behavior_name': u'Học viên được thay đổi Advisor'},
        {'behavior_id': '10', 'behavior_code': 'SB9', 'behavior_name': u'Học viên thay đổi trạng thái gói học'},
        {'behavior_id': '11', 'behavior_code': 'SB10', 'behavior_name': u'Học viên học LS'},
        {'behavior_id': '12', 'behavior_code': 'SB11', 'behavior_name': u'Học viên học SC'},
        {'behavior_id': '13', 'behavior_code': 'SB12', 'behavior_name': u'Học viên học LT'},
        {'behavior_id': '14', 'behavior_code': 'SB13', 'behavior_name': u'Học viên học Voxy'},
        {'behavior_id': '15', 'behavior_code': 'SB14', 'behavior_name': u'Học viên học Homework'},
        {'behavior_id': '16', 'behavior_code': 'SB15', 'behavior_name': u'Học viên học Native Talk'},
        {'behavior_id': '17', 'behavior_code': 'SB16', 'behavior_name': u'Học viên học NCSB'},
        {'behavior_id': '18', 'behavior_code': 'SB17', 'behavior_name': u'Học viên học Micro'},
        {'behavior_id': '19', 'behavior_code': 'SB18', 'behavior_name': u'Học viên học AIT'},
        {'behavior_id': '20', 'behavior_code': 'SB19', 'behavior_name': u'Học viên thi ngày AIP'},
        {'behavior_id': '21', 'behavior_code': 'SB20', 'behavior_name': u'Học viên thi tuần AIP'},
        {'behavior_id': '22', 'behavior_code': 'SB21', 'behavior_name': u'Học viên thi Native test tuần'},
        {'behavior_id': '23', 'behavior_code': 'SB22', 'behavior_name': u'Học viên thi Native test tháng'},
        {'behavior_id': '24', 'behavior_code': 'SB23', 'behavior_name': u'Học viên rating Cara (LS/SC)'},
        {'behavior_id': '25', 'behavior_code': 'SB24', 'behavior_name': u'Học viên rating hotline'},
        {'behavior_id': '26', 'behavior_code': 'SB25', 'behavior_name': u'Học viên rating Native smile caresoft'},
        {'behavior_id': '27', 'behavior_code': 'SB26', 'behavior_name': u'Học viên rating Native smile caresoft'},
        {'behavior_id': '28', 'behavior_code': 'SB27', 'behavior_name': u'Học viên rating lớp LT'},
        {'behavior_id': '29', 'behavior_code': 'SB28', 'behavior_name': u'Học viên rating Native test tuần'},
        {'behavior_id': '30', 'behavior_code': 'SB29', 'behavior_name': u'Học viên rating Native test tháng'},
        {'behavior_id': '31', 'behavior_code': 'SB30', 'behavior_name': u'Học viên login vào hệ thống LMS'}
    ]

    df_behavior_category = sqlContext.createDataFrame(behavior_category_dir)
    print ('df_behavior_category')
    df_behavior_category.printSchema()
    df_behavior_category.show()

    dyf_behavior_category = DynamicFrame.fromDF(df_behavior_category, glueContext, 'dyf_behavior_category')

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_behavior_category,
        connection_type="s3",
        connection_options={"path": "s3://dtsodin/student_behavior/behavior"},
        format="parquet")


if __name__ == "__main__":
    main()