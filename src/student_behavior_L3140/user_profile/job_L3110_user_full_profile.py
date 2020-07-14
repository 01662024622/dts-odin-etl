import sys
import pyspark.sql.functions as f

from pyspark import SparkContext
from pyspark.sql.types import StringType

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import Filter, ApplyMapping, ResolveChoice, DropNullFields, RenameField, Join
from datetime import date

#--------------------------------------------------Spark and Glue--------------------------------------------------------------------#
# sparkcontext
sc = SparkContext()
# glue with sparkcontext env
glueContext = GlueContext(sc)
# create new spark session working
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "GMT+07:00")


#--------------------------------------------------Data for Comunication-------------------------------------------------------------------#
is_dev = True
is_limit = False
today = date.today()
d4 = today.strftime("%Y-%m-%d").replace('-','')
comunication=[("Id", "int", "user_id", "bigint"),
              ("communication_type", 'int', 'communication_type', 'int'),
              ("is_primary", 'boolean', 'is_primary', 'boolean'),
              ("is_deleted", 'boolean', 'is_deleted', 'boolean'),
              ("comunication", 'string', 'communication', 'string'),
              ("last_update_date", 'string', 'last_update_date', 'bigint')]

profile=[("Id", "int", "user_id", "bigint"),
              ("mapper_user_id", "int", "mapped_user_id", "bigint"),
              ("Gender", "int", "gender", "int"),
              ("Birthday", "string", "birthday", "timestamp"),
              ("Job", "string", "job", "string"),
              ("is_root", "boolean", "is_root", "boolean"),
              ("position", "string", "position", "string"),
              ("last_update_date", "string", "last_update_date", "bigint")]



mapper=[("Id", "int", "user_id", "bigint"),
              ("source_type", "int", "source_type", "int"),
              ("source_id", "string", "source_id", "string"),
              ("description", 'string', "description", "string"),
              ("last_update_date", "string", "last_update_date", "bigint")]

mapper_add_collum={'description': f.lit(''),
                   'source_type': f.lit(1),
                   'last_update_date': f.lit(d4)}

comunication_add_collum={'communication_type': f.lit(1),
                         'is_primary': f.lit(True),
                         'is_deleted': f.lit(False),
                         'last_update_date': f.lit(d4)}

profile_add_collum={'position': f.lit(''),
                         'mapper_user_id': f.lit(0),
                         'is_root': f.lit(True),
                         'last_update_date': f.lit(d4)}
comunication_add_collum_for_phone={'communication_type': f.lit(1),
                         'is_primary': f.lit(1),
                         'last_update_date': f.lit(d4)}

#------------------------------------------------------Function for Tranform-------------------------------------------#
def connectGlue(database="",table_name="",select_fields=[],fillter=[],cache=False,duplicates=[]):
    if database=="" and table_name=="":
        return
    dyf = glueContext.create_dynamic_frame.from_catalog(database=database,
                                                                   table_name=table_name)

    if is_dev:
        print("full schema of table: " ,database, "and table: ", table_name)
        dyf.printSchema()
    #     select felds
    if len(select_fields)!=0:
        dyf = dyf.select_fields(select_fields)

    if len(fillter) != 0:
        dyf= fillterOutNull(dyf,fillter)
    else:
        dyf= fillterOutNull(dyf,select_fields)
        
    if is_limit and cache:
        df = dyf.toDF()
        df = df.limit(500)
        df = checkDumplicate(df,duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit== False and cache:
        df = dyf.toDF()
        df = checkDumplicate(df,duplicates)
        df = df.cache()
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    elif is_limit and cache==False:
        df = dyf.toDF()
        df = checkDumplicate(df,duplicates)
        df = df.limit(500)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)
    else:
        df = dyf.toDF()
        df = checkDumplicate(df,duplicates)
        dyf = DynamicFrame.fromDF(df, glueContext, table_name)

    if is_dev:
        print("full schema of table: " ,database, " and table: ", table_name," after select")
        dyf.printSchema()
    return dyf
#----------------------------------------------------------------------------------------------------------------------#

def print_is_dev(string):
    if is_dev:
        print string

#----------------------------------------------------------------------------------------------------------------------#

def checkDumplicate(df,duplicates):
    if len(duplicates) == 0:
        return df.dropDuplicates()
    else:
        return df.dropDuplicates(duplicates)

#----------------------------------------------------------------------------------------------------------------------#
def parquetToS3(dyf,path='default',format="parquet"):
    glueContext.write_dynamic_frame.from_options(frame=dyf, connection_type="s3",
                                                 connection_options={
                                                     "path": path
                                                 },
                                                 format=format,
                                                 transformation_ctx="datasink6")
#-----------------------------------------------------------------------------------------------------------------------#
def fillterOutNull(dynamicFrame, fields):
    for field in fields:
        dynamicFrame = Filter.apply(frame=dynamicFrame, f=lambda x: x[field] is not None and x[field] != '')
    return dynamicFrame

#-----------------------------------------------------------------------------------------------------------------------#
def myFunc(data_list):
    for val in data_list:
        if val is not None and val !='':
            return val
    return None

# -----------------------------------------------------------------------------------------------------------------------#


def mappingForAll(dynamicFrame,mapping,add_collum):
    df_communication = dynamicFrame.toDF()
    df_communication = df_communication.dropDuplicates()

    for k, v in add_collum.items():
        df_communication = df_communication.withColumn(k, v)

    dyf_communication = DynamicFrame.fromDF(df_communication, glueContext, 'dyf_communication')
    applymapping2 = ApplyMapping.apply(frame=dyf_communication,
                                       mappings=mapping)

    resolvechoice2 = ResolveChoice.apply(frame=applymapping2, choice="make_cols",
                                         transformation_ctx="resolvechoice2")

    dyf_communication_after_mapping = DropNullFields.apply(frame=resolvechoice2, transformation_ctx="dropnullfields2")
    return dyf_communication_after_mapping




#-------------------------------------------------------Main-----------------------------------------------------------#

def main():
    # --------------------------------------------------------------------------------------------------------------#
    dyf_crm_contacts= connectGlue(database='crm_native',table_name='contacts',
                select_fields=['Id', 'Code', 'Fullname', 'Address', 'Email', 'Email2', 'Job', 'Birthday', 'Gender'],
                fillter=['Id', 'Code'],
                cache=True)

    dyf_crm_contacts.show(10)
   # ------------------------------------------------------------------------------------------------------------------#
    number_crm_concat = dyf_crm_contacts.count()

    if is_dev:
        print ('number_crm_concat: ', number_crm_concat)
    if number_crm_concat < 1:
        return

    etl_user_profile(dyf_crm_contacts)
    # etl_user_map(dyf_crm_contacts)
    etl_user_communication(dyf_crm_contacts)
    # dyf_crm_concat = DynamicFrame.fromDF(df_source_cache, glueContext, 'dyf_crm_concats_name')
    #
    # dyf_crm_concat = DynamicFrame.fromDF(df_source_cache, glueContext, 'dyf_crm_concats_name')



def etl_user_profile(dyf_crm_concat):
    print_is_dev('etl_user_profile___****************************************************************************************')
    # ETL for profile
    myUdf = f.udf(myFunc, StringType())

    df_crm_concat_pf = dyf_crm_concat.toDF()
    df_crm_concat_pf = df_crm_concat_pf.groupBy('Id')\
        .agg(f.collect_list('Gender').alias('Gender'),
             f.collect_list('Birthday').alias('Birthday'),
             f.collect_list('Job').alias('Job'))\
        .withColumn('Gender', myUdf('Gender'))\
        .withColumn('Birthday', myUdf('Birthday'))\
        .withColumn('Job', myUdf('Job'))
    dyf_crm_concat_pf = DynamicFrame.fromDF(df_crm_concat_pf, glueContext, 'dyf_crm_contacts')
    dyf_crm_concat_pf = mappingForAll(dyf_crm_concat_pf, profile, profile_add_collum)

    if is_dev:
        print('dyf_crm_concat_pf')
        dyf_crm_concat_pf.printSchema()
        dyf_crm_concat_pf.show(10)
    ########  save to s3######
    #parquetToS3(dyf_crm_concat_pf, path="s3://dtsodin/student_behavior/up/user_profile")
    ############################################################

    # save to redshift

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_crm_concat_pf,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "up_user_profile",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_profile/up_user_profile",
                                                   transformation_ctx="datasink4")


    #------------------------------------------------------------------------------------------------------------------#


def etl_user_map(dyf_crm_concats):
    print_is_dev('etl_user_map___*******************************************************************************************')

    # ETL for mapper
    df_crm_concats= dyf_crm_concats.toDF()
    df_crm_concat_clear= df_crm_concats.dropDuplicates(['Id', 'Code'])
    dyf_crm_concat_clear=DynamicFrame.fromDF(df_crm_concat_clear, glueContext, 'dyf_crm_concat_clear')

    dyf_crm_concat_map = RenameField.apply(dyf_crm_concat_clear, "Code", "source_id")
    dyf_crm_concat_map = mappingForAll(dyf_crm_concat_map, mapper, mapper_add_collum)

    if is_dev:
        print('dyf_crm_concat_map')
        dyf_crm_concat_map.printSchema()
        dyf_crm_concat_map.show(10)

    ########  save to s3######
    #parquetToS3(dyf_crm_concat_map,path="s3://dtsodin/student_behavior/up/user_mapper")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_crm_concat_map,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "up_user_map",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_profile/up_user_map",
                                                   transformation_ctx="datasink4")

    ############################################################

    print_is_dev('etl_user_map___********************************************************************************************')
    # ---------------------------------------------------------------------------------------------------------------#
    dyf_adv_student = connectGlue(database='tig_advisor', table_name='student_contact',
                                   select_fields=["contact_id", "student_id"],
                                   fillter=['contact_id', 'student_id'])
    count_adv_student = dyf_adv_student.count()
    if count_adv_student < 1:
        return

    dyf_adv_student.show(10)
    # -----------------------------------------------------------------------------------------------------------------#
    # Start Tranform

    # ETL for phone comunication
    dyf_crm_concat_clear = DynamicFrame.fromDF(df_crm_concat_clear, glueContext, 'dyf_crm_concat_clear')

    dyf_join_contact_student = Join.apply(dyf_adv_student, dyf_crm_concat_clear, 'contact_id', 'Code')
    dyf_adv_student = RenameField.apply(dyf_join_contact_student, "student_id", "source_id")
    mapper_add_collum['source_type']=f.lit(2)
    dyf_adv_student = mappingForAll(dyf_adv_student, mapper,mapper_add_collum)
    dyf_adv_student.show(10)
    ########  save to s3######
    #parquetToS3(dyf_adv_student, path="s3://dtsodin/student_behavior/up/user_mapper")
    ############################################################
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_adv_student,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "up_user_map",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_profile/up_user_map",
                                                   transformation_ctx="datasink4")



def save_communication_redshift(dyf_communication):
    glueContext.write_dynamic_frame.from_jdbc_conf(frame=dyf_communication,
                                                   catalog_connection="glue_redshift",
                                                   connection_options={
                                                       "dbtable": "up_user_communication",
                                                       "database": "transaction_log"
                                                   },
                                                   redshift_tmp_dir="s3n://datashine-dev-redshift-backup/translation_log/user_profile/up_user_communication",
                                                   transformation_ctx="datasink4")



def etl_user_communication(dyf_crm_concats):
    print_is_dev('etl_user_user_communication___****************************************************************************')
    # Cache all data(Extra data)
    # ----------------------------------------------------------------------------------------------------------------#
    dyf_adv_phones = connectGlue(database='tig_advisor', table_name='student_contact_phone',
                                  select_fields=["contact_id", "phone","deleted"],
                                  fillter=['contact_id', 'phone'])
    dyf_adv_phones= RenameField.apply(dyf_adv_phones,"deleted","is_deleted")
    count_adv_phones = dyf_adv_phones.count()
    if count_adv_phones < 1:
        return
    # ----------------------------------------------------------------------------------------------------------------#
    dyf_adv_advisor = connectGlue(database='tig_advisor', table_name='student_contact',
                                  select_fields=["contact_id", "student_id"],
                                  fillter=['contact_id', 'student_id'])
    count_adv_advisor = dyf_adv_advisor.count()
    if count_adv_advisor < 1:
        return
    # ----------------------------------------------------------------------------------------------------------------#
    dyf_toa_enterprise = connectGlue(database='dm_toa', table_name='toa_mapping_student_enterprise',
                                  select_fields=["lms_id", "enterprise_id"],
                                  fillter=['lms_id', 'enterprise_id'])

    count_toa_enterprise = dyf_toa_enterprise.count()
    if count_toa_enterprise < 1:
        return

    dyf_toa_enterprise.printSchema()
    # --------------------------------------------------------------------------------------------------------------#
    # Start Tranform

    print_is_dev("id and code----------------------------------------------------")
    print_is_dev(dyf_crm_concats.count())

    df_crm_concats = dyf_crm_concats.toDF()
    df_concat_enterprise_clear = df_crm_concats.dropDuplicates(['Id', 'Code'])
    dyf_concat_enterprise_clear=DynamicFrame.fromDF(df_concat_enterprise_clear, glueContext, 'dyf_concat_enterprise_clear')

    print_is_dev(dyf_concat_enterprise_clear.count())

    dyf_toa_enterprise = dyf_toa_enterprise.resolveChoice(specs=[('enterprise_id', 'cast:string')])
    dyf_jon_contact_enterprise = Join.apply(dyf_concat_enterprise_clear, dyf_adv_advisor, 'Code', 'contact_id')
    dyf_jon_contact_enterprise = Join.apply(dyf_toa_enterprise, dyf_jon_contact_enterprise, 'lms_id', 'student_id')
    dyf_jon_contact_enterprise = RenameField.apply(dyf_jon_contact_enterprise, "enterprise_id", "comunication")
    comunication_add_collum['communication_type']=f.lit(8)
    dyf_concat_enterprise = mappingForAll(dyf_jon_contact_enterprise, comunication, comunication_add_collum)

    if is_dev:
        print('dyf_concat_enterprise')
        dyf_concat_enterprise.printSchema()
        dyf_concat_enterprise.show(10)
    ########  save to s3######
    # parquetToS3(dyf_concat_enterprise, path="s3://dtsodin/student_behavior/up/user_comunication")

    save_communication_redshift(dyf_concat_enterprise)
    ##########################################

    # ETL for phone comunication
    dyf_jon_contact_phones = Join.apply(dyf_concat_enterprise_clear, dyf_adv_phones, 'Code', 'contact_id')
    dyf_jon_contact_phones = RenameField.apply(dyf_jon_contact_phones, "phone", "comunication")
    comunication_add_collum['communication_type']=f.lit(1)
    dyf_concat_phone = mappingForAll(dyf_jon_contact_phones,comunication,comunication_add_collum_for_phone)

    if is_dev:
        print('dyf_concat_phone')
        dyf_concat_phone.printSchema()
        dyf_concat_phone.show(10)

    ########  save to s3######
    #parquetToS3(dyf_concat_phone, path="s3://dtsodin/student_behavior/up/user_comunication")
    save_communication_redshift(dyf_concat_phone)
    ##########################################


    # ETL for username comunication
    dyf_crm_concat_name = fillterOutNull(dyf_crm_concats, ['Fullname'])
    df_crm_concat_name=dyf_crm_concat_name.toDF()
    df_concat_name_clear = df_crm_concat_name.dropDuplicates(['Id', 'Code','Fullname'])
    dyf_concat_name_clear=DynamicFrame.fromDF(df_concat_name_clear, glueContext, 'dyf_concat_name_clear')
    dyf_crm_concat_name = RenameField.apply(dyf_concat_name_clear, "Fullname", "comunication")
    # set comunication type
    comunication_add_collum['communication_type'] = f.lit(4)

    dyf_concat_name = mappingForAll(dyf_crm_concat_name, comunication, comunication_add_collum)


    if is_dev:
        print('dyf_concat_name')
        dyf_concat_name.printSchema()
        dyf_concat_name.show(10)

    ########  save to s3######
    #parquetToS3(dyf_concat_name, path="s3://dtsodin/student_behavior/up/user_comunication")
    save_communication_redshift(dyf_concat_name)
    ##########################################

    # ETL for email comunication

    dyf_crm_concat_emails = fillterOutNull(dyf_crm_concats, ['Email'])
    df_crm_concat_emails=dyf_crm_concat_emails.toDF()
    df_crm_concat_emails = df_crm_concat_emails.dropDuplicates(['Id', 'Code','Email'])
    dyf_crm_concat_emails=DynamicFrame.fromDF(df_crm_concat_emails, glueContext, 'dyf_crm_concat_emails')
    dyf_crm_concat_email = RenameField.apply(dyf_crm_concat_emails, "Email", "comunication")

    comunication_add_collum['communication_type']=f.lit(2)
    dyf_concat_email = mappingForAll(dyf_crm_concat_email,comunication,comunication_add_collum)

    if is_dev:
        print('dyf_concat_email')
        dyf_concat_email.printSchema()
        dyf_concat_email.show(10)
    ########  save to s3######
    #parquetToS3(dyf_concat_email, path="s3://dtsodin/student_behavior/up/user_comunication")
    save_communication_redshift(dyf_concat_email)
    ##########################################


    # ETL for email2 comunication
    dyf_crm_concat_email_2 = fillterOutNull(dyf_crm_concats, ['Email2'])
    df_crm_concat_email_2=dyf_crm_concat_email_2.toDF()
    df_crm_concat_email_2 = df_crm_concat_email_2.dropDuplicates(['Id', 'Code','Email2'])
    dyf_crm_concat_email_2=DynamicFrame.fromDF(df_crm_concat_email_2, glueContext, 'dyf_crm_concat_email_2')

    dyf_crm_concat_email_2 = RenameField.apply(dyf_crm_concat_email_2, "Email", "comunication")
    comunication_add_collum['is_primary']=f.lit(0)

    dyf_concat_email2 = mappingForAll(dyf_crm_concat_email_2, comunication,comunication_add_collum)

    if is_dev:
        print('dyf_concat_email2')
        dyf_concat_email2.printSchema()
        dyf_concat_email2.show(10)
    ########  save to s3######
    #parquetToS3(dyf_concat_email2, path="s3://dtsodin/student_behavior/up/user_comunication")
    save_communication_redshift(dyf_concat_email2)
    ##########################################


    # ETL for address comunication
    dyf_crm_concat_address = fillterOutNull(dyf_crm_concats, ['Address'])
    df_crm_concat_address=dyf_crm_concat_address.toDF()
    df_crm_concat_address = df_crm_concat_address.dropDuplicates(['Id', 'Code','Address'])
    dyf_crm_concat_address=DynamicFrame.fromDF(df_crm_concat_address, glueContext, 'dyf_crm_concat_address')

    dyf_crm_concat_address = RenameField.apply(dyf_crm_concat_address, "Address", "comunication")
    comunication_add_collum['is_primary'] = f.lit(1)
    comunication_add_collum['communication_type'] = f.lit(6)
    dyf_concat_address = mappingForAll(dyf_crm_concat_address,comunication, comunication_add_collum)

    if is_dev:
        print('dyf_concat_address')
        dyf_concat_address.printSchema()
        dyf_concat_address.show(10)

    ########  save to s3######
    #parquetToS3(dyf_concat_address, path="s3://dtsodin/student_behavior/up/user_comunication")
    save_communication_redshift(dyf_concat_address)
    ##########################################

if __name__ == "__main__":
    main()