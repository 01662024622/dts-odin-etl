import sys

from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import concat_ws
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import udf


def main():
    def get_all_answer(answer_a,answer_b,answer_c,answer_d,correct_answer):
        if answer_a is None:
            answer_a = ''
        if answer_b is None:
            answer_b = ''
        if answer_c is None:
            answer_c = ''
        if answer_d is None:
            answer_d = ''
        if correct_answer is None:
            correct_answer = ''

        return answer_a +","+ answer_b +","+ answer_c +","+ answer_d +","+ correct_answer

    udf_all_answer = udf(get_all_answer, StringType())



    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session

    dyf_weekly_question_native_test = glueContext.create_dynamic_frame.from_catalog(
                                database="moodle",
                                table_name="weekly_question_native_test"
    )
    dyf_weekly_question_native_test = dyf_weekly_question_native_test.select_fields(['question_code', 'correct_answer',
                                                                                     'answer_a', 'answer_b',
                                                                                     'answer_c', 'answer_d',
                                                                                     'lo', 'lc']
                                                                                    )

    df_weekly_question_native_test = dyf_weekly_question_native_test.toDF()
    # df_weekly_question_native_test = df_weekly_question_native_test.drop_duplicates()
    df_weekly_question_native_test = df_weekly_question_native_test.withColumn(
        "all_answer", udf_all_answer(df_weekly_question_native_test["answer_a"],
                                     df_weekly_question_native_test["answer_b"],
                                     df_weekly_question_native_test["answer_c"],
                                     df_weekly_question_native_test["answer_d"],
                                     df_weekly_question_native_test["correct_answer"]
                                     )
    )
    print('df_weekly_question_native_test count')
    print(df_weekly_question_native_test.count())
    df_weekly_question_native_test.cache()



    #######dyf_top_quesntion_answers
    dyf_top_question_answers =glueContext.create_dynamic_frame.from_catalog(
        database='moodle',
        table_name='top_question_answers'
    )

    dyf_top_question_answers = dyf_top_question_answers.select_fields(
        ['question', 'answer','fraction'])

    dyf_top_answers = Filter.apply(frame=dyf_top_question_answers, f=lambda x: x["fraction"] == '1.0000000')

    df_top_question_answers = dyf_top_question_answers.toDF()
    df_top_question_answers.cache()
    df_top_question_answers = df_top_question_answers.drop('fraction')

    df_top_answers = dyf_top_answers.toDF()
    df_top_answers = df_top_answers.drop('fraction')
    df_top_answers.cache()
    # df_top_answers.printSchema()
    # df_top_answers.show(10)
    df_concat = df_top_answers.union(df_top_question_answers)




    df_top_question_answer = df_concat.groupby('question').agg(collect_list('answer').alias("answer_list"))
    df_top_question_answer = df_top_question_answer.withColumn("answer_s", concat_ws(",", "answer_list"))
    df_top_question_answer.cache()
    df_top_question_answer = df_top_question_answer.drop('answer_list')
    df_top_question_answer = df_top_question_answer.drop_duplicates()

    print(df_top_question_answer.count())
    # df_top_question_answer.printSchema()
    # df_top_question_answer.show()

    # #########dyf_top_question
    dyf_top_question =glueContext.create_dynamic_frame.from_catalog(
        database='moodle',
        table_name='top_question'
    )

    dyf_top_question = dyf_top_question.select_fields(['id','category'])
    df_top_question=dyf_top_question.toDF()

    df_top_question.cache()


    ########dyf_dyf_top_question_categories
    dyf_top_question_categories = glueContext.create_dynamic_frame.from_catalog(
        database='moodle',
        table_name='top_question_categories'
    )

    dyf_top_question_categories.printSchema()
    print(dyf_top_question_categories.count())
    dyf_top_question_categories.show(10)
    arrName = ['G01','G02','G03','G04','G05']
    dyf_top_question_categories = dyf_top_question_categories.select_fields(['id','name']).rename_field('id', 'categories_id')
    dyf_top_question_categories = Filter.apply(frame = dyf_top_question_categories,f=lambda x:x['name'] in arrName)

    df_top_question_categories = dyf_top_question_categories.toDF()
    df_top_question_categories.cache()

    ###########################_____JOIN_______ top_question voi top_question_categories
    print('join top_question voi top_question_categories')
    df_join_1 = df_top_question_categories.join(df_top_question,
                                                df_top_question_categories.categories_id == df_top_question.category)

    print(df_join_1.count())
    df_join_1.printSchema()
    df_join_1.show(10)

    #########################________JOIN________ df_join_1 voi df_top_question_answer de lay answer_s
    df_join_category_question_answers = df_join_1.join(df_top_question_answer,
                               df_join_1.id == df_top_question_answer.question)
    print('df_join_category_question_answers')
    # print('df_join_category_question_answers count before drop', df_join_category_question_answers.count())
    # df_top_question = df_top_question.drop_duplicates()
    # print('df_join_category_question_answers count after drop', df_join_category_question_answers.count())
    df_join_category_question_answers.printSchema()
    df_join_category_question_answers.show()
    # df_join_top_question_answers = df_join_top_question_answers.drop_duplicates()

    #########################________JOIN_________ df_weekly_question_native_test voi df_join_category_question_answers de lay lo lc

    print('join df_weekly_question_native_test voi df_join_category_question_answers de lay lo lc')

    print('df_weekly_question_native_test::before::join::number: ', df_weekly_question_native_test.count())
    print('df_join_category_question_answers::before::join::number: ', df_join_category_question_answers.count())

    df_join_c_q_a_w = df_join_category_question_answers.join(df_weekly_question_native_test,
                               f.encode(df_weekly_question_native_test.all_answer, 'UTF-8')
                                                             == f.encode(df_join_category_question_answers.answer_s, 'UTF-8')
    )


    print('df_weekly_question_native_test::after::join::number: ', df_weekly_question_native_test.count())
    print('df_join_category_question_answers::after::join::number: ', df_join_category_question_answers.count())
    df_join_c_q_a_w.printSchema()
    print(df_join_c_q_a_w.count())
    df_join_c_q_a_w.show(10)

    dyf_join_c_q_a_w = DynamicFrame.fromDF(df_join_c_q_a_w, glueContext, "dyf_join_c_q_a_w")
    dyf_join_c_q_a_w = Filter.apply(frame = dyf_join_c_q_a_w,f=lambda x:x['lo'] is not None
                                                                        and x['lc'] is not None
                                                                        and x['lo'] != ''
                                                                        and x['lc'] != '')

    if (dyf_join_c_q_a_w.count() > 0):
        apply_mapping = ApplyMapping.apply(frame=dyf_join_c_q_a_w,
                                           mappings=
                                           [("question", "bigint", "question", "long"),
                                            ("lo", "string", "lo", "string"),
                                            ("lc", "string", "lc", "string")
                                            ])

        resolve_choice= ResolveChoice.apply(frame = apply_mapping, choice ="make_cols",
                                                                         transformation_ctx="resolvechoice2")

        resolve_choice.printSchema()
        resolve_choice.show()
        dropnullfields = DropNullFields.apply(frame=resolve_choice,transformation_ctx="dropnullfields")

        df_dropnullfields = dropnullfields.toDF()
        df_dropnullfields = df_dropnullfields.drop_duplicates()
        print(df_dropnullfields.count())
        df_dropnullfields.show()
        dyf_dropnullfields = DynamicFrame.fromDF(df_dropnullfields, glueContext, "dyf_dropnullfields")

    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf_dropnullfields,
                                                                                      catalog_connection="glue_redshift",
                                                                                      connection_options={
                                                                                          "dbtable": "mapping_grammar_lo",
                                                                                          "database": "dts_odin"
                                                                                      },
                                                                                      redshift_tmp_dir="s3a://dts-odin/mapping_grammar_lo_v01/",
                                                                                      transformation_ctx="datasink")

    print('START WRITE TO S3-------------------------')

    datasink1 = glueContext.write_dynamic_frame.from_options(frame=dyf_dropnullfields, connection_type="s3",
                                                             connection_options={
                                                                 "path": "s3://dts-odin/moodle/mapping_grammar_lo/"},
                                                             format="parquet",
                                                             transformation_ctx="datasink1")
    print('END WRITE TO S3-------------------------')

    # # lay max _key tren datasource
    # df_temp = df_join_top_question_answer.toDF()
    # flag = df_temp.agg({"_key": "max"}).collect()[0][0]
    # flag_data = [flag]
    # df = spark.createDataFrame(flag_data, "long").toDF('flag')
    #
    # # ghi de flag moi vao s3
    # df.write.parquet("s3a://dts-odin/flag/", mode="overwrite")

    df_weekly_question_native_test.unpersist()
    df_top_question.unpersist()
    df_top_question_categories.unpersist()
    df_top_question_answers.unpersist()
    df_top_question_answer.unpersist()

    df_top_answers.unpersist()
if __name__ == "__main__":
    main()