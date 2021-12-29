import getpass
import data_generator
import os
import util
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def main():
    configs = util.get_config()
    data_generator.generate_lms_data()

    util.copy_local_to_hdfs()

    username = getpass.getuser()
    spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        appName(f'{username} | Python - Data Processing - Overview'). \
        master('yarn'). \
        getOrCreate()

    u_char = configs['file.delimiter'].encode('utf8').decode('unicode-escape')
    for table_name in os.listdir(configs['input.base.dir']):
        csv_df = spark.read.csv(configs['hdfs.base.dir'] + '/lms/' + table_name + '/', sep=u_char, header=True)

        csv_df = csv_df.select(
            *(f.when(f.col(c) == '\\N', None).otherwise(f.col(c)).alias(c) for c in csv_df.columns))

        spark_df = spark.read.table(f"{configs['hive.db']}.{table_name}")

        dtypes_casted = list(
            map(lambda dtype: f' trim(cast({dtype[0]} as {dtype[1]})) as {dtype[0]} ', spark_df.dtypes))

        csv_df = csv_df.selectExpr(dtypes_casted[0:len(dtypes_casted)])
        spark.sql(f"truncate table {configs['hive.db']}.{table_name}")

        csv_df.write.format("hive"). \
            saveAsTable(f"{configs['hive.db']}.{table_name}", mode="append")


if __name__ == "__main__":
    main()
