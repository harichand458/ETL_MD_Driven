from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F


import getpass
username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config('spark.dynamicAllocation.enabled', 'false'). \
    config('spark.ui.port', '0'). \
    config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'). \
    config('spark.jars.packages', 'org.apache.kudu:kudu-spark2_2.11:1.13.0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    appName(f'{username} - Transfer json to kafka topic'). \
    master('yarn'). \
    getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', '2')
spark.conf.set('spark.sql.streaming.schemaInference', 'true')
#kafka_bootstrap_servers = 'w01.itversity.com:9092,w02.itversity.com:9092'
kafka_bootstrap_servers = 'cdp03.itversity.com:9092,cdp04.itversity.com:9092,cdp05.itversity.com:9092'

df = spark. \
  readStream. \
  format('kafka'). \
  option('kafka.bootstrap.servers', kafka_bootstrap_servers). \
  option("startingOffsets", "earliest"). \
  option('subscribe', 'retail_db'). \
  load()
#option('subscribe', f'harikrishna_retail'). \poc_retail_db   retail_db

# df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)"). \
#     select(from_json("value", schema)).alias('kafka_msg'). \
#     writeStream. \
#     format("memory"). \
#     queryName("retail_db"). \
#     start()

# df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)"). \
#     select(from_json("value", schema)).alias('kafka_msg'). \
#     writeStream. \
#     format("console"). \
#     start()

#spark.sql('SELECT * FROM retail_db').show(truncate=False)
# kafka_msg  =spark.sql('SELECT * FROM retail_db')
# print(kafka_msg.printSchema())
#spark.read.json(kafka_msg.rdd).show(truncate=False)

def foreach_batch_function(df, epoch_id):
    #print(df.printSchema())
    kafka_df = df.select(F.col("jsontostructs(value).*"))
    #print(kafka_df.show())
    # iterating through the list of distinct table names
    for table in kafka_df.select('table_name').distinct().toLocalIterator():
        records = kafka_df. filter(f'table_name = "{table.table_name}"').select('record')
        # reading RDD format data to show it as dataframe
        transformed_df = spark.read.json(records.select('record').rdd.map(lambda item: item.record))
        print(transformed_df.show())
        des_table = spark.read.format('org.apache.kudu.spark.kudu')\
            .option('kudu.master', "cdp01.itversity.com:7051") \
            .option('kudu.table', f'retail_db.{table.table_name}').load()
        dtypes_casted = list(
            map(lambda dtype: f'cast(trim({dtype[0]}) as {dtype[1]}) {dtype[0]}', des_table.dtypes))
        dep_table = transformed_df.withColumn('last_updated_ts', F.current_timestamp()) \
            .selectExpr(dtypes_casted[0:len(dtypes_casted)])
        dep_table.write.format('org.apache.kudu.spark.kudu') \
            .option('kudu.master', "cdp01.itversity.com:7051") \
            .option('kudu.table', f'retail_db.{table.table_name}').mode("append").save()
    pass

schema = StructType().add('table_name', StringType(), False).add('record', StringType(), False)
query= df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)"). \
    select(from_json("value", schema)).alias('kafka_msg'). \
    writeStream. \
    foreachBatch(foreach_batch_function).start()

query.awaitTermination()



