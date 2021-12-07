#!/usr/bin/env python
"""
Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType


def check_event_schema():
    """
    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)
    """
    return StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", StringType(), True),
        StructField("offset", StringType(), True)
    ])

#######   define event type   ##########
""
name = 'check'
""
@udf('boolean')
def test(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == name:
        return True
    return False



def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    


    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    check_events = raw_events \
        .filter(test(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          check_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')


    sink = check_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_check_events") \
        .option("path", "/tmp/check_stream_data") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink.awaitTermination()

if __name__ == "__main__":
    main()
