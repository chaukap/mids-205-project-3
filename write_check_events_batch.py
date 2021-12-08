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
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load() 

    check_events = raw_events \
        .select(raw_events.value.cast('string').alias('stats'),\
                raw_events.timestamp.cast('string'))\
        .filter(test('stats'))


    extracted_check_events = check_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.stats))) \
        .toDF()

    extracted_check_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/check_cell')

    print('WRITE CHECK EVENTS TO PARQUET COMPLETE!')

if __name__ == "__main__":
    main()
