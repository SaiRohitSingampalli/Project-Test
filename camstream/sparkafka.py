import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
if __name__ == "__main__":

    sc = SparkContext(appName="StreamingErrorCount")
    ssc = StreamingContext(sc, 1)
    kvs = KafkaUtils.createStream(ssc,"localhost:5000","mystream",{"kafka_video":1}) 

    
    kvs.pprint()
   
    ssc.start()
    ssc.awaitTermination()
