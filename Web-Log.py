from __future__ import print_function
from pyspark import SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import time
from datetime import datetime
import argparse
import re

def load_row(line):
    pattern = '(?:[\d]{1,3})\.(?:[\d]{1,3})\.(?:[\d]{1,3})\.(?:[\d]{1,3})'
    for word in line:
        if not word:
            continue
        else:
            match = re.search(pattern, str(word))
        ipaddress = match.group(0)
        return (ipaddress, 1)
    
def update_frequency(new_entry, running_count):
  if not running_count:
    running_count = 0
  return sum(new_entry,running_count)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('-rh', '--host', default="localhost:9092")
  parser.add_argument('-t', '--topic', default="weblogs")
  args = parser.parse_args()
  print('Starting the process...\n')
  start=datetime.now()
  sc = SparkContext(appName="DetectDDoS")
  ssc = StreamingContext(sc, 10)
  ssc.checkpoint('checkpoint')
  kvs = KafkaUtils.createDirectStream(ssc, [args.topic], {"metadata.broker.list": args.host})	
  parsed = kvs.map(load_row)  
  updated = parsed.updateStateByKey(update_frequency)
  updated2 = updated.map(lambda (k,v): (str(k), v))
  #criteria to detect DDOS
  high_freq = updated2.filter(lambda (k,v): v >= 50)
  high_freq.pprint()
  #high_freq.saveAsTextFiles('DDOS_attacker_found_output')
  ssc.start()
  time.sleep(60)
  ssc.stop()
  ssc.awaitTermination()
  print('Process ended.')
  print('Time taken:', datetime.now()-start)
