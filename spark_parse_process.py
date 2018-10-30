import os,sys
import yaml
import argparse
import multiprocessing
from pyspark import SparkContext,SparkConf,SparkFiles,StorageLevel
from pyspark.streaming import StreamingContext
import re
import inspect as _inspect
#from elasticsearch import Elasticsearch,helpers
#import pygeohash as pgh
#import geoip2.database
#import shlex
#import logging

'''Author: Amine Boukhtouta 23-10-2018'''
'''python spark_parse_process.py -p logs_streaming -c config.yaml -o output''' #Running command
'''nohup python2.7 spark_parse_process.py -p logs_streaming -c config.yaml -o output &''' #Running the script in the background
'''spark-submit --master local[*] --jars jars/elasticsearch-hadoop-5.4.0.jar ./code/spark_parse.py''' #For hadoop: stills bogus


#important classes to check mapping function on RDDs

class _Try(object): pass    

class Failure(_Try):
    def __init__(self, e):
        if Exception not in _inspect.getmro(e.__class__):
            msg = "Invalid type for Failure: {0}"
            raise TypeError(msg.format(e.__class__))
        self._e = e
        self.isSuccess =  False
        self.isFailure = True

    def get(self): raise self._e

    def __repr__(self):
        return "Failure({0})".format(repr(self._e))

class Success(_Try):
    def __init__(self, v):
        self._v = v
        self.isSuccess = True
        self.isFailure = False

    def get(self): return self._v

    def __repr__(self):
        return "Success({0})".format(repr(self._v))

def Try(f, *args, **kwargs):
    try:
        return Success(f(*args, **kwargs))
    except Exception as e:
        return Failure(e)

def resource(filename):
	cwd = os.path.dirname(os.path.realpath(__file__))
	return os.path.join(cwd, filename)

#TCP SYn 6 |..| 0X02
#Per IP: sorted scanned ports (ascending), sum nbr packets, total nbr unique destinations, nbr unique source ports, nbr active intervals, avg IP length 	
def get_data(data):
	if not data.isEmpty():
		s = data.toDebugString()
		for m in re.finditer(rg1,s):
			path=s[m.start():m.end()]
		path=path[path.rfind(os.sep)+1:]
		path=path.replace(".txt",".csv")
		data=data.persist(StorageLevel.MEMORY_AND_DISK)
		values = data.filter(lambda x: x!=header).map(lambda x: Try(parse_trace,x.strip())).filter(lambda x: x.isSuccess).map(lambda x: x.get())
		# Mappings with by indexing src_IPs
		maps_src_dst_IPs_count=sc.parallelize(map(lambda(a,b): (a, len(set(b))),sorted(values.map(lambda x: (x[0],x[1])).groupByKey().collect())))
		maps_src_source_ports_count=sc.parallelize(map(lambda(a,b): (a, len(set(b))),sorted(values.map(lambda x: (x[0],x[2])).groupByKey().collect())))
		maps_src_dst_ports=sc.parallelize(map(lambda(a,b): (a, sorted(set(b))),sorted(values.map(lambda x: (x[0],int(x[3]))).groupByKey().collect())))
		maps_src_ttl_avg=sc.parallelize(map(lambda(a,b): (a, reduce(lambda x, y: x + y, list(b)) / len(list(b))),sorted(values.map(lambda x: (x[0],int(x[4]))).groupByKey().collect())))
		maps_src_ttl_max=sc.parallelize(map(lambda(a,b): (a, max(list(b))),sorted(values.map(lambda x: (x[0],int(x[4]))).groupByKey().collect())))
		maps_src_ttl_min=sc.parallelize(map(lambda(a,b): (a, min(list(b))),sorted(values.map(lambda x: (x[0],int(x[4]))).groupByKey().collect())))
		maps_src_ip_length_avg=sc.parallelize(map(lambda(a,b): (a, reduce(lambda x, y: x + y, list(b)) / len(list(b))),sorted(values.map(lambda x: (x[0],int(x[5]))).groupByKey().collect())))
		maps_src_packets_sum=sc.parallelize(map(lambda(a,b): (a,sum(b)),sorted(values.map(lambda x: (x[0],int(x[6]))).groupByKey().collect())))
		maps_src_intervals=sc.parallelize(map(lambda(a,b): (a, len(set(b))),sorted(values.map(lambda x: (x[0],x[7])).groupByKey().collect())))
		
		group=sorted(maps_src_dst_IPs_count.groupWith(maps_src_source_ports_count,maps_src_dst_ports,maps_src_ttl_avg,maps_src_ttl_max,maps_src_ttl_min,maps_src_ip_length_avg,maps_src_packets_sum,maps_src_intervals).collect())
		
		col=map(lambda (x,y): (x, (list(y[0]), list(y[1]), list(y[2]), list(y[3]), list(y[4]), list(y[5]), list(y[6]), list(y[7]), list(y[8]))),group)
		#for item in col:
			#print item
			#sys.exit(1)
		res=convert_json_bulk(col)
		with open(resource(args.out)+os.sep+path, "wb") as f:
			f.write('[\n')
			cpt=1
			length=len(res)
			for item in res:
				line=str(item)
				if cpt!=length:
					line=line+",\n"
				else:
					line=line+"\n"
				f.write(line)
			f.write(']')
		
def convert_json_bulk(col):
	l=[]
	for item in col:
		obj={
			'ip': item[0],
			'unique_dst_ips': item[1][0][0],
			'unique_src_ports': item[1][1][0],
			'dst_ports': item[1][2][0],
			'ttl_avg': item[1][3][0],
			'ttl_max': item[1][4][0],
			'ttl_min': item[1][5][0],
			'ip_length_avg': item[1][6][0],
			'sum_packets': item[1][7][0],
			'active_intervals': item[1][8][0]
		}
		rec={'_index':'features_index', '_type': 'stats_aggregates', '_source':obj} 
		l.append(rec)
	return l
		

#Function to parse a line in Web log trace
def parse_trace(line):
	el=re.split(r'\t+',line)
	return el[0],int(el[1]),int(el[2]),int(el[3]),int(el[4]),int(el[5]),int(el[6]),int(el[7])
	
		
def main():
	os.environ["PYSPARK_PYTHON"]="/usr/bin/python2.7"
	parser = argparse.ArgumentParser(description='Log Parser')
	parser.add_argument('-p','--path', nargs='?', default ='./logs_streaming', help='path to streaming folder')
	parser.add_argument('-c','--conf', nargs='?', default ='config.yaml', help='path to config file')
	parser.add_argument('-o','--out', nargs='?', default ='./output', help='path to output folder')
	r1='((?:\\/[\\w\\.\\-]+)+)'	# Unix Path pattern
	#parser.add_argument('-f','--fs', nargs='?', default ='hadoop', choices=['hadoop', 'pyelastic'], help='Insertion through Hadoop API or ES')

	global rg1
	global header
	global sc
	global args

	rg1 = re.compile(r1,re.IGNORECASE|re.DOTALL)
	args = parser.parse_args()
	header="src-IP\tanon-dst-IP	src-port\tdst-port\tTTL\tIP-Length\tPackets\tminute"
	
	with open(resource(args.conf), 'r') as f:
		try:
			cfg=yaml.load(f)
			conf=SparkConf()
			#if args.fs == 'pyelastic':
			conf.setMaster(cfg['spark']['master']) 
			conf.setAppName(cfg['spark']['name'])
			if cfg['spark']['cpus']=="-1":
				conf.set("spark.cores.max", str(multiprocessing.cpu_count())) #Full CPUs
			else:
				conf.set("spark.cores.max", cfg['spark']['cpus']) #Configured number of CPUs
			conf.set('spark.driver.memory','10g')
			sc = SparkContext(conf=conf)
			ssc = StreamingContext(sc, cfg['spark']['interval'])
			stream = ssc.textFileStream(resource(args.path))
			stream.foreachRDD(lambda d: get_data(d))
			ssc.start()             # Start the computation
			ssc.awaitTermination()  # Wait for the computation to terminate
		
		except yaml.YAMLError as exc:
			print(exc)
	
if __name__=="__main__":
	main()