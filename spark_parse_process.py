import os,sys
import yaml
import argparse
import multiprocessing
from pyspark import SparkContext,SparkConf,SparkFiles,StorageLevel
from pyspark.streaming import StreamingContext
#from elasticsearch import Elasticsearch,helpers
#import pygeohash as pgh
#import geoip2.database
#import shlex
#import re
import inspect as _inspect
#import logging

'''Author: Amine Boukhtouta 23-10-2018'''
'''python spark_parse_corsaro.py -p ./logs_streaming -c config.yaml '''
'''spark-submit --master local[*] --jars jars/elasticsearch-hadoop-5.4.0.jar ./code/spark_parse.py''' # for hadoop, stills bugy


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

def get_data(data):
	if not data.isEmpty():
		index=data.zipWithIndex()
		l=index.collect()
		for item in l:
			print l
		
# def get_data(data):
	# if not data.isEmpty():
		# data=data.persist(StorageLevel.MEMORY_AND_DISK)
		# #data.cache()
		# filenames=[]
		# #index_col={}
		# s = data.toDebugString()# get filenames out of debut strings
		# for m in re.finditer(rg1,s):
			# path=s[m.start():m.end()]
			# path=path[path.find('ACCESS'):]
			# print path
			# filenames.append(path) #init structure
			
		# if filenames!=[]:
			# d=[]
			# c=data.count()
			# index=data.zipWithIndex()
			# headers=index.filter(lambda x: x[0]==header or x[1]==c-1).map(lambda x: x[1]).collect() #get headers positions
			# #print headers
			# col=zip(headers[0::1],headers[1::1])
			# for i in range(0,len(col)):
				# first_line=index.filter(lambda x: x[1]==col[i][0]+1).map(lambda x: x[0]).first()
				# if i!=len(col)-1:
					# last_line=index.filter(lambda x: x[1]==col[i][1]-1).map(lambda x: x[0]).first()
				# else:
					# last_line=index.filter(lambda x: x[1]==col[i][1]).map(lambda x: x[0]).first()
				# obj={'File':filenames[i],
					# 'First':rg2.search(first_line).group(1).encode('utf-8'),
					# 'Last':rg2.search(last_line).group(1).encode('utf-8')
				# }
				# if args.fs=='pyelastic':
					# #d.append({'_op_type': 'update','_index': 'mdn_afl', '_type': 'logs_files', '_id': 'ctx._id', '_source': { '_script': {'inline': "if ((ctx._source.doc) && (ctx._source.doc.File != doc.File)) { ctx.op='noop' } else { ctx._source.doc=doc }",'params': { 'doc': obj },'_upsert': { 'doc': obj }}}})
					# d.append({'_index': 'mdn_afl_files', '_type': 'logs_files', '_source': obj})
				# if 	args.fs=='hadoop':
					# d.append(obj)
			# if args.fs=='pyelastic':
				# helpers.bulk(es,d)
			# if args.fs=='hadoop':
				# r=sc.parallelize(d).map(lambda x: ('key',x))
				# insert_es(r,0)
				
		# if args.fs=='pyelastic':
			# #docs=data.filter(lambda x: x != header).map(lambda x: {'_index': 'mdn_afl_collections', '_type': 'logs_collection', '_source': parse_web_trace(x.strip())})
			# docs=data.filter(lambda x: x != header).map(lambda x: Try(parse_web_trace,x.strip())).filter(lambda x: x.isSuccess).map(lambda x: x.get())
			# ips=docs.filter(lambda x: x!=None).map(lambda x: x['IP']).distinct().map(lambda x: {'_index': 'mdn_afl_ips', '_type': 'logs_ips', '_source':Ip2city(x)})
			# cols=docs.map(lambda x: {'_index': 'mdn_afl_collections', '_type': 'logs_collection', '_source': x})
			# helpers.bulk(es,ips.collect())
			# helpers.bulk(es,cols.collect())
			
			
		# if args.fs=='hadoop':
			# docs=data.filter(lambda x: x != header).map(lambda x: ('key',parse_web_trace(x.strip())))
			# insert_es(docs,1)
			# # get dinstinct IP
			# ips=docs.map(lambda x:x['IP']).distinct().map(lambda x: ('key',Ip2city(x)))
			# insert_es(ips,2)
	

	

#Function to parse a line in Web log trace
def parse_trace(line):

	return 0
	
		
def main():
	os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python2.7"
	parser = argparse.ArgumentParser(description='Log Parser')
	parser.add_argument('-p','--path', nargs='?', default ='./logs_streaming', help='path to streaming folder')
	parser.add_argument('-c','--conf', nargs='?', default ='config.yaml', help='path to config file')
	#parser.add_argument('-f','--fs', nargs='?', default ='hadoop', choices=['hadoop', 'pyelastic'], help='Insertion through Hadoop API or ES')

	
	#global rg1
	#global rg2
	
	#global es_write_conf0
	#global es_write_conf1
	#global es_write_conf2
	#global header
	#global reader
	global sc
	#global es
	global args
	#global geoDBpath
	#global logger
	#rg1 = re.compile(r1,re.IGNORECASE|re.DOTALL)
	#rg2 = re.compile(r2,re.IGNORECASE|re.DOTALL)
	args = parser.parse_args()

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

#TCP SYn 6 |..| 0X02
#per IP: sorted scanned ports (ascending), sum nbr packets, total nbr unique destinations, nbr unique source ports, nbr active intervals, avg IP length,  		
if __name__=="__main__":
	main()