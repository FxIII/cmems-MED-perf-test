#!/usr/bin/env python
import os, sys
sys.path.append("motu-client-python/lib")
import yaml, ftplib
import logging
import logging.config
import utils_log
from motu_api import execute_request
from itertools import cycle 
import json
import datetime
import math
from pprint import pprint 
import re 

def str2Delta(s):
    if s is None:
        return None
    d = re.match(
            r'((?P<days>\d+) days, )?(?P<hours>\d+):'
            r'(?P<minutes>\d+):(?P<seconds>\d+)',
            str(s)).groupdict(0)
    return datetime.timedelta(**dict(( (key, int(value))
                              for key, value in d.items() )))  

class MotuConf:
  def __init__(self):
    self.__dict__ = yaml.load(open("conf.yaml"))
    self.__dict__.update(yaml.load(open("auth.yaml")))
  def update(self,data):
    self.__dict__.update(data)

class FTPConf:
  def __init__(self):
    self.__dict__.update(yaml.load(open("auth.yaml")))
  def update(self,data):
    self.__dict__.update(data)

class Summary:
    def __repr__(self):
       return repr(self.__dict__)
    def __str__(self):
       return str(self.__dict__)
    def __sum__(self,other):
       ret = Summary()
       ret.__dict__.update(self.__dict__)
       for key in ["processing","downloading","total"]:
       	a = getattr(self,key)
       	b = getattr(other,key)
       	setattr(ret,key,a+b)
    def __div__(self,denominator):
       ret = Summary()
       ret.__dict__.update(self.__dict__)
       for key in ["processing","downloading","total"]:
       	a = getattr(self,key)
       	setattr(ret,key,a/denominator)
         
	
class SummaryHandler(logging.Handler):
    fileSizeRE = re.compile("\((.*) B")
    def __init__(self,*a,**k):
        logging.Handler.__init__(self,*a,**k)
	self.summary = Summary()
    @classmethod
    def fileSize2Bytes(cls,fileSize):
      return int(cls.fileSizeRE.search(fileSize).groups()[0])
    def emit(self, record):
        if record.msg.startswith("File size"):
          self.summary.fileSizeStr= record.message
          self.summary.fileSize = self.fileSize2Bytes(record.message)
        if record.msg.startswith("Processing  time"):
          self.summary.processing = str2Delta(record.args[0])
        if record.msg.startswith("Downloading time"):
          self.summary.downloading = str2Delta(record.args[0])
        if record.msg.startswith("Download rate"):
          self.summary.rate = "%s/s"%record.args[0]
        if record.msg.startswith("Total time"):
          self.summary.total = str2Delta(record.args[0])
    def getResults(self):
	ret = self.summary
	self.summary = Summary()
	return ret

def runMotuTest(name, round, data):
   try: 
       os.makedirs(name)
   except OSError:
       if not os.path.isdir(name):
          raise
   target = os.path.join(name,str(round)+".json")
   print "running",name,round
   if os.path.exists(target):
      return
   conf = MotuConf()
   conf.update(data)
   execute_request(conf)
   results = summary.getResults()
   ret = {
      "size": results.fileSize,
      "download": results.downloading.total_seconds(),
      "processing": results.processing.total_seconds(),
      "total": (results.processing + results.downloading).total_seconds(),
   }
   json.dump(ret ,open(target,"w"))

def runFTPTest(host,basepath,files,name,round):
   try: 
       os.makedirs(name)
   except OSError:
       if not os.path.isdir(name):
          raise
   target = os.path.join(name,str(round)+".json")
   print "running",name,round
   if os.path.exists(target):
      return
   conf = FTPConf()
   ftp = ftplib.FTP(host,conf.user,conf.pwd)
   ftp.cwd(basepath)
   size = [0]
   def update(data):
      old = size[0]/1024/1024
      size[0]+=len(data)
      new = size[0]/1024/1024
      if old != new:
        print new
   t = datetime.datetime.now()
   for file in files:
    	ftp.retrbinary('RETR %s'%file, update)
   t= datetime.datetime.now() - t
   ret = {
      "size": size[0],
      "download": t.total_seconds()
   }
   json.dump(ret ,open(target,"w"))

def populateFTP(dataset):
   conf = FTPConf()
   ret = []
   for test in dataset["ftp"]:
      ftp = ftplib.FTP(test["host"],conf.user,conf.pwd)
      ftp.cwd(test["basepath"])
      pool = cycle([ i for i in ftp.nlst() if i.endswith(".nc.gz")])
      for size,name in zip(test["sizes"],test["names"]):
         for round in xrange(10):
         	files = [f for f,i in zip(pool,xrange(size))]
         	ret.append(("runFTPTest",(test["host"],test["basepath"],files,name,round)))
   return ret
def populateMotu(dataset):
    ret =[]
    for test in dataset["motu"]:
        baseconf = {k: test[k] for k in ["service_id","product_id"]}
        def dateRange(range):
            d = range[0]
            while True:
              yield d
              d = d + datetime.timedelta(1)
              if d > range[1]:
                return
        def getStrides(dataSize,span,strideNumber):
          step = (dataSize - span) / (strideNumber - 1)
          return [int(math.floor(step * i)) for i in xrange(strideNumber)]
        dates = list(dateRange(test["date_range"]))
        for size in test["sizes"]:
           span = size["dates"]
           strides = getStrides(len(dates), span, 10)
           for round,stride in enumerate(strides):
             conf = dict(baseconf)
             conf.update(size["conf"])
             conf["date_min"] = str(dates[stride])
             conf["date_max"] = str(dates[stride+span-1])
             ret.append((size["name"],round,conf))
    return ret
                           	
logging.addLevelName(utils_log.TRACE_LEVEL, 'TRACE')
logging.config.fileConfig(  os.path.join(os.path.dirname(__file__),'motu-client-python/etc/log.ini') )
log = logging.getLogger("motu-client-python")
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().handlers[0].stream = sys.stderr

summary = SummaryHandler()
logging.getLogger().addHandler(summary)

dataset = yaml.load(open("dataset.yaml"))

for d in populateMotu(dataset):
  runMotuTest(*d)

for f,d in populateFTP(dataset):
  runFTPTest(*d)

results={ d[7:]: [ json.load(open(os.path.join(d,f))) 
                      for f in files if f.endswith(".json")] 
                   for  d,s,files in os.walk(".") if d.startswith("./test")}

print results
res = {}
for k,vs in results.items():
   ret = dict(vs[0])
   for v in vs[1:]:
      for subkey in v:
          ret[subkey] += v[subkey]
      for subkey in v:
          ret[subkey] /= len(vs)
   print k,ret
   res[k] = ret
json.dump(res, open("results.json","w"),sort_keys=True,indent=4, separators=(',', ': '))
