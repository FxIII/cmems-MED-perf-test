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
    def __init__(self,*a,**k):
        logging.Handler.__init__(self,*a,**k)
	self.summary = Summary()
    def emit(self, record):
        if record.msg.startswith("File size"):
          self.summary.fileSize= record.message
        if record.msg.startswith("Processing  time"):
          self.summary.processing = record.args[0]
        if record.msg.startswith("Downloading time"):
          self.summary.downloading = record.args[0]
        if record.msg.startswith("Download rate"):
          self.summary.rate = "%s/s"%record.args[0]
        if record.msg.startswith("Total time"):
          self.summary.total = record.args[0]
    def getResults(self):
	ret = self.summary
	self.summary = Summary()
	return ret

def runMotuTest(data):
   conf = MotuConf()
   conf.update(data)
   execute_request(conf)
   return summary.getResults()

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
      "size": size,
      "download": t.total_second()
   }
   print  "\n",ret
   json.dump(ret ,open(target,"w"))

def populateFTP(dataset):
   conf = FTPConf()
   ret = []
   for test in dataset["ftp"]:
      print test["host"],test
      ftp = ftplib.FTP(test["host"],conf.user,conf.pwd)
      ftp.cwd(test["basepath"])
      pool = cycle([ i for i in ftp.nlst() if i.endswith(".nc.gz")])
      for size,name in zip(test["sizes"],test["names"]):
         for round in xrange(10):
         	files = [f for f,i in zip(pool,xrange(size))]
         	ret.append(("runFTPTest",(test["host"],test["basepath"],files,name,round)))
         	for f in files:
         	  print name, round, f
   return ret
   
logging.addLevelName(utils_log.TRACE_LEVEL, 'TRACE')
logging.config.fileConfig(  os.path.join(os.path.dirname(__file__),'motu-client-python/etc/log.ini') )
log = logging.getLogger("motu-client-python")
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().handlers[0].stream = sys.stderr

summary = SummaryHandler()
logging.getLogger().addHandler(summary)

dataset = yaml.load(open("dataset.yaml"))

ret = populateFTP(dataset)
runFTPTest(*ret[0][1])
print runFTPTest({})
