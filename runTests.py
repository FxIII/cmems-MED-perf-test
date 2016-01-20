#!/usr/bin/env python
import os, sys
sys.path.append("motu-client-python/lib")
import yaml 
import logging
import logging.config
import utils_log
from motu_api import execute_request

class Conf:
  def __init__(self):
    self.__dict__ = yaml.load(open("conf.yaml"))
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

def runTest(data):
   conf = Conf()
   conf.update(data)
   execute_request(conf)
   return summary.getResults()
         
logging.addLevelName(utils_log.TRACE_LEVEL, 'TRACE')
logging.config.fileConfig(  os.path.join(os.path.dirname(__file__),'motu-client-python/etc/log.ini') )
log = logging.getLogger("motu-client-python")
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().handlers[0].stream = sys.stderr

summary = SummaryHandler()
logging.getLogger().addHandler(summary)

print runTest({})