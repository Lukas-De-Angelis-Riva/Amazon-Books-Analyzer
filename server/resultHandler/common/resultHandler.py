from multiprocessing import Process, Lock
from common.resultReceiver import ResultReceiver
#from common.resultSender import ResultSender

class ResultHandler():
    def __init__(self, config_params):
        self.lock = Lock()
        self.resultReceiver = ResultReceiver(config_params['file_name'], self.lock)
        #self.resultSender = ResultSender(config_params['ip'], config_params['port'], config_params['file_name'], self.lock)
  
    def run(self):
        p1 = Process(target=self.resultReceiver.run, args=())
        #p2 = Process(target=self.resultSender.run, args=())
        p1.start(); #p2.start()
        p1.join() ; #p2.join()