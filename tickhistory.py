
import collections
import logging
import time
import os.path
import argparse
import datetime
import inspect ,traceback
from random import randint
import csv
import subprocess
import sys, getopt
from ibapi.utils import iswrapper
from ibapi.common import * 
from ibapi.contract import * 
from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper
from ibapi.scanner import ScanData


def clear():
    if os.name in ('nt','dos'):
        subprocess.call("cls")
    elif os.name in ('linux','osx','posix'):
        subprocess.call("clear")
    else:
        print("\n") * 120


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")
    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")
    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'
    logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
                        filemode="w",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)


def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)
    return fn2


def printinstance(self,inst:Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))


class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsdId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass


# ! [socket_declare]
class VukClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(VukClient, methName, self.countReqId(methName, meth))

                # print("TestClient.clntMeth2reqIdIdx", self.clntMeth2reqIdIdx)


# ! [ewrapperimpl]
class VukWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(VukWrapper, methName, self.countWrapReqId(methName, meth))


class TickHistory(VukWrapper, VukClient):

    HDATE=""#"20190131"
    SYMBOL=""#"DLF"
    IP=""
    PORT=4001
    SECTYPE=""
    STRIKE=""
    RIGHT=""
    EXPIRY=""
    contract = Contract()

    def __init__(self):
        VukWrapper.__init__(self)
        VukClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None
    
    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))


    @iswrapper
    # ! [historicaltickslast]
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        self.fulldaydata(ticks)
    # ! [historicaltickslast]

    def fulldaydata(self,ticks):
        try:
            with open(self.SYMBOL+"_"+self.SECTYPE+"_"+str(self.HDATE)+".csv", 'a', newline='') as csvfile:
                filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
                if (len(ticks) >=1000):
                    i = 0
                    for tick in ticks:
                        #print(str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time))))+","+str(tick.price)+","+str(tick.size))
                        filewriter.writerow([str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time)))), str(tick.price),str(tick.size)])
                        i=i+1
                        if (i==1000):
                            self.reqHistoricalTicks(randint(10, 999), self.contract,
                                    str(self.HDATE)+" "+str(time.strftime("%H:%M:%S", time.localtime(int(tick.time)))), "", 1000, "TRADES", 1, True, [])
                            break
                else:
                    for tick in ticks:
                        #print(str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time))))+","+str(tick.price)+","+str(tick.size))
                        filewriter.writerow([str(time.strftime("%D %H:%M:%S", time.localtime(int(tick.time)))), str(tick.price),str(tick.size)])
                        
        except Exception as e:
            logging.error(traceback.format_exc())
            print(traceback.format_exc())
        finally:
            self.dumpTestCoverageSituation()
            self.dumpReqAnsErrSituation()

    @iswrapper
    # ! [currenttime]
    def currentTime(self, time:int):
        super().currentTime(time)
        print("CurrentTime:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"))
    # ! [currenttime]

    
def main():
    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    cmdLineParser = argparse.ArgumentParser("Vuk History Data Bot :")
    cmdLineParser.add_argument("-ip", "--ip", action="store", type=str,
                            dest="ip", default="127.0.0.1", help="The IP to get IB Gateway connection")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                            dest="port", default=4002, help="The TCP port to use For eg: 1122")
    cmdLineParser.add_argument("-s", "--symbol", action="store", type=str,
                            dest="symbol", default="INFY",
                            help="Instrument Symbol For eg: INFY ")
    cmdLineParser.add_argument("-d", "--date", action="store", type=str,
                            dest="date", default="20190131",
                            help="Date (yyyymmdd) For eg: 20190131")
    cmdLineParser.add_argument("-st", "--sectype", action="store", type=str,
                            dest="sectype", default="STK",
                            help="Security Type For eg: 'STK','FUT','OPT'")
    cmdLineParser.add_argument("-e", "--expiry", action="store", type=str,
                            dest="expiry", default="",
                            help="Expiry Date For eg: FUT-201903, OPT-20190315")
    cmdLineParser.add_argument("-sp", "--strike", action="store", type=str,
                            dest="strike", default="",
                            help="Option Strike Price For eg: 11222.50")
    cmdLineParser.add_argument("-r", "--right", action="store", type=str,
                            dest="right", default="",
                            help="Option Rights For eg: C or P")
    args = cmdLineParser.parse_args()
    from ibapi import utils
    Contract.__setattr__ = utils.setattr_log
    DeltaNeutralContract.__setattr__ = utils.setattr_log

    try:
        app = TickHistory()
        app.SYMBOL = args.symbol
        app.HDATE = args.date
        app.IP = args.ip
        app.PORT = args.port
        app.SECTYPE = args.sectype
        print("Using args", args)
        logging.debug("Using args %s", args)
        app.connect(app.IP, app.PORT, clientId=1)
        clear()
        print("\n")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("~~~~~~~~~ Vuk.ai Start Collecting High Resolution Historical Data ~~~~~~~~~")
        print("IB Gateway Time:%s connectionTime:%s" % (app.serverVersion(),
                                                    app.twsConnectionTime()))
        print("Symbol : "+app.SYMBOL )
        print("Date : "+str(app.HDATE) )
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print("\n")
        app.contract.symbol = app.SYMBOL
        app.contract.currency = "INR"  
        app.contract.exchange = "NSE"
        if(app.SECTYPE == "STK"):
            app.contract.secType = app.SECTYPE
        elif(app.SECTYPE == "FUT"):
            app.EXPIRY = args.expiry
            app.contract.secType = app.SECTYPE
            app.contract.lastTradeDateOrContractMonth = app.EXPIRY
        elif(app.SECTYPE == "OPT"):
            app.STRIKE = args.strike
            app.RIGHT = args.right
            app.EXPIRY = args.expiry
            app.contract.secType = app.SECTYPE
            app.contract.lastTradeDateOrContractMonth = app.EXPIRY
            app.contract.strike = app.STRIKE
            app.contract.right = app.RIGHT
            #app.contract.multiplier = "100"
    
        with open(app.SYMBOL+"_"+app.SECTYPE+"_"+str(app.HDATE)+".csv", 'w') as csvfile:
            filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
            filewriter.writerow(["TIME","PRICE","SIZE"])
        app.reqHistoricalTicks(1, app.contract,
                                str(app.HDATE)+" 09:10:00", "", 1000, "TRADES", 1, True, [])
        
        app.run()
        app.disconnect()
       
    except Exception :
        logging.error(traceback.format_exc())
        print("\n")
        print("~~~~~~~~~~~~~~~~~~~~~~~~ Error ~~~~~~~~~~~~~~~~~~~~~~~")
        print("Error : "+traceback.format_exc())
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    finally:
        app.dumpTestCoverageSituation()
        app.dumpReqAnsErrSituation()
        print("\n")
     

if __name__ == "__main__":
    main()