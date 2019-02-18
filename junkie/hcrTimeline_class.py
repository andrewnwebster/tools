from func_forAll import func_forAll as func
from ioFuncs import IOfromFile as iof
import historyLog_settings as historySettings
import pandas as pd
import numpy as np


class AnalysisBaseClass:
	def __init__(self, classDict):

		self.historyLogDict=historySettings.historyLogParams()
		self.historyLogLoc=self.historyLogDict['location']
		self.historyLogFileName=self.historyLogDict['filename']

		self.odbcVar='DSN=as400'

		self.className=classDict['filePreFix']
		self.outputLoc=classDict['outputFileLocation']
		self.reportMonthsPast=classDict['monthsInPast']
		self.saveFileName=''

		self.dateParams=func.reportYYYYmmStats()
		self.reportYearMonth=self.dateParams['reportYYYYmm']
		self.reportYear=self.dateParams['reportYYYY']
		self.reportPrevYr=self.dateParams['report_prevYr']
		self.reportMonth=self.dateParams['reportMonth']
		
		self.monthRangeArray=self.getYyyyMmList(self.reportYearMonth, self.reportMonthsPast)

	def getYyyyMmList(self, reportYearMonth, reportMonthsPast):
		return func.getYyyyMmList(reportYearMonth, reportMonthsPast)

	def getDataFromCsv(self, dfFileName):
		try:
			tempDF=iof.fromCSV(filePath=self.outputLoc, fileName=dfFileName)
			return tempDF
		except:
			errorMsg=self.saveFileName+' failed to import (getDataFromCsv)'
			func.errorLog(self.historyLogLoc, self.historyLogFileName\
							, errorMsg)
			return pd.DataFrame()
	def saveFileNameFunc(self, yyyyMMStr):
		return '/'+self.className+yyyyMMStr+'.csv'

class surveyAnalysisClass(AnalysisBaseClass):
	def __init__(self, classDict):
		AnalysisBaseClass.__init__(self,classDict)

		self.joinKeys=[
			'DEALER_CODE',
			'RO_NUMBER'
		]

class participantLists(AnalysisBaseClass):
	def __init__(self):
		AnalysisBaseClass.__init__(self,classDict)
		self.fileParamDict=classDict

class roAnalysisClass(AnalysisBaseClass):
	def __init__(self, classDict, surveys=None):

		AnalysisBaseClass.__init__(self,classDict)
		self.trimKeys=['DEALER_CD','REPAIR_ORDER_NUM']

		self.dealerMetricsGroupBy=[
			'DEALER_CODE',
			'REGION',
			'DISTRICT',
			'NL'
		]

		self.joinDrops=['DEALER_CD','REPAIR_ORDER_NUM','CREATE_DATE','COMPLETE_DATE','REPAIR_ORDER']

		self.dealerTrim=[]
		self.districtTrim=['DEALER_CODE',]
		self.regionTrim=['DEALER_CODE','DISTRICT']
		self.nlTrim=['DEALER_CODE','REGION','DISTRICT']

		self.districtMetricsGroupBy=[
			'REGION',
			'DISTRICT',
			'NL'
		]

		self.regionMetricsGroupBy=[
			'REGION',
			'NL'
		]

		self.nlMetricsGroupBy=[
			'NL'
		]

		self.joinKeys={
			'left':['DEALER_CD','REPAIR_ORDER_NUM'],
			'right':['DEALER_CODE','REPAIR_ORDER'],
		}

		
	def joinDataFrames(self, framesDict, keysDict, joinType='inner'):
		return func.joinDataFrames(framesDict, keysDict, joinType)

	def groupByDataFrames(self, inputFrame, groupbyColumns):
		tempDF=inputFrame.groupby(groupbyColumns, as_index=False)
		tempDF_2=tempDF.agg(['sum','count']).rename(columns={'sum':'sum','count':'count'})

		newidx = []
		for (n1,n2) in tempDF_2.columns.ravel():
		    newidx.append("%s-%s" % (n1,n2))
		tempDF_2.columns=newidx

		return tempDF_2

	def finalSave(self, saveName, dFrameName):
		try:
			iof.toCSV(pathList=[self.outputLoc], fileName=saveName, dFrame=dFrameName, index=True)
		except:
			errorMsg=saveName+' export failure (finalSave)'
			print(errorMsg)
			func.errorLog(self.historyLogLoc, self.historyLogFileName, errorMsg)

'''		
class roAnalysis:
	def __init__(self, classDict):
		self.odbcVar='DSN=as400'

		self.className=classDict['filePreFix']
		self.outputLoc=classDict['outputFileLocation']
		self.reportMonthsPast=classDict['monthsInPast']
		self.saveFileName=''

		self.dateParams=func.reportYYYYmmStats()
		self.reportYearMonth=self.dateParams['reportYYYYmm']
		self.reportYear=self.dateParams['reportYYYY']
		self.reportPrevYr=self.dateParams['report_prevYr']
		self.reportMonth=self.dateParams['reportMonth']

		self.monthRangeArray=[]
		self.sqlData=''
		self.sqlDataReturn=pd.DataFrame()

		self.surveyExpandedParamsBit=0

		self.historyLogDict=historySettings.historyLogParams()
		self.historyLogLoc=self.historyLogDict['location']
		self.historyLogFileName=self.historyLogDict['filename']

		self.finalResultsFileName=''
		self.finalResults=pd.DataFrame()
		self.tempResults=pd.DataFrame()
		self.tempConcatDfs=[]

		self.roDataFrameDict={}
		self.aggregatedDict={}'''
		
'''
	def getYyyyMmList(self, reportYearMonth, reportMonthsPast):
		return func.getYyyyMmList(reportYearMonth, reportMonthsPast)

	def saveFileNameFunc(self, yyyyMMStr):
		return '/'+self.className+yyyyMMStr+'.csv'

	def getOutputFileName(self):
		return '/combined_'+self.className+str(self.reportYearMonth)+'.csv'

	def getDataFromCsv(self):
		try:
			tempDF=iof.fromCSV(filePath=self.outputLoc, fileName=self.saveFileName)
			return tempDF
		except:
			errorMsg=self.saveFileName+' failed to import (getDataFromCsv)'
			func.errorLog(self.historyLogLoc, self.historyLogFileName\
							, errorMsg)
			return pd.DataFrame()

	def finalSave(self):
		self.finalResultsFileName=self.getOutputFileName()
		try:
			iof.toCSV(pathList=[self.outputLoc], fileName=self.finalResultsFileName, dFrame=self.finalResults)
		except:
			errorMsg=self.finalResultsFileName+' export failure (finalSave)'
			func.errorLog(self.historyLogLoc, self.historyLogFileName\
							, errorMsg)
'''