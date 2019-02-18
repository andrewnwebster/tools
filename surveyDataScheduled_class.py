from func_forAll import func_forAll as func
import historyLog_settings as historySettings
import pandas as pd

class roDataScheduled:
	def __init__(self, classDict):
		self.odbcVar='DSN=as400'
		self.reportMonthCheckFlag=1 #check for recent download, set to 0 to always run query

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

		self.surveyExpandedParamsBit=1

		self.historyLogDict=historySettings.historyLogParams()
		self.historyLogLoc=self.historyLogDict['location']
		self.historyLogFileName=self.historyLogDict['filename']

	def getSurveySQLQuery(self, yyyyMMIn):
		return func.getSurveySQLQuery(yyyyMMIn, self.surveyExpandedParamsBit)

	def as400Print(self, yyyyMMStr):
		self.saveFileName=self.saveFileNameFunc(yyyyMMStr)
		as400TestList=func.getUpdatedThisMonthResults(self.outputLoc, self.saveFileName, self.reportMonthCheckFlag)
		if as400TestList[0]==0:
			self.sqlDataReturn=func.runAS400Query(queryText=self.sqlData, odbcVarName=self.odbcVar)
			func.df_tocsv(self.sqlDataReturn, [self.outputLoc], self.saveFileName)
			func.errorLog(self.historyLogLoc, self.historyLogFileName\
							, as400TestList[1])
		else:
			pass
			func.errorLog(self.historyLogLoc, self.historyLogFileName\
							, as400TestList[1])
	def getYyyyMmList(self, reportYearMonth, reportMonthsPast):
		return func.getYyyyMmList(reportYearMonth, reportMonthsPast)

	def saveFileNameFunc(self, yyyyMMStr):
		return '/'+self.className+yyyyMMStr+'.csv'