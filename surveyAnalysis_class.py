from func_forAll import func_forAll as func
from ioFuncs import IOfromFile as iof
import historyLog_settings as historySettings
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time


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
		self.prevMonthYm=self.monthRangeArray[1]
		self.prevYrYm=self.reportPrevYr+self.reportMonth

		self.thisMonthFileName='/'+self.className+self.reportYearMonth+'.csv'
		self.lastMonthsFileName='/'+self.className+self.prevMonthYm+'.csv'
		self.lastYearsFileName='/'+self.className+self.prevYrYm+'.csv'

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

class surveyAnalysisClass(AnalysisBaseClass):
	def __init__(self, classDict):
		AnalysisBaseClass.__init__(self,classDict)

		self.joinKeys=[
			'DEALER_CODE',
			'RO_NUMBER'
		]

		self.thisMonthDF=self.getDataFromCsv(self.thisMonthFileName)
		self.lastMonthDF=self.getDataFromCsv(self.lastMonthsFileName)
		self.lastYearDF=self.getDataFromCsv(self.lastYearsFileName)
		

class roAnalysisClass(AnalysisBaseClass):
	def __init__(self, classDict, surveys):
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

		self.surveyList={'thisMonth':surveys.thisMonthDF,
							'lastMonth':surveys.lastMonthDF,
							'lastYear': surveys.lastYearDF}

		self.thisMonthDF=self.getDataFromCsv(self.thisMonthFileName)
		self.lastMonthDF=self.getDataFromCsv(self.lastMonthsFileName)
		self.lastYearDF=self.getDataFromCsv(self.lastYearsFileName)

		self.thisMonthDFtrimmed=self.thisMonthDF[self.trimKeys]
		self.lastMonthDFtrimmed=self.lastMonthDF[self.trimKeys]
		self.lastYearDtrimmedF=self.lastYearDF[self.trimKeys]

		self.roListKeys=['thisMonth','lastMonth','lastYear']
		self.roList={'thisMonth':self.thisMonthDFtrimmed,
							'lastMonth':self.lastMonthDFtrimmed,
							'lastYear': self.lastYearDtrimmedF}
		
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

class surveyMonthClass:
	def __init__(self, filenameYYYYmm, inputLoc):
		self.inputFileLoc=inputLoc

		self.inputMonthFileTypes={
			'surveys'	:'/surveys'+filenameYYYYmm+'.csv',
			'cp'		:'/cpRO'+filenameYYYYmm+'.csv',
			'cce'		:'/cceRO'+filenameYYYYmm+'.csv',
			'all'		:'/allRO'+filenameYYYYmm+'.csv',
		}

		self.inputMonthFileDFs={
			'surveys'	:pd.DataFrame(),
			'cp'		:pd.DataFrame(),
			'cce'		:pd.DataFrame(),
			'all'		:pd.DataFrame(),
		}		

		self.outputFileDFs={
			'cp'	:pd.DataFrame(),
			'cce'	:pd.DataFrame(),
			'all'	:pd.DataFrame(),
		}

		self.outputSummaryDFs={
			'cp'	:pd.DataFrame(),
			'cce'	:pd.DataFrame(),
			'all'	:pd.DataFrame(),
		}

		self.joinKeys={
			'ro'	:[
						'DEALER_CD',
						str('REPAIR_ORDER_NUM'),
						'VIN'
						],
			'survey':[
						'DEALER_CODE',
						str('REPAIR_ORDER'),
						'VINSURV'
						],
			}

class hcrTimelineClass:
	def __init__(self):
		self.historyLogDict=historySettings.historyLogParams()
		self.historyLogLoc=self.historyLogDict['location']
		self.historyLogFileName=self.historyLogDict['filename']
		self.timelineMaster=pd.DataFrame()
		self.baseLocation='//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pydata'
		self.importLocation={
			'cp':'/cp_hcr_survey_list_13_mo.csv',
			'cce':'/cce_hcr_survey_list_13_mo.csv',
			'all':'/all_hcr_survey_list_13_mo.csv',
		}
		self.outputLocation={
			'cp':'/cp_hcr_agg_13_mo.csv',
			'cce':'/cce_hcr_agg_13_mo.csv',
			'all':'/all_hcr_agg_13_mo.csv',
		}
		
		self.timelineDFs={
			'cp'	:pd.DataFrame(),
			'cce'	:pd.DataFrame(),
			'all'	:pd.DataFrame(),
			}

		self.timelineDFsInclude={
			'RODATE':'3MthDt',
			'SURCDT':'3Mth',
			'DEALER':'Dealer Code',
			'RATING':'DL_HCRAvg',
		}

		self.groupByColumns=[
			'3MthDt',
			'3Mth',
			'Dealer Code',
		]

		self.cceFlagHeaderandValue={
			'cp':'CP_HCR',
			'cce':'CCE_HCR',
			'all':'All_HCR',
		}
		
	def change3MthDt(self, dateParam):
		temp=str(dateParam)
		temp=re.sub(r'/[0-9]+/','/1/', temp)
		return str(temp)

	def change3Mth(self, yyyymmddParam):
		tempdtObj=datetime.strptime(yyyymmddParam, "%m/%d/%Y")
		tempTuple=tempdtObj.timetuple()
		temp=time.strftime("%Y%m", tempTuple)
		return str(temp)

	def finalSave(self, saveName, dFrameName):
		#iof.toCSV(pathList=[self.baseLocation], fileName=saveName, dFrame=dFrameName, index=True)
		try:
			iof.toCSV(pathList=[self.baseLocation], fileName=saveName, dFrame=dFrameName, index=False)
		except:
			errorMsg=saveName+' export failure (finalSave)'
			print(errorMsg)
			func.errorLog(self.historyLogLoc, self.historyLogFileName, errorMsg)

	def timelineConcat(self, df1, df2):
		tempDF=pd.concat([df1, df2])
		return tempDF

class surveyFullClass:
	def __init__(self, inputLoc, timeDict):
		self.historyLogDict=historySettings.historyLogParams()
		self.historyLogLoc=self.historyLogDict['location']
		self.historyLogFileName=self.historyLogDict['filename']
		#self.outputFileLoc='C:/Users/HMA91571/Desktop'
		self.outputFileLoc='//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pydata'
		#self.outputFileLoc=inputLoc

		self.outputFileTypes={
			'cp'	:'cpSurveys.csv',
			'cce'	:'cceSurveys.csv',
			'all'	:'allSurveys.csv',
			}

		self.masterRawDF={
			'cp'	:pd.DataFrame(),
			'cce'	:pd.DataFrame(),
			'all'	:pd.DataFrame(),
			}

		self.masterSummarizedDF={
			'cp'	:pd.DataFrame(),
			'cce'	:pd.DataFrame(),
			'all'	:pd.DataFrame(),
			}

		self.masterSummarizedInclude=\
					[	
						'CREATE_DATE_YM',
						'DEALER_CODE',
						'OLD_SURVEY_COUNT',
						'NEW_SURVEY_COUNT',
						'RATING',
						'TIMELY_SERVICE',
						'RECO_DEALER',
						'RECO_BRAND',
						'RETURN_DEALER',
						'TIMELINESS_RATING',
						'VALUE_RATING',
						'COMMUNICATION',
						'FACILITY_AMENITIES',
						'SERVICE_QUALITY',
						'REGION',
						'DISTRICT',
						'NATL',
						'TIMELINESS_DIFF',
					]

		self.summarizedlevelGroupBy={
			'dealer'	:['DEALER_CODE','REGION','DISTRICT','NATL', 'CREATE_DATE_YM'],
		}

		self.masterRawInclude=\
					[	
						'VIN',
						'RO_CLOSE_DATE',
						'CREATE_DATE',
						'REPAIR_ORDER_NUM',
						'DEALER_CODE',
						'OLD_SURVEY_COUNT',
						'NEW_SURVEY_COUNT',
						'RATING',
						'TIMELY_SERVICE',
						'RECO_DEALER',
						'RECO_BRAND',
						'RETURN_DEALER',
						'TIMELINESS_RATING',
						'VALUE_RATING',
						'COMMUNICATION',
						'FACILITY_AMENITIES',
						'SERVICE_QUALITY',
						'REGION',
						'DISTRICT',
						'NATL',
						'HR',
						'TIMELINESS_DIFF',
					]

		self.accessRawDF={
			'cp':pd.DataFrame(),
			'cce':pd.DataFrame(),
			'all':pd.DataFrame(),
		}


		self.SummarizedlevelGroupBy={
			'dealer'	:['DEALER_CODE','REGION','DISTRICT','NATL'],
		}

		self.accessRawRenameInclude={
			'cp'	:{	
						'VIN'						:'VIN',
						'RO_CLOSE_DATE'				:'RODATE',
						'CREATE_DATE'				:'SURCDT',
						'REPAIR_ORDER_NUM'			:'RONUM',
						'DEALER_CODE'				:'DEALER',
						'RATING'					:'RATING',
						'TIMELY_SERVICE'			:'TIMELY',
						'RECO_DEALER'				:'WREC',
						'RECO_BRAND'				:'RECO_BRAND',
						'RETURN_DEALER'				:'RTN',
						'HR'						:'HR',
						'TIMELINESS_RATING'			:'TIMELINESS_RATING',
						'VALUE_RATING'				:'VALUE',
						'COMMUNICATION'				:'COMMUNICATION',
						'FACILITY_AMENITIES'		:'AMENITIES',
						'SERVICE_QUALITY'			:'QUALITY',
						'TIMELINESS_DIFF'			:'TIMELINESS_DIFF',
						},
			'all'	:{	
						'VIN'						:'VIN',
						'RO_CLOSE_DATE'				:'RODATE',
						'CREATE_DATE'				:'SURCDT',
						'REPAIR_ORDER_NUM'			:'RONUM',
						'DEALER_CODE'				:'DEALER',
						'RATING'					:'RATING',
						'TIMELY_SERVICE'			:'TIMELY',
						'RECO_DEALER'				:'WREC',
						'RECO_BRAND'				:'RECO_BRAND',
						'RETURN_DEALER'				:'RTN',
						'HR'						:'HR',
						'TIMELINESS_RATING'			:'TIMELINESS_RATING',
						'VALUE_RATING'				:'VALUE',
						'COMMUNICATION'				:'COMMUNICATION',
						'FACILITY_AMENITIES'		:'AMENITIES',
						'SERVICE_QUALITY'			:'QUALITY',
						'TIMELINESS_DIFF'			:'TIMELINESS_DIFF',
						},
			'cce'	:{
						'VIN'						:'VIN',
						'RO_CLOSE_DATE'				:'RODATE',
						'CREATE_DATE'				:'SURCDT',
						'REPAIR_ORDER_NUM'			:'RONUM',
						'DEALER_CODE'				:'DEALER',
						'RATING'					:'RATING',
						'TIMELY_SERVICE'			:'TIMELY',
						'RECO_DEALER'				:'WREC',
						'RECO_BRAND'				:'RECO_BRAND',
						'RETURN_DEALER'				:'RTN',
						'HR'						:'HR',
						'TIMELINESS_RATING'			:'TIMELINESS_RATING',
						'VALUE_RATING'				:'VALUE',
						'COMMUNICATION'				:'COMMUNICATION',
						'FACILITY_AMENITIES'		:'AMENITIES',
						'SERVICE_QUALITY'			:'QUALITY',
						'TIMELINESS_DIFF'			:'TIMELINESS_DIFF',
						},
			}

		self.roType={
			'all'	:'/allRO',
			'cce'	:'/cceRO',
			'cp'	:'/cpRO',
			}

		self.summaryOutputFileName=''

		self.timeFrameVar='RO_CLOSE_DATE'

		self.timeFrameType={
			'lastMonth'	:timeDict['lastMonth'],
			'lastYear'	:timeDict['lastYear'],
			'thisMonth'	:timeDict['thisMonth'],
			}

		self.levelTypes={
			'dealer'	:'dealer.csv',
			'district'	:'district.csv',
			'region'	:'region.csv',
			'national'	:'national.csv',
			}

		self.levelGroupBy={
			'dealer'	:['DEALER_CODE','REGION','DISTRICT','NATL'],
			'district'	:['REGION','DISTRICT','NATL'],
			'region'	:['REGION',	'NATL'],
			'national'	:['NATL'],
			}

		self.levelTrims={
			'dealer'	:['VIN', 'REPAIR_ORDER_NUM', 'CREATE_DATE', 'RO_CLOSE_DATE'],
			'district'	:['VIN', 'REPAIR_ORDER_NUM', 'CREATE_DATE',  'RO_CLOSE_DATE', 'DEALER_CODE'],
			'region'	:['VIN', 'REPAIR_ORDER_NUM', 'CREATE_DATE',  'RO_CLOSE_DATE', 'DEALER_CODE','DISTRICT'],
			'national'	:['VIN', 'REPAIR_ORDER_NUM', 'CREATE_DATE',  'RO_CLOSE_DATE', 'DEALER_CODE','REGION','DISTRICT'],
			}

	def groupByDataFrames(self, inputFrame, groupbyColumns):
		tempDF=inputFrame.groupby(groupbyColumns, as_index=False)
		tempDF_2=tempDF.agg(['sum','count']).rename(columns={'sum':'sum','count':'count'})

		newidx = []
		for (n1,n2) in tempDF_2.columns.ravel():
		    newidx.append("%s-%s" % (n1,n2))
		tempDF_2.columns=newidx

		return tempDF_2.reset_index()

	def finalSave(self, saveName, dFrameName):
		#iof.toCSV(pathList=[self.outputFileLoc], fileName=saveName, dFrame=dFrameName, index=True)
		try:
			iof.toCSV(pathList=[self.outputFileLoc], fileName=saveName, dFrame=dFrameName, index=True)
		except:
			errorMsg=saveName+' export failure (finalSave)'
			print(errorMsg)
			func.errorLog(self.historyLogLoc, self.historyLogFileName, errorMsg)

	def changeROdate(self, yyyymmddParam):
		tempdtObj=datetime.strptime(yyyymmddParam, "%m/%d/%Y")
		tempTuple=tempdtObj.timetuple()
		temp=time.strftime("%Y%m%d", tempTuple)
		return int(temp)