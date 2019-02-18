# -*- coding: utf-8 -*-

from datetime import datetime
from func_forAll import func_forAll as func
import pandas as pd
import os
import numpy as np

class savefinancialStatementResults():

	def __init__(self):
		self.finalResults=pd.DataFrame()
		self.queryTypes=[
			#'PartSales',
			#'PartGross',
			'roCount',
			#'roGross',
			#'roSales',
		]

		self.odbcName='DSN=as400'

		self.msAccessodbcName={
			'local':'DSN=CCE_Dealer_Report',
			'live':'',
		}

		self.saveLocations=[
			'C://Users//HMA91571//desktop//git//financialStatements_output',\
			'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files'
			]

		self.nlColName='NL'
		self.saveFileName='/finStatementROData_'
		self.saveFileNameList=[]

		self.accessTableInput='''
			SELECT DISTINCT rptRGNDETAIL_2_NTL_TBL.*
			FROM rptRGNDETAIL_2_NTL_TBL;

			UNION ALL SELECT DISTINCT rptRGNDETAIL_2_RGN_TBL.*
			FROM rptRGNDETAIL_2_RGN_TBL;

			UNION ALL SELECT DISTINCT rptRGNDETAIL_2_DST_TBL.*
			FROM rptRGNDETAIL_2_DST_TBL;

			UNION ALL SELECT DISTINCT rptRGNDETAIL_2_DLR_TBL.*
			FROM rptRGNDETAIL_2_DLR_TBL;
		'''

		self.accessTableDf=self.fromODBC()
		#left as400, right self.finalResults

		self.joinToAccessType='left'
		self.joinToAccessKeys={
			'left':['DLRCD'],
			'right':['DTDELR'],
		}

		self.dealerNameColumnName='DLRNM'

	def removeDealerNameCommas(self, column):
		return np.vectorize(func.replaceValue)(column,',','')

	def getSaveFileName(self, reportYYYYmm, ext='.csv'): 
		return [self.saveFileName+ext, self.saveFileName+reportYYYYmm+ext]

	def saveToCsv(self):
		func.df_tocsv(self.finalResults, \
						pathList=self.saveLocations, \
						fileName=self.saveFileNameList)

	def fromODBC(self):
		return func.runAS400Query(self.accessTableInput,\
										self.msAccessodbcName['local'])

class financialStatementsClass():

	def __init__(self, queryType):
		self.curYearParamHeaders=[]#to be populated later
		self.prevYearHeaders=[]#to be populated later
		self.reportParams={
			'PartSales':{
							'varPre':'PS',
							'BFFLID':'0956',
						},
			'PartGross':{
							'varPre':'PG',
							'BFFLID':'0957',
						},
			'roCount':{
							'varPre':'ROC',
							'BFFLID':'0981',
						},
			'roGross':{
							'varPre':'ROG',
							'BFFLID':'0983',
						},
			'roSales':{
							'varPre':'ROS',
							'BFFLID':'0982',
						},
		}

		self.calendarConversionKeys={
			1:'JAN',
			2:'FEB',
			3:'MAR',
			4:'APR',
			5:'MAY',
			6:'JUN',
			7:'JUL',
			8:'AUG',
			9:'SEP',
			10:'OCT',
			11:'NOV',
			12:'DEC',
		}

		self.financialStatementTypeDF=pd.DataFrame()
		self.financialStatementSplits={
			'left':pd.DataFrame(),
			'right':pd.DataFrame(),
		}

		#(reportYYYYmm, reportYYYY, report_prevYr, reportMonth)
		self.reportingTimePeriods=func.reportYYYYmmStats()
		self.reportYYYYmm=self.reportingTimePeriods['reportYYYYmm']
		self.reportYYYY=self.reportingTimePeriods['reportYYYY']
		self.report_prevYr=self.reportingTimePeriods['report_prevYr']
		self.reportMonth=self.reportingTimePeriods['reportMonth']

		self.varPreFix=self.reportParams[queryType]['varPre']
		self.BFFLID=self.reportParams[queryType]['BFFLID']

		#placeholder for current month parameter header name
		self.curMonthName=func.joinStrings([self.varPreFix,'CUR'])
		self.curMonthVar=func.findCurMonthVar(self.reportMonth,\
													self.calendarConversionKeys,\
													self.varPreFix)

		self.curPrevYearMonthsDict=func.getCurPrevYearMonths(self.reportMonth,\
														self.varPreFix,\
														self.calendarConversionKeys)

		self.curYearMonths=self.curPrevYearMonthsDict['curYearMonths']
		self.prevYearMonths=self.curPrevYearMonthsDict['prevYearMonths']

		self.joinKeys={
			'left':['DTDELR','DTREGN','DTDSCD'],
			'right':['DTDELR','DTREGN','DTDSCD'],
		}


class financialStatementsTrimAndSummary():
	def __init__(self, reportPreFix, resultsDF):
		self.reportingTimePeriods=func.reportYYYYmmStats()
		self.calendarConversionKeys={
			1:'JAN',
			2:'FEB',
			3:'MAR',
			4:'APR',
			5:'MAY',
			6:'JUN',
			7:'JUL',
			8:'AUG',
			9:'SEP',
			10:'OCT',
			11:'NOV',
			12:'DEC',
		}
		self.inputResults=resultsDF
		self.reportMonth=self.reportMonthDropLeadingZero(self.reportingTimePeriods)
		self.reportMonthTxt=self.calendarConversionKeys[int(self.reportMonth)]
		self.financialDataType=reportPreFix
		self.monthInclude=self.monthToKeep(self.reportMonthTxt, self.financialDataType)
		
		self.includeList=[
			'DTDELR'
			,'DTREGN'
			,'DTDSCD'
			, self.monthInclude
		]
		self.outputResultsDF=self.sumMonthsList(list(self.calendarConversionKeys.values()), self.financialDataType, self.inputResults, self.includeList)
		

		
	def reportMonthDropLeadingZero(self, reportingTimePeriods):
		reportingMonth=reportingTimePeriods['reportMonth']
		if str(reportingMonth)[0]=='0':
			reportingMonth=int(str(reportingMonth)[1:])
		return reportingMonth

	def monthToKeep(self, reportMonth, preFix):
		return preFix+'CUR'

	def sumMonthsList(self, monthsList,preFix, resultsDF, includeList):
		tempList=[]
		tempDict={
			'JAN':preFix+'JAN',
			'FEB':preFix+'FEB',
			'MAR':preFix+'MAR',
			'APR':preFix+'APR',
			'MAY':preFix+'MAY',
			'JUN':preFix+'JUN',
			'JUL':preFix+'JUL',
			'AUG':preFix+'AUG',
			'SEP':preFix+'SEP',
			'OCT':preFix+'OCT',
			'NOV':preFix+'NOV',
			'DEC':preFix+'DEC',

		}

		resultsDF[preFix+'TOT']=np.vectorize(self.vectorSumMonths)(resultsDF[tempDict['JAN']],
																		resultsDF[tempDict['FEB']],
																		resultsDF[tempDict['MAR']],
																		resultsDF[tempDict['APR']],
																		resultsDF[tempDict['MAY']],
																		resultsDF[tempDict['JUN']],
																		resultsDF[tempDict['JUL']],
																		resultsDF[tempDict['AUG']],
																		resultsDF[tempDict['SEP']],
																		resultsDF[tempDict['OCT']],
																		resultsDF[tempDict['NOV']],
																		resultsDF[tempDict['DEC']]
																		)

		includeList.append(preFix+'TOT')
		finalResultsDF=resultsDF[includeList]
		return finalResultsDF

	def vectorSumMonths(self, mon1,mon2,mon3,mon4,mon5,mon6,mon7,mon8,mon9,mon10,mon11,mon12):
		tempSum=mon1+mon2+mon3+mon4+mon5+mon6+mon7+mon8+mon9+mon10+mon11+mon12
		return tempSum