# -*- coding: utf-8 -*-
from func_forAll import func_forAll as func
import pandas as pd
import os

class databaseColumnQueryClass:
	def __init__(self):
		self.odbcName='DSN=as400'

		self.databaseNamePrompt='What is the databaseName?   '
		self.databaseName=func.cleanStrInput(
					prompt=self.databaseNamePrompt,\
					strLength=25
			)

		self.tableNamePrompt='What is the tableName?   '
		self.tableName=func.cleanStrInput(
					prompt=self.tableNamePrompt,\
					strLength=25
			)

		self.fullDf=pd.DataFrame()
		self.headersCol=pd.DataFrame()
		self.headersRow=pd.DataFrame()

		self.saveFileDir='C:/Users/HMA91571/Desktop/git/querySamples'
		self.saveFileName='/{0}.xlsx'.format(self.tableName)
		self.saveFileSheet=['rows','cols','samples']

		self.queryText=''

	def saveHeadersToExcel(self):
		cols=self.headersCol
		rows=self.headersRow
		samples=self.fullDf

		saves=[rows, cols, samples]
		sheets=self.saveFileSheet

		path=self.saveFileDir+self.saveFileName

		if cols.empty:
			return 0
		if rows.empty:
			return 0

		func.dfToExcel(dFList=saves,\
						path=path, \
						sheetsList=sheets)

	def openOutputFile(self):
		temp=self.saveFileDir+self.saveFileName
		os.startfile(temp)

	def getColumnQueryText(self, databaseName='',tableName=''):
		if databaseName=='':
			return 0
		if tableName=='':
			return 0

		tempQueryText="""
			SELECT *
			FROM {0}.{1}
			fetch first 10 rows only
		""".format(databaseName, tableName)
		return tempQueryText

	def runSamplesQuery(self):
		self.fullDf=func.runAS400Query(\
							queryText=self.queryText,\
							odbcVarName=self.odbcName)
	def getDataFrameHeaders(self):
		tempSeries=func.getDFHeaders(tempDF=self.fullDf)
		self.headersCol=pd.DataFrame(tempSeries)
		self.headersRow=self.headersCol.transpose()
