# -*- coding: utf-8 -*-

from func_forAll import func_forAll as func
import financialStatements_class as pyc
import pandas as pd
import os
import numpy as np

def main():
	print('\n')

	final=pyc.savefinancialStatementResults()
	queryTypes=final.queryTypes

	saveLocations=[
			'C://Users//HMA91571//desktop//git//financialStatements_output',\
			'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files'
			]
	
	#iterate through financial statement parameters
	for x in queryTypes:
		#generate a class
		x=pyc.financialStatementsClass(x)

		#local variables (corresponding to class)
		rptYear=x.reportYYYY
		rptPrevYear=x.report_prevYr
		varPreFix=x.varPreFix
		bmCode=x.BFFLID

		#query generation
		queryData=func.financialStatementReportQuery(rptYear,varPreFix,bmCode)
		prevYearQueryData=func.financialStatementReportQuery(rptPrevYear,varPreFix,bmCode)
		
		#query execution
		reportYearDF=func.runAS400Query(queryText=queryData, odbcVarName=final.odbcName)
		reportPrevYearDF=func.runAS400Query(queryText=prevYearQueryData, odbcVarName=final.odbcName)
		print(reportYearDF)
		#split results by year, delete duplicate months
		reportYearDrops=x.prevYearMonths
		reportPrevYearDrops=x.curYearMonths

		#print('-----')
		#print(reportPrevYearDF.describe())
		#print(reportPrevYearDrops)
		reportYearDF=reportYearDF.drop(reportYearDrops, axis=1)
		reportPrevYearDF=reportPrevYearDF.drop(reportPrevYearDrops, axis=1)

		#print(reportYearDrops)
		#print('-----')
		#print(reportPrevYearDrops)

		print('-----')
		#print(reportPrevYearDF.head())

		#print('-----')
		#print(reportPrevYearDF.describe())

		#filling out class variables
		x.financialStatementSplits['left']=reportYearDF
		x.financialStatementSplits['right']=reportPrevYearDF

		#rejoin years
		x.financialStatementTypeDF=func.joinDataFrames(
				x.financialStatementSplits,
				x.joinKeys
			)
		#add current month and national columns
		#print(x.financialStatementTypeDF.describe())
		x.financialStatementTypeDF[x.curMonthName]=\
								x.financialStatementTypeDF[x.curMonthVar]
		#print(x.financialStatementTypeDF.head())

		reportTrimClass=pyc.financialStatementsTrimAndSummary(varPreFix,x.financialStatementTypeDF)
		#print(reportTrimClass.reportMonth)
		#print(reportTrimClass.reportMonthTxt)
		#print(list(reportTrimClass.calendarConversionKeys.values()))
		#print(reportTrimClass.sumMonths)
		#print(reportTrimClass.monthInclude)
		#print(reportTrimClass.outputResultsDF)
		trimmedParamDF=reportTrimClass.outputResultsDF
		del reportTrimClass



		#join financial statement parameter reports
		#create if none found
		if final.finalResults.empty:
			final.finalResults=trimmedParamDF
		else:
			final.finalResults=func.joinDataFrames({'left':final.finalResults,\
														'right':trimmedParamDF,},\
													x.joinKeys)

	final.finalResults[final.nlColName]=final.nlColName
	print(final.finalResults.head())

	final.finalResults=func.joinDataFrames({'left':final.accessTableDf,\
												'right':final.finalResults,},\
											final.joinToAccessKeys,\
											final.joinToAccessType)



	final.finalResults[final.dealerNameColumnName]\
		=final.removeDealerNameCommas(final.finalResults[final.dealerNameColumnName])

	final.saveFileNameList=final.getSaveFileName(x.reportYYYYmm)
	final.saveToCsv()

	print('\n')
	print('-----')
	print('-----')
	print('-----')
	print('-----')
	print('-----')
	print('-----')
	print('-----')
	#print(final.finalResults['PSDEC'])

main()
