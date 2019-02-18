# -*- coding: utf-8 -*-
import os
import pandas as pd
from func_forAll import func_forAll as func
from ioFuncs import IOfromFile as iopy
import numpy as np
import surveyAnalysis_class as rodsc

import surveyDataScheduled_settings as surveyDataSettings
import roDataScheduled_settings as roDataSettings

def main():
	
	surveyImportDict=surveyDataSettings.getDataPullDictionary()
	roImportDict=roDataSettings.getDataPullDictionary()	

	surveyMonthsBack=	surveyImportDict['surveys']['monthsInPast']

	cpROFix=			roImportDict['cpRO']['filePreFix']
	allROFix=			roImportDict['allRO']['filePreFix']
	cceROFix=			roImportDict['cceRO']['filePreFix']

	roMonthsBack=		roImportDict['allRO']['monthsInPast']

	outputFileLocation=	surveyImportDict['surveys']['outputFileLocation']

	reportYearStats=	func.reportYYYYmmStats()

	#create list of yyyymm for 24 months
	if surveyMonthsBack<roMonthsBack:
		monthsInPastOfficial=surveyMonthsBack
	else:
		monthsInPastOfficial=roMonthsBack

	monthsInPastOfficial=13

	print('...Looking back '+str(monthsInPastOfficial)+' months in the past...')

	startMonth=			reportYearStats['reportYYYYmm']
	lastYearMonth=		reportYearStats['report_prevYr']+reportYearStats['reportMonth']
	monthsArray=		func.getYyyyMmList(startMonth, monthsInPastOfficial)
	lastMonth=			monthsArray[1]

	masterMonthFilters={
		'lastMonth': lastMonth,
		'lastYear': lastYearMonth,
		'thisMonth': startMonth,
	}

	#create master class
	masterClass=rodsc.surveyFullClass(outputFileLocation, masterMonthFilters)

	#start loop of yyyyMM list
	for x in monthsArray:
		#creates surveyMonthClass with initialized variables
		monthClass=rodsc.surveyMonthClass(x, outputFileLocation)

		#import input .csv files to monthClass.inputMonthFileDFs
		for k,v in monthClass.inputMonthFileTypes.items():
			print(v)
			monthClass.inputMonthFileDFs[k]=iopy.fromCSV(monthClass.inputFileLoc,\
															v)
		#inner join survey to each ro file
		for k,v in masterClass.outputFileTypes.items():
			framesDict={
				'left':monthClass.inputMonthFileDFs['surveys'],
				'right':monthClass.inputMonthFileDFs[k],
			}
			joinKeys={
				'left':monthClass.joinKeys['survey'],
				'right':monthClass.joinKeys['ro'],
			}

			#join monthly RO data with monthly survey data 
			#surveys pertain to month ROs, might be after the month
			monthClass.outputFileDFs[k]=func.joinDataFrames(framesDict, joinKeys, joinType='inner')
			
			#summaryTables(sum/count format)
			monthClass.outputSummaryDFs[k]=monthClass.outputFileDFs[k][masterClass.masterSummarizedInclude]
			monthClass.outputSummaryDFs[k]=masterClass.groupByDataFrames(monthClass.outputSummaryDFs[k], masterClass.summarizedlevelGroupBy['dealer'])
			sourceSummaryDF=masterClass.masterSummarizedDF[k]
			augmentSummaryDF=monthClass.outputSummaryDFs[k]
			dataFrameSummaryList=[sourceSummaryDF,augmentSummaryDF]
			masterClass.masterSummarizedDF[k]=func.concatDataFrames(dataFrameSummaryList)

			#add raw results to rawMasterFiles
			#concat master raw files
			sourceDF=masterClass.masterRawDF[k]
			augmentDF=monthClass.outputFileDFs[k]

			dataframeList=[sourceDF,augmentDF]
			#concat to master file (surveys by dealer day)
			masterClass.masterRawDF[k]=func.concatDataFrames(dataframeList)
			masterClass.masterRawDF[k]=masterClass.masterRawDF[k][masterClass.masterRawInclude]

			del dataframeList
			del framesDict
			del joinKeys
	
	#create access version
	for k,v in masterClass.accessRawRenameInclude.items(): 
		tempDP=pd.DataFrame()
		if v:
			tempDF=masterClass.masterRawDF[k]
			tempDF=tempDF[list(masterClass.accessRawRenameInclude[k].keys())]
			tempDF=tempDF.rename(columns=masterClass.accessRawRenameInclude[k])
			tempDF=tempDF.reset_index(drop=True)
			masterClass.accessRawDF[k]=tempDF
			print(tempDF.head())
			masterClass.finalSave('/'+k+'_hcr_survey_list_13_mo.csv', tempDF)
		try:
			del tempDF
		except:
			pass
	
	#create HCR timeline report

	hcrTimelineClass=rodsc.hcrTimelineClass()
	for k,v in hcrTimelineClass.importLocation.items():
		importTemp=pd.read_csv(hcrTimelineClass.baseLocation+v)
		trimmedTemp = importTemp[list(hcrTimelineClass.timelineDFsInclude.keys())]
		trimmedTemp=trimmedTemp.rename(index=str, columns=hcrTimelineClass.timelineDFsInclude)
		trimmedTemp['3MthDt']=np.vectorize(hcrTimelineClass.change3MthDt)(trimmedTemp['3MthDt'])
		trimmedTemp['3Mth']=np.vectorize(hcrTimelineClass.change3Mth)(trimmedTemp['3MthDt'])
		summarizedDF=trimmedTemp.groupby(hcrTimelineClass.groupByColumns).mean().reset_index()
		summarizedDF['cceFlag']=hcrTimelineClass.cceFlagHeaderandValue[k]

		hcrTimelineClass.timelineMaster=hcrTimelineClass.timelineConcat(hcrTimelineClass.timelineMaster, summarizedDF)
		print(summarizedDF.head())

		hcrTimelineClass.finalSave(hcrTimelineClass.outputLocation[k], summarizedDF)

		del importTemp
		del trimmedTemp
		del summarizedDF
	#print(hcrTimelineClass.cceFlagHeaderandValue['cce'])
	#print(hcrTimelineClass.cceFlagHeaderandValue['all'])

	hcrTimelineClass.timelineMaster=hcrTimelineClass.timelineMaster\
			[(hcrTimelineClass.timelineMaster['cceFlag'] == hcrTimelineClass.cceFlagHeaderandValue['cce'])\
			 | (hcrTimelineClass.timelineMaster['cceFlag'] == hcrTimelineClass.cceFlagHeaderandValue['all'] )]
	hcrTimelineClass.finalSave('/hcr_12mo_dealer_timeline.csv', hcrTimelineClass.timelineMaster)
	
	
	for c,d in masterClass.roType.items():
		print(c)
		tempDF=pd.DataFrame()
		tempDF=masterClass.masterRawDF[c]
		tempDF[masterClass.timeFrameVar]=np.vectorize(masterClass.changeROdate)(tempDF[masterClass.timeFrameVar])

		for a,b in masterClass.timeFrameType.items():
			print(a)
			tempDF2=pd.DataFrame()
			print(tempDF[masterClass.timeFrameVar])
			print(type(tempDF[masterClass.timeFrameVar]))
			print(type(masterClass.timeFrameType[a]))

			#changeRODate mm/dd/yyyy to yyyymm


			tempDF2=tempDF[(tempDF[masterClass.timeFrameVar]>=int(masterClass.timeFrameType[a])*100+1)\
				& (tempDF[masterClass.timeFrameVar]<=int(masterClass.timeFrameType[a])*100+31)]

			for e,f in masterClass.levelTypes.items():
				print(e)
				tempDF3=pd.DataFrame()
				tempDF3=masterClass.groupByDataFrames(tempDF2.drop(masterClass.levelTrims[e], axis=1), masterClass.levelGroupBy[e])
				masterClass.summaryOutputFileName=d+a+f
				masterClass.finalSave(masterClass.summaryOutputFileName, tempDF3)

			del tempDF3
		del tempDF2
	del tempDF

	#output master file
	del masterClass
	del surveyImportDict
	del monthClass
	del roImportDict
	
	
main()