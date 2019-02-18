# -*- coding: utf-8 -*-
import os
import pandas as pd
from func_forAll import func_forAll as func
import hcrTimeline_class as rodsc

import surveyDataScheduled_settings as surveyDataSettings
import roDataScheduled_settings as roDataSettings
import participantLists_settings as participantSettings


def main():
	surveyImportDict=surveyDataSettings.getDataPullDictionary()
	roImportDict=roDataSettings.getDataPullDictionary()
	programImportDict=participantSettings.getDataPullDictionary()

	#create masterCCE/cp/allRO files
	cceHCRoutputDF=pd.DataFrame()
	cpHCRoutputDF=pd.DataFrame()
	allROHCRoutputDF=pd.DataFrame()

	#create reportArray list
	tempSurveyClass=rodsc.AnalysisBaseClass(surveyImportDict['surveys'])
	reportDateArray=tempSurveyClass.monthRangeArray
	print(reportDateArray)
	del tempSurveyClass

	#cycle months in reportArray -- for each month
	#x=yyyyMM

	programClassDict={}
	for programType, programParamDict in programImportDict.items():
		programClassDict[programType]=rodsc.surveyAnalysisClass(programParamDict)

	for datesYM in reportDateArray:
		#print(datesYM)

		#cycle survey dict -- for each survey type (currently only 1):
		for surveyType, surveyParamsDict in surveyImportDict.items():
			#print(surveyType)

			#find importName for roFile
			surveyClass=rodsc.roAnalysisClass(surveyParamsDict)
			surveyImportFileName=surveyClass.saveFileNameFunc(datesYM)
			#print(surveyImportFileName)
			#pull survey df


			for roType,roParamsDict in roImportDict.items():
				#print(roType)
				#find importName for roFile
				roClass=rodsc.roAnalysisClass(roParamsDict, surveyClass)
				roImportFileName=roClass.saveFileNameFunc(datesYM)
				#print(roImportFileName)
				#pull RO df
				#join RO and survey

				#3 currently -- CCE / PDPI / FIS
				#cycle program listsClassDict (with assigned consultants)


					#pull participants lists (cce/nonCCE)
					#join RO and survey with participants lists
						#one per timeline type
						#for x in paramsDict
							#group by for metric average rating and count
							#concat to master output file (create if non-existent); 1 per metric-type

	#output as [rotype]_['hcrTimeline']_reportMonth.csv

main()

