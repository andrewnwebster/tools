# -*- coding: utf-8 -*-
import os
import re
import datetime
import xlrd

import pandas as pd

from ioFuncs import IOfromFile as iof
import expressAssess_class as exprAssClass

def expressAssess_paramDict():
	parameterTypes={
			'Vehicle #1 Observation':{
				'Customer Arrived Until Greeted':[],
				'Reception & Write-Up':[],
				'Vehicle Waiting In Service Drive After Write-Up':[],
				'Vehicle Parked In Staging Area':[],
				'Vehicle Moved Into Express Bay And MPI Completed':[],
				'Time From MPI Completion To Vehicle Service Completion':[],
				'Vehicle staged for wash':[],
				'Vehicle wash completed':[],
				'Vehicle Moved To Express Delivery Area':[],
				'Invoicing / Payment - Cashier':[],
				'Service Delivery And Customer Leaves':[],
			},
			'Vehicle #2 Observation':{
				'Customer Arrived Until Greeted':[],
				'Reception & Write-Up':[],
				'Vehicle Waiting In Service Drive After Write-Up':[],
				'Vehicle Parked In Staging Area':[],
				'Vehicle Moved Into Express Bay And MPI Completed':[],
				'Time From MPI Completion To Vehicle Service Completion':[],
				'Vehicle staged for wash':[],
				'Vehicle wash completed':[],
				'Vehicle Moved To Express Delivery Area':[],
				'Invoicing / Payment - Cashier':[],
				'Service Delivery And Customer Leaves':[],
			},
			'Dealer_Code':'',
			'Date_Performed':'',
			'Year_Performed':'',
		}

	returnDict=parameterTypes
	return returnDict

def trimAndSplit(stringVar):
	temp=stringVar
	temp=str(temp).split(':')
	typeTemp=temp[0]

	dateTemp=re.sub('\'', '', temp[1]).strip()
	if dateTemp=='Date':
		return [typeTemp, 'Date']

	varTemp=re.sub('\'', '', temp[-1]).strip()
	return [typeTemp, varTemp]

def returnXLSpreadsheetData(filePath, fileName, sheetName, rowCount):

	print(filePath, fileName)
	paramsDict=expressAssess_paramDict()
	assessmentSheetVars=exprAssClass.assessmentSheetVariablesCls()
	assessmentSheetVars.fileName=fileName
	assessmentSheetVars.filePath=filePath
	fullFilePath='{0}/{1}'.format(filePath, fileName)
	finalResults_DF=pd.DataFrame()
	try:
		workbook=xlrd.open_workbook(fullFilePath)	
		testKey=''
		dealerCode=''

		worksheet=workbook.sheet_by_name(sheetName)

		#dealer code
		tempDealerCode=filePath.split('\\')
		tempDealerCode=tempDealerCode[-1].split('_')
		paramsDict['Dealer_Code']=tempDealerCode[0]

		for x in range(rowCount):
			formattedValue=''
			try:
				#print('------------------------------------------------------------')
				for y in range(len(worksheet.row_slice(rowx=x))):
					splitCell=trimAndSplit(worksheet.row_slice(rowx=x)[y])
					formattedValue=splitCell[1]

					if x==7:
						if formattedValue=='Date' and paramsDict['Date_Performed']=='':
							print(worksheet.row_slice(rowx=x)[y+1])
							try:
								dateTemp=datetime.datetime(*xlrd.xldate_as_tuple(worksheet.row_slice(rowx=x)[y+1].value,workbook.datemode))
								dtTemp=dateTemp.strftime("%Y-%m-%d")
								yearTemp=dateTemp.strftime("%Y")
								paramsDict['Date_Performed']=dtTemp
								paramsDict['Year_Performed']=yearTemp
							except:
								paramsDict['Date_Performed']=trimAndSplit(worksheet.row_slice(rowx=x)[y+1])[1]

							if paramsDict['Year_Performed'] not in assessmentSheetVars.acceptableYears:
								paramsDict['Date_Performed']='1990-01-01'
								paramsDict['Year_Performed']='1990'

					if formattedValue==assessmentSheetVars.totalTimeStr:
						assessmentSheetVars.totalTestTimeTemp=trimAndSplit(worksheet.row_slice(rowx=x)[y+1])[1]
						if assessmentSheetVars.totalTestTimeTemp=='Comment':
							pass
						else:
							assessmentSheetVars.totalTestTimeTemp=(datetime.timedelta(days=float(assessmentSheetVars.totalTestTimeTemp)).seconds)/60.0
							assessmentSheetVars.sheetTotalScoreFromTotalTime+=assessmentSheetVars.totalTestTimeTemp
							assessmentSheetVars.totalTestTimeTemp=0.0
					if formattedValue == assessmentSheetVars.testKey_1 or formattedValue == assessmentSheetVars.testKey_2:
						testKey=formattedValue

						if testKey==assessmentSheetVars.testKey_1:
							pass

						elif testKey==assessmentSheetVars.testKey_2:
							#print(x,y)
							#print(testKey)
							#print('test1Complete')
							if assessmentSheetVars.observationCompletionFlag==1:
								assessmentSheetVars.sheetDF=assessmentSheetVars.testDF
								assessmentSheetVars.testDF=pd.DataFrame()
								assessmentSheetVars.observationCompletionFlag=0
								assessmentSheetVars.test2Flag=1
							#because apparently there's a hidden test 2 flag, causing test 2 key to pass twice per worksheet
							elif assessmentSheetVars.test2Flag==0:
								pass
								#halt, no results
								return pd.DataFrame()
						else:
							pass
								

					if testKey!='':
						if splitCell[0]=='text' and formattedValue in paramsDict[testKey] :
							if paramsDict[testKey][formattedValue]:
								pass
							else:
								tempDict={}

								startTimeList=trimAndSplit(worksheet.row_slice(rowx=x)[y+1])
								startTimeType=startTimeList[0]
								startTimeVar=startTimeList[1]

								endTimeList=trimAndSplit(worksheet.row_slice(rowx=x)[y+2])
								endTimeType=endTimeList[0]
								endTimeVar=endTimeList[1]

								totalTimeList=trimAndSplit(worksheet.row_slice(rowx=x)[y+3])
								totalTimeType=totalTimeList[0]
								totalTimeVar=totalTimeList[1]

								if startTimeVar!='empty' and endTimeVar!='empty'\
																	and startTimeVar!='0.0' and endTimeVar!='0.0'\
																	and startTimeVar!='' and endTimeVar!=''\
																	and startTimeVar<=endTimeVar:
									print(startTimeVar, type(startTimeVar), endTimeVar, type(endTimeVar))
									tempDict['start']=startTimeVar
									tempDict['end']=endTimeVar
									tempDict['total']=(datetime.timedelta(days=float(totalTimeVar)).seconds)/60.0
									assessmentSheetVars.observationCompletionFlag=1
									totalTimeTemp=tempDict['total']
								else:
									totalTimeTemp=0
								assessmentSheetVars.sheetTotalScoreFromTasks+=totalTimeTemp

								#We need to store values for this outside of NULL/BLANK if
								#because we don't want to ignore incomplete entries
								tempDict_results={
									'Observation_#':testKey,
									'Task_Type':assessmentSheetVars.taskRenameDict[formattedValue],
									'Minutes_To_Complete':totalTimeTemp,
									'Dealer_Code':paramsDict['Dealer_Code'],
									'Date_Performed':paramsDict['Date_Performed'],
									'Year_Performed':paramsDict['Year_Performed'],
									'filename':assessmentSheetVars.fileName,
								}
								paramsDict[testKey][formattedValue]=tempDict_results['Minutes_To_Complete']
								assessmentSheetVars.taskDF=pd.DataFrame([tempDict_results])
								temppDict_results=pd.DataFrame()
								assessmentSheetVars.testDF=pd.concat([assessmentSheetVars.testDF,assessmentSheetVars.taskDF])
								assessmentSheetVars.taskDF=pd.DataFrame()
								#print('{0}	|	{1}	|	{2}'.format(formattedValue, testKey, paramsDict[testKey][formattedValue]))
			except:
				pass
	except:
		print('Bad Worksheet Name?')

	print('TASK TIME: {0}'.format(assessmentSheetVars.sheetTotalScoreFromTasks))
	print('TOT. TIME: {0}'.format(assessmentSheetVars.sheetTotalScoreFromTotalTime))
	if assessmentSheetVars.sheetTotalScoreFromTasks!=assessmentSheetVars.sheetTotalScoreFromTotalTime:
		print('Check {0}:{1}'.format(filePath, fileName))
		tempUserInput=input('. to continue')

	if assessmentSheetVars.observationCompletionFlag==1:
		assessmentSheetVars.sheetDF=pd.concat([assessmentSheetVars.sheetDF,assessmentSheetVars.testDF])
	else:
		pass
		return pd.DataFrame()

	if not assessmentSheetVars.sheetDF.empty:
		print('\n\n\n\n\n\n\n\n\n\n\n')
		returnDF=assessmentSheetVars.sheetDF
		del assessmentSheetVars
		return returnDF.drop_duplicates()
	else:
		print('\n\n\n\n\n\n\n\n\n\n\n')
		del assessmentSheetVars
		return pd.DataFrame()


def main():
	rootDir = '//hke.local/HMA/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_CCE/3 Dlr Contact'
	#rootDir='C:/Users/HMA91571/Desktop/testDirectory/'
	sheetName='WorkFlow'
	masterData=pd.DataFrame()

	for dirName, subdirList, fileList in os.walk(rootDir):
		#print('Found directory: %s' % dirName)
		os.chdir(dirName)
		for fname in fileList:
			fileExtension=fname.split('.')[-1]
			if fileExtension=='xls':
				if datetime.datetime.fromtimestamp(os.stat(fname).st_mtime).year >= 2016:
					fileName=fname
					directory=os.getcwd()
					tempDF=returnXLSpreadsheetData(directory, fileName, sheetName, 75)
					#print('{0} : {1}'.format(fileName, directory))
					if not tempDF.empty:
						masterData=pd.concat([masterData, tempDF])
	print(masterData)
	iof.df_tocsv_extension(masterData,\
							'C:/Users/HMA91571/Desktop/testDirectory/assessmentMetrics.csv'
							)

def main_2():
	directory='C:/Users/HMA91571/Desktop/testDirectory'
	filename='(SA) GA075 AutoNation Columbus-Rgiammara_10052017.xls'
	sheetName='WorkFlow'
	returnXLSpreadsheetData(directory, filename, sheetName, 75)
	
main()

