import os
import xlrd
import datetime
import string
import pandas as pd
import expressAssess_assessmentAnalysis_class
from ioFuncs import IOfromFile as iof
import subprocess

def main():
	networkLocation= '//hke.local/hma/Dept/Customer_Satisfaction/Service Training & Field Ops/Service Business Improvement/_Programs and Vendors/MSXi/_CCE/3 Dlr Contact'
	testLocation	= os.getcwd()+'//expressAssess_2_test'
	testFileName	= '/( SA) AL009 Tuscaloosa Hyundai DKimball 03-12-18.xls'
	sheetName		= 'Action Plan'
	testFileFullLocation = testLocation + testFileName
	masterReportClass = expressAssess_assessmentAnalysis_class.masterReportVariablesCls()

	os.chdir('H:/')

	for dirName, subdirList, fileList in os.walk(networkLocation):
		#print(dirName)
		#print(subdirList)
		#print(fileList)
		os.chdir(dirName)
		for fname in fileList:
			while(True):
				#print(fname)
				masterReportClass.currentFolder=dirName.split('\\')[-1]
				masterReportClass.currentFile=fname
				
				currentVisitParameters=expressAssess_assessmentAnalysis_class.assessmentSheetVariablesCls(fname)
				#check for correct file params (extensions, etc.)
				if findCorrectFile(fname)!=1:
					masterReportClass.printStatus("Bad File Extension")
					del currentVisitParameters
					break #onto the next file
				directory=os.getcwd()
				fullFilePath='{0}/{1}'.format(directory, fname)
				try:
					workbook=xlrd.open_workbook(fullFilePath)
					worksheet=workbook.sheet_by_name('Action Plan')
				except:
					masterReportClass.printStatus("No Action Plan")
					del currentVisitParameters
					try:
						closeAndDeleteWorkbook(workbook)
					except:
						pass
					break

				currentVisitParameters.finalResultsRow_Dict['DLR_CD']		=getDealerCode(dirName, fname)
				currentVisitParameters.finalResultsRow_Dict['DLR_NM']		=getDealerName(worksheet, currentVisitParameters.finalResultsRow_Dict['DLR_NM'])
				currentVisitParameters.finalResultsRow_Dict['DATE']			=getVisitDate(worksheet, workbook, currentVisitParameters.finalResultsRow_Dict['DATE'])
				
				if currentVisitParameters.finalResultsRow_Dict['DLR_CD']==0:
					masterReportClass.printStatus("No Dealer Code")
					del currentVisitParameters
					closeAndDeleteWorkbook(workbook)
					break #onto the next file, no dealer code
				if currentVisitParameters.finalResultsRow_Dict['DATE']==0:
					masterReportClass.printStatus("Before Earliest Visit Date")
					del currentVisitParameters
					closeAndDeleteWorkbook(workbook)
					break #onto the next file, no visit date OR visit is before 2017
				if currentVisitParameters.finalResultsRow_Dict['DLR_NM']==0:
					masterReportClass.printStatus("No Dealer Name")
					del currentVisitParameters
					closeAndDeleteWorkbook(workbook)
					break #onto the next file, no dealer name
				if checkForRowHeaders(worksheet, currentVisitParameters.finalResultsRow_Dict)!=1:
					masterReportClass.printStatus("Bad Row Headers")
					del currentVisitParameters
					closeAndDeleteWorkbook(workbook)
					break #onto the next file, the row heads are not in place
				
				#get row count for iteration
				rowCount=getRowCount(worksheet)
				for x in range(5,rowCount):
					if checkItemPerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['STATUS'])==1:
						singleTaskParams=expressAssess_assessmentAnalysis_class.actionPointVariablesCls(x)
						singleTaskParams.finalResultsRow_Dict['TASK_NO']	=getValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['TASK_NO'])
						singleTaskParams.finalResultsRow_Dict['ITEM']		=getValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['ITEM'])
						singleTaskParams.finalResultsRow_Dict['WHO']		=getValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['WHO'])
						singleTaskParams.finalResultsRow_Dict['DOES WHAT']	=getValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['DOES WHAT'])
						singleTaskParams.finalResultsRow_Dict['BY WHEN']	=getDateValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['BY WHEN'], workbook)
						singleTaskParams.finalResultsRow_Dict['STATUS']		=getValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['STATUS'])
						singleTaskParams.finalResultsRow_Dict['UPDATED']	=getDateValuePerRow(worksheet, x, currentVisitParameters.finalResultsRow_Dict['UPDATED'], workbook)
						singleTaskParams.finalResultsRow_Dict['DLR_CD']		=currentVisitParameters.finalResultsRow_Dict['DLR_CD']
						singleTaskParams.finalResultsRow_Dict['DLR_NM']		=currentVisitParameters.finalResultsRow_Dict['DLR_NM']
						singleTaskParams.finalResultsRow_Dict['DATE']		=currentVisitParameters.finalResultsRow_Dict['DATE']
						currentVisitParameters = addRowToVisitDF(singleTaskParams, currentVisitParameters)
						del singleTaskParams
				masterReportClass.printStatus("GOOD REPORT")
				masterReportClass = addVisitToMasterDF(currentVisitParameters, masterReportClass)

				del currentVisitParameters
				closeAndDeleteWorkbook(workbook)
				break

	iof.df_tocsv_extension(masterReportClass.finalMasterResults_DF,\
						'C:/Users/AWEBSTER/DESKTOP/ActionPointResults.csv'
						)
	iof.df_tocsv_extension(masterReportClass.errorLogResults_DF,\
						'C:/Users/AWEBSTER/DESKTOP/ActionPointDownloadLog.csv'
						)

def findCorrectFile(fname):
	fileExtension=fname.split('.')[-1]
	if fileExtension!='xls':
		return 0
	return 1

def getDealerCode(directoryName, fileName):
	try:
		tempDealerCode=directoryName.split('\\')
		tempDealerCode=tempDealerCode[-1].split('_')
		return tempDealerCode[0]
	except:
		print("{0} did not yield a dealer code, skipping file {1}".format(directoryName, fileName))
		return 0

def closeAndDeleteWorkbook(workbook):
	workbook.release_resources()
	del workbook
def getDealerName(actionPointWorksheet, dealerNameWorksheetLocation):
	try:
		tempDealerName=\
			actionPointWorksheet.row_slice(rowx=dealerNameWorksheetLocation[0])[dealerNameWorksheetLocation[1]].value
		if tempDealerName=='':
			return 0
		else:
			return tempDealerName
	except:
		return 0
def getVisitDate(actionPointWorksheet, actionPointWorkBook, visitDateWorksheetLocation):
	try:
		dateTemp_2=actionPointWorksheet.row_slice(rowx=visitDateWorksheetLocation[0])[visitDateWorksheetLocation[1]].value
	except:
		return 0

	try:
		dateTemp=datetime.datetime(*xlrd.xldate_as_tuple(actionPointWorksheet.row_slice(rowx=visitDateWorksheetLocation[0])[visitDateWorksheetLocation[1]].value,actionPointWorkBook.datemode))
		dtTemp=dateTemp.strftime("%Y-%m-%d")
		yearTemp=dateTemp.strftime("%Y")
		if int(yearTemp) in (2017,2018):
			return dtTemp
		else:
			return 0
	except:
		return dateTemp_2

def getRowCount(worksheet):
	return worksheet.nrows

def checkForRowHeaders(actionPointWorkSheet, locationDict):
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['ITEM'][0])[locationDict['ITEM'][1]].value.upper())
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['WHO'][0])[locationDict['WHO'][1]].value.upper())
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['DOES WHAT'][0])[locationDict['DOES WHAT'][1]].value.upper())
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['BY WHEN'][0])[locationDict['BY WHEN'][1]].value.upper())
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['STATUS'][0])[locationDict['STATUS'][1]].value.upper())
	#print(actionPointWorkSheet.row_slice(rowx=locationDict['UPDATED'][0])[locationDict['UPDATED'][1]].value.upper())
	if actionPointWorkSheet.row_slice(rowx=locationDict['ITEM'][0])[locationDict['ITEM'][1]].value.upper()!='ITEM':
		print('Could not find ITEM header')
		return 0
	if actionPointWorkSheet.row_slice(rowx=locationDict['WHO'][0])[locationDict['WHO'][1]].value.upper()!='WHO':
		print('Could not find WHO header')
		return 0
	if actionPointWorkSheet.row_slice(rowx=locationDict['DOES WHAT'][0])[locationDict['DOES WHAT'][1]].value.upper()!='DOES WHAT':
		print('Could not find DOES WHAT header')
		return 0
	if actionPointWorkSheet.row_slice(rowx=locationDict['BY WHEN'][0])[locationDict['BY WHEN'][1]].value.upper()!='BY WHEN':
		print('Could not find BY WHEN header')
		return 0
	if actionPointWorkSheet.row_slice(rowx=locationDict['STATUS'][0])[locationDict['STATUS'][1]].value.upper()!='STATUS':
		print('Could not find STATUS header')
		return 0
	if actionPointWorkSheet.row_slice(rowx=locationDict['UPDATED'][0])[locationDict['UPDATED'][1]].value.upper()!='UPDATED':
		print('Could not find UPDATED header')
		return 0
	return 1
def checkItemPerRow(actionPointWorksheet, x, statusLocation):
	tempStatus=actionPointWorksheet.row_slice(rowx=x)[statusLocation[1]].value
	if tempStatus.upper() in ('IN PROGRESS','COMPLETED','NOT STARTED'):
		return 1
	else:
		return 0
def getValuePerRow(actionPointWorksheet, x, valueLocationCoordinates):
	tempValue=actionPointWorksheet.row_slice(rowx=x)[valueLocationCoordinates[1]].value
	return tempValue
def getDateValuePerRow(actionPointWorksheet, x, valueLocationCoordinates, actionPointWorkBook):
	try:
		dateTemp=datetime.datetime(*xlrd.xldate_as_tuple(actionPointWorksheet.row_slice(rowx=x)[valueLocationCoordinates[1]].value,actionPointWorkBook.datemode))
		dtTemp=dateTemp.strftime("%Y-%m-%d")
		return dtTemp
	except: 
		actionPointWorksheet.row_slice(rowx=x)[valueLocationCoordinates[1]]
def addRowToVisitDF(taskClass, visitClass):
	#taskDF =  pd.DataFrame(taskClass.finalResultsRow_Dict, index=[0,]) 
	taskDF=pd.DataFrame([taskClass.finalResultsRow_Dict])
	visitDF = visitClass.finalVisitResults_DF
	visitClass.finalVisitResults_DF = pd.concat([visitDF, taskDF])
	return visitClass
def addVisitToMasterDF(visitClass, masterClass):
	visitDF = visitClass.finalVisitResults_DF
	masterDF = masterClass.finalMasterResults_DF
	masterClass.finalMasterResults_DF = pd.concat([visitDF, masterDF])
	return masterClass

main()

