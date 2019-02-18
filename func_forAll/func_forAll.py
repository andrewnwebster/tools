# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd
import time
import os
from datetime import datetime
from datetime import timedelta

from queries import cceRO
from queries import cpRO
from queries import allRO
from queries import surveys
from queries import edChangRequest

#NEVER CHANGE A CURRENT FUNCTION
#MAKE NEW COPIES
#PROGRAMS MAY BE DEPENDENT ON THE SPECIFIC FUNCTIOn

def saveExcel():
	pass

def pyodbcConnect(odbcName=''):
	#print(odbcName)
	time.sleep(1)
	if odbcName=='':
		return 0
	cnxn=pyodbc.connect(odbcName)
	cursor=cnxn.cursor()
	return (cnxn, cursor)

def pyodbcClose(connection, cursor):
	cursor.close()
	connection.close()

def runAS400Query(queryText='', odbcVarName=''):
	if queryText=='':
		return 0
	if odbcVarName=='':
		return 0
	(connection,cursor)=pyodbcConnect(odbcVarName)
	resultsDF=pd.read_sql(queryText, connection)
	pyodbcClose(connection,cursor)
	return resultsDF

def cleanStrInput(prompt='', strLength=0):
		if prompt=='':
			return 0
		if strLength==0:
			return 0

		while True:
			tempInput=input(prompt)
			if len(tempInput)<=strLength:
				return tempInput
			else:
				print('bad strLength -- reEntry required')

def getDFHeaders(tempDF=pd.DataFrame()):
	if tempDF.empty:
		return 0

	tempResults=list(tempDF.columns.values)
	return tempResults

def dfToExcel(dFList=[], path='', sheetsList=[]):
	counter=0
	writer=pd.ExcelWriter(path)  
	for x in dFList:
	    x.to_excel(writer, sheet_name=sheetsList[counter], index=False)
	    print('.')
	    counter+=1
	writer.save()



def financialStatementReportQuery(reportYear, varPreFix, bmCode):
	return """
				SELECT DISTINCT 
					DTDELR
					, DTREGN
					, DTDSCD
					, BFYEAR as {0}YEAR
					, BFJAN as {0}JAN
					, BFFEB as {0}FEB
					, BFMAR as {0}MAR
					, BFAPR as {0}APR
					, BFMAY as {0}MAY
					, BFJUN as {0}JUN
					, BFJUL as {0}JUL
					, BFAUG as {0}AUG
					, BFSEP as {0}SEP
					, BFOCT as {0}OCT
					, BFNOV as {0}NOV
					, BFDEC as {0}DEC

				FROM DLPDAT.DLFSTP, DLPDAT.DLDLTP

				WHERE (
					BFFLID ='{1}'
					AND BFDELR = DTDELR
					AND DTSTAF = 'A'
					AND BFYEAR = '{2}'
				)
	""".format(varPreFix,bmCode,reportYear)

def concatDataFrames(dataframeList):
	return pd.concat(dataframeList)

def joinDataFrames(framesDict, keysDict, joinType='inner'):
	tempDF=pd.DataFrame()
	leftDF=framesDict['left']
	leftKey=keysDict['left']
	#print(leftDF.head())
	#print(leftKey)

	rightDF=framesDict['right']
	rightKey=keysDict['right']
	#print(rightDF.head())
	#print(rightKey)

	return pd.merge(leftDF, rightDF, left_on=leftKey, right_on=rightKey, how=joinType)

def df_tocsv(dFrame, pathList=[], fileName='', index=False):
	if pathList==[]:
		return 0
	elif fileName=='':
		return 0
	else:
		for x in pathList:
			if isinstance(fileName, str):
				tempPath=x+fileName
				df_tocsv_extension(dFrame, tempPath, index)
			elif isinstance(fileName, list):
				for y in fileName:
					tempPath=x+y
					df_tocsv_extension(dFrame, tempPath, index)
			else:
				return 0

def df_tocsv_extension(dFrame, tempPath, index):
	try:
		print('SAVE SUCCESS: '+tempPath+' ...')
		dFrame.to_csv(tempPath, index=index)
	except:
		print('SAVE ERROR: '+tempPath+' ...\n...not a valid path!')

def joinStrings(strList):
	temp=''
	for x in strList:
		try:
			temp+=x
		except:
			print('FAIL: joinStrings')
	return temp

def findCurMonthVar(reportMonth, calendarConversionKeys, BFFLID):
	temp=int(reportMonth)
	temp_Month=BFFLID+calendarConversionKeys[temp]
	return temp_Month

def replaceValue(paramValue, old, new):
	if paramValue is not None:
		return paramValue.replace(old, new)
	else:
		return ""

def edChangRO(yyyyMMIn, additionalParams=0):
	return edChangRequest.edChangQuery(yyyyMMIn, additionalParams)

def allRODataQuery(yyyyMMIn, additionalParams=0):
	return allRO.allRO(yyyyMMIn, additionalParams)

def cceROQuery(yyyyMMIn, additionalParams=0):
	return cceRO.cceROQuery(yyyyMMIn, additionalParams)

def cpROQuery(yyyyMMIn, additionalParams=0):
	return cpRO.cpROquery(yyyyMMIn, additionalParams)

def getSurveySQLQuery(yyyyMMIn, additionalParams=0):
	return surveys.surveysQuery(yyyyMMIn, additionalParams)


#function input string yyyymm
#function inpout monthsPast to add to array
#function output array of yyyyMM : 
#	[start month--> x -->(start month - monthsPast)]
def getYyyyMmList(startMonth, monthsPast):
	tempList=[startMonth]
	startmm=int(startMonth[-2:])
	startyyyy=int(startMonth[:4])
	year_temp=startyyyy
	
	for x in range(monthsPast):
		startmm+=-1
		month=startmm%12
		if month==0:
			month_str=str(12)
			startmm=12

			year_temp+=-1
		elif month<10 and month>0:
			month_str='0'+str(month)
		else:
			month_str=str(month)
		year_str=str(year_temp)
		tempList.append(year_str+month_str)
	return tempList

#input report YYYYmm string.
#outputs arrays dicti0nary
#	current year months list
#	previous year months list
def getCurPrevYearMonths(reportingMonthStr, varPreFix, convDict=''):
	calendarConversionKeys=convDict
	if calendarConversionKeys=='':
		calendarConversionKeys=\
			{
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

	reportingMonthStr=int(reportingMonthStr)

	curYear=[]
	prevYear=[]
	max=13
	min=1

	for x in range(min,reportingMonthStr+1):
		curYear.append(varPreFix+calendarConversionKeys[x]) 
	for x in range(reportingMonthStr, max):
		prevYear.append(varPreFix+calendarConversionKeys[x]) 

	prevYear.pop(0)

	tempDict=\
			{
				'curYearMonths':curYear,
				'prevYearMonths':prevYear,
			}

	return tempDict

#no inputs
#outputs dictionary of strings
#	last 'yyyyMM' (ex. 201711 if today is 20171201)
#	last month 'yyyy'
#	last month last year 'yyyy'
#	last month 'mm'
def reportYYYYmmStats():

	todaysDate=datetime.today()

	
	reportYear=str(todaysDate.year)
	reportMonth=str(todaysDate.month-1)
	reportprevYear=str(todaysDate.year-1)

	todaysYear=str(todaysDate.year)
	todayMonth=str(todaysDate.month)

	if len(todayMonth)==1:
		todayMonth=addLeadingZero(todayMonth)

	if len(reportMonth)==1 and reportMonth!='0':
		reportMonth=addLeadingZero(reportMonth)

	elif reportMonth=='0':
		reportMonth='12'
		reportYear=str(todaysDate.year-1)
		reportprevYear=str(todaysDate.year-2)
	else:
		reportMonth=str(reportMonth)

	reportDay=str(todaysDate.day)
	if len(reportDay)==1:
		reportDay=addLeadingZero(reportDay)

	reportYYYYmmDD=reportYear+reportMonth+reportDay
	todayYYYYmmDD=todaysYear+todayMonth+reportDay
	firstDayTodayYYYYmm=todaysYear+todayMonth+'01'
	reportYYYYmm=reportYear+reportMonth

	#ALL STRINGS
	return{
		'reportYYYYmm':reportYYYYmm,
		'reportYYYY':reportYear,
		'report_prevYr':reportprevYear,
		'reportMonth':reportMonth,
		'reportYYYYmmDD':reportYYYYmmDD,
		'todayYYYYmmDD':todayYYYYmmDD,
		'firstDayTodayYYYYmm':firstDayTodayYYYYmm,

	} 

def addLeadingZero(inputStr):
	temp=str(inputStr)
	temp='0'+temp
	return temp

def getFolderFileList(directory):
	return os.listDir(directory)

def getFileUpdateTime(fileDir, fileName):
	fileStats = os.stat(fileDir+'/'+fileName)
	lastModifiedTS = datetime.fromtimestamp(fileStats.st_mtime)
	lastModifiedYYYYmmDD=str.replace(str(lastModifiedTS)[0:10],'-','')

	finalDict=\
		{
		'updateTS':lastModifiedTS,
		'updateYYYYmmDD':lastModifiedYYYYmmDD,
		}

	return finalDict

#input filename and filedirectory
#returns 1 if updated between first day of this month and today (inclusive)
#returns 0 if not
def getUpdatedThisMonthResults(fileDir, fileName, reportMonthCheckFlag=1):
	try:
		temp = getFileUpdateTime(fileDir, fileName)
	except:
		return [0,fileName+' does not exist, running query...']
	tempUpdated = temp['updateYYYYmmDD']

	temp_2 = reportYYYYmmStats()
	
	tempDates =\
		{
			'min':temp_2['firstDayTodayYYYYmm'],
			'max':temp_2['todayYYYYmmDD'],
		}
	minDate=tempDates['min']
	maxDate=tempDates['max']
	
	if reportMonthCheckFlag==0:
		return [0,fileName+' skipping update check, running query...']
	elif minDate<=tempUpdated<=maxDate:
		return [1,fileName+' up to date, skipping query...']
	else:
		return [0,fileName+' outdated, running query...']


#provide directory location, filename and desired message
#no output, writes to history log
def errorLog(location, filename, msg):
	now_formatted=datetime.now().strftime("%Y-%m-%d %H:%M")
	updateMsg='['+now_formatted+'] '+msg+'\n'
	fullFileName=location+'/'+filename
	if os.path.exists(fullFileName):
		f = open(fullFileName, 'a')
	else:
		f = open(fullFileName, 'w')
	f.write(updateMsg)
	f.close()