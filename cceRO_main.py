# -*- coding: utf-8 -*-
"""
dealerData
Created on 20171011
@author: AWEBSTER
"""
import pyodbc
import pandas as pd
import datetime
import pickle
import os
import sys
from func_forAll import func_forAll as func


roMonthRange=[]

def inputroMonthRange():
	fourLoops=4
	while True:
		global roMonthRange
		temp = datetime.datetime.now()
		for x in range(4):
			temp=temp - datetime.timedelta(days=float(365/12))
			temp_0=temp - datetime.timedelta(days=float(365))
			temp_in=temp.strftime('%Y%m')
			temp_0_in=temp_0.strftime('%Y%m')
			roMonthRange.append(temp_in)
			roMonthRange.append(temp_0_in)
		break
def edChang_RO(yyyyMMIn):
	queryTemp=func.edChangRO(yyyyMMIn)
	return queryTemp
def cce_RO(yyyyMMIn):
	queryTemp=func.cceROQuery(yyyyMMIn)
	return queryTemp

def cust_pay_RO(yyyyMMIn):
	queryTemp=func.cpROQuery(yyyyMMIn)
	return queryTemp

def pyodbcConnect(odbcName):
	cnxn=pyodbc.connect(odbcName)
	print('connection open')
	cursor=cnxn.cursor()
	print('cursor open')
	return (cnxn, cursor)

def pyodbcClose(connection, cursor):
	cursor.close()
	print('cursor closed')
	connection.close()
	print('connection closed')
	pass

def updateHCRTimeStampPickle(hcrUpdatePickle,\
				hcrUpdatePickleDate,\
				dateDict):
	with open(hcrUpdatePickle, 'wb') as f:
			pickle.dump(dateDict, f)

def runHCRDatabase(hcrFullTableDir,\
				hcrFullTableDBFile,\
				#hcrUpdatePickle,\ commented out 20171212
				hcrUpdatePickleDate,\
				odbcName,\
				#lastUpdateHCR,\ commented out 20171212
				curDate,\
				dateDict,\
				user_input):
	counter=0
	global roMonthRange
	hcrFullTablePickleDB=''
	delimiterType=''
	tempDF=pd.DataFrame()
	if user_input=='3':
		roMonthRange=[
			'201602',
			'201602',
			'201603',
			'201604',
			'201605',
			'201606',
			'201607',
			'201608',
			'201609',
			'201610',
			'201611',
			'201612',
			'201701',
			'201702',
			'201703',
			'201704',
			'201705',
			'201706',
			'201707',
			'201708',
			'201709',
			'201710',
			'201711',
			'201712',
			'201801',
		]
	for x in roMonthRange:
		print(x)
		if user_input=='1':
			data=cce_RO(x)
			hcrFullTablePickleDB=hcrFullTableDir+hcrFullTableDBFile[0]
			delimiterType='|'
		elif user_input=='2':
			data=cust_pay_RO(x)
			hcrFullTablePickleDB=hcrFullTableDir+hcrFullTableDBFile[1]
			delimiterType='|'
		elif user_input=='3':
			data=edChang_RO(x)
			hcrFullTablePickleDB=hcrFullTableDir+hcrFullTableDBFile[2]
			delimiterType='|'
		else:
			return 0
		(connection,cursor)=pyodbcConnect(odbcName)
		table = pd.read_sql(data, connection)
		table.to_csv(hcrFullTableDir+'\\'+str(x)+'_data_201801.csv', mode='w', index=False, sep=delimiterType)
		pyodbcClose(connection,cursor)
		print(table.describe())
		'''try:
			(connection,cursor)=pyodbcConnect(odbcName)
			table = pd.read_sql(data, connection)
			table.to_csv(hcrFullTableDir+'\\'+str(x)+'_data_201801.csv', mode='w', index=False, sep=delimiterType)
			pyodbcClose(connection,cursor)
			print(table.describe())
		except:
			if counter==10:
				sys.exit()
			print('failed query for '+str(x)+' saving')
			tempDF.to_csv(hcrFullTableDir+'\\'+str(counter)+'_data_201801.csv', mode='w', index=False, sep=delimiterType)
			counter+=1'''
			
		tempDF=pd.concat([tempDF,table])
		#we want to save our DF as a text file, NOT a pickle
		#df.to_pickle(hcrFullTablePickleDB)
	tempDF.to_csv(hcrFullTablePickleDB, mode='w', index=False, sep=delimiterType)

	#updateHCRTimeStampPickle(hcrUpdatePickle,\ commented out 20171212
	#			hcrUpdatePickleDate,\ 			commented out 20171212
	#			dateDict)						commented out 20171212
	del table
	try:
		pyodbcClose(connection,cursor)
	except:
		pass

def printTimeStamps(timeVar, timeVarStr):
	strVar1='---------------------------\n'
	timeVarStrFormatted='{:25.25}'.format(timeVarStr)

	printVar1='''%s
%s : %s
	'''%(
		strVar1
		,timeVarStrFormatted
		,timeVar
		)
	print(printVar1)

def main():
	time_start=0
	time_end=0

	inputDir=os.getcwd()
	hcrFullTableDBFile=('\CCERO_R4M.txt','\CUST_PAY_RO_R4M.txt', '\SURVEYS_201801.csv')

	inputroMonthRange()
	print(roMonthRange)
	hcrFullTableDir=inputDir
	#hcrUpdatePickle=inputDir+'\hcrLastUpdatedPickle.pickle'	commented out 20171212
	hcrUpdatePickleDate='currentDate'
	curDate=datetime.datetime.today().strftime('%Y%m%d')
	dateDict={}
	dateDict[hcrUpdatePickleDate]=curDate
	odbcName='DSN=as400;'
	
	while True:
	#	with open(hcrUpdatePickle, 'rb') as f:					commented out 20171212
	#		lastUpdateHCR=pickle.load(f)						commented out 20171212

		mainPrompt='''
TIME TO CHOOSE:
1--Update CCERO(TEST) text
2--Update CUSTPAY(TEST) text
3--20180124_Surveys
q--Quit
		'''
		input_1=input(mainPrompt)
		if input_1=='1' or input_1=='2'or input_1=='3':
			time_start=datetime.datetime.now().time()
			printTimeStamps(time_start, 'Start Time')
			runHCRDatabase(hcrFullTableDir,\
				hcrFullTableDBFile,\
				#hcrUpdatePickle,\								commented out 20171212
				hcrUpdatePickleDate,\
				odbcName,\
				#lastUpdateHCR,\								commented out 20171212
				curDate,\
				dateDict,\
				input_1)
			time_end=datetime.datetime.now().time()
			printTimeStamps(time_end, 'End Time')
		elif input_1=='q':
			print('Goodbye!')
			break
		else:
			print('Bad Entry...Try Again')
		print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')


main()

