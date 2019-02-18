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

def dataWindow():
	queryTemp='''
		SELECT A.* 
		from (
			select A.*,INT(A.SUSDAT)/100 as YYYYmm
			from capdat.csurel00 A
			where A.survfy=1
		)A, 
		(
		select INT(YEAR(CURRENT_DATE-1 YEAR)) * 100 + MONTH(CURRENT_DATE - 4 MONTH) as limitYYYYmm 
		from sysibm.sysdummy1
		) aa
		where A.YYYYmm>201708
	'''

	queryTempSingleMonth='''
	'''
	return queryTemp

def joinDealerProperties():
	pass

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
	with open(fullDir, 'wb') as f:
			pickle.dump(dateDict, f)

def runHCRDatabase(hcrFullTablePickleDB,\
				hcrUpdatePickle,\
				hcrUpdatePickleDate,\
				odbcName,\
				lastUpdateHCR,\
				curDate,\
				dateDict):
	(connection,cursor)=pyodbcConnect(odbcName)
	data=dataWindow()
	table = pd.read_sql(data, connection)
	print(table.head())
	df.to_pickle(hcrFullTablePickleDB)
	updateHCRTimeStampPickle(hcrUpdatePickle,\
				hcrUpdatePickleDate,\
				curDate,\
				dateDict)
	del df
	pyodbcClose(connection,cursor)

def performHCROperations():
	pass

def main():
	outputDir='H:/pyData'

	inputDir=os.getcwd()
	hcrFullTablePickleDB=inputDir+'\hcrDatabase.pickle'
	hcrUpdatePickle=inputDir+'\hcrLastUpdatedPickle.pickle'
	hcrUpdatePickleDate='currentDate'
	curDate=datetime.datetime.today().strftime('%Y%m%d')
	dateDict={}
	dateDict[hcrUpdatePickleDate]=curDate

	outputFileName='\hcrAvg.csv'
	outputFilePath=outputDir+outputFileName

	odbcName='DSN=as400;'
	
	with open(hcrUpdatePickle, 'rb') as f:
		lastUpdateHCR=pickle.load(f)

	mainPrompt='''
TIME TO CHOOSE:
1--Update CSUREL00 pickle (last Update: %s)
2--Run Script from CSUREL00 pickle
q--Quit
	'''%(lastUpdateHCR[hcrUpdatePickleDate])

	while True:
		input_1=input(mainPrompt)
		if input_1=='1':
			runHCRDatabase(hcrFullTablePickleDB,\
				hcrUpdatePickle,\
				hcrUpdatePickleDate,\
				odbcName,\
				lastUpdateHCR,\
				curDate,\
				dateDict)
		elif input_1=='2':
			performHCROperations()
		elif input_1=='q':
			print('Goodbye!')
			break
		else:
			print('Bad Entry...Try Again')
		print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
'''

'''
main()

