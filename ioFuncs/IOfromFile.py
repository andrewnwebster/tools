# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd
import time
import os
from datetime import datetime
from datetime import timedelta

#NEVER CHANGE A CURRENT FUNCTION
#MAKE NEW COPIES
#PROGRAMS MAY BE DEPENDENT ON THE SPECIFIC FUNCTIOn

def fromCSV(filePath, fileName):
	tempFilePath=filePath+fileName
	tempDF=pd.read_csv(tempFilePath)
	return tempDF

def toCSV(pathList, fileName, dFrame, index=False):
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

def df_tocsv_extension(dFrame, tempPath, index=False):
	#dFrame.to_csv(tempPath, index=index)
	try:
		print('SAVE SUCCESS: '+tempPath+' ...')
		dFrame.to_csv(tempPath, index=index)
	except:
		print('SAVE ERROR: '+tempPath+' ...\n...not a valid path!')