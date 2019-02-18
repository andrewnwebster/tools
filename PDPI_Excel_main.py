# -*- coding: utf-8 -*-
"""
Created on 20171004
@author: AWEBSTER
"""

import os
import pandas as pd
import numpy as np
import datetime
import sys
from openpyxl import load_workbook

#local libraries
import PDPI_Excel_inputFileDict as ifd

'''
TO DO
_X_		PDPI Tableau data prep
__ 		FOS Tableau data prep

Variable Types:
(REQUIRED FOR ALL) -- DEALER PROGRAM
(REQUIRED FOR ALL) -- Region Code
(REQUIRED FOR ALL) -- District Code
(REQUIRED FOR ALL) -- Consultant
(REQUIRED FOR ALL) -- DEALER PROGRAM
(REQUIRED FOR ALL) -- PICK ONE COMBO

	WEIGHTED AVERAGES
	--HCR Rating / Count of HCR Rating
	--Would Recommend / Count of Would Recommend
	--Would Return / Count of RTN
	--Timeliness of Service / Count of Timely

	NUMERATOR SUM / DENOMINATOR SUM
	--Brand Numerator Sum / Brand Denominator Sum [Rtn - DEALER slide]
	--2ymc Numerator Sum / 2ymc Denominator Sum [Rtn - CMC slide]
	--Cp Gross Sum / Cp Saless [GP % Sales]

	YOY % Change
	1-([(YYYY-1)mm cpRoCount]/[YYYYmm cpRoCount]) [RO - CP Ct YOY % slide]
	1-([(YYYY-1)mm cpdol]/[YYYYmm cpdol]) [RO - CP dol YOY % slide]
	1-([(YYYY-1)mm totalroct]/[YYYYmm totalroct]) [RO -Total CT YOY % slide ]

'''

def dfMerger(leftDF, rightDF, keys, howJoin='left'):
	tempDF=pd.DataFrame()
	tempDF=pd.merge(leftDF\
					, rightDF\
					, left_on=keys['left']\
					, right_on=keys['right']\
					, how=howJoin)
	return tempDF

def createDealerNamePH(missingLink):
	temp=''
	temp=missingLink + ' Non-Prog'
	return temp

def wavg(group, avg_name, weight_name):
	""" http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
	In rare instance, we may not have weights, so just return the mean. Customize this if your business case
	should return otherwise.
	"""
	d = group[avg_name]
	w = group[weight_name]
	try:
		return (d * w).sum() / w.sum()
	except ZeroDivisionError:
		print('Zero division error in wavg')
		return (d * w).sum() / w.sum()

def divisionGroup(group, num, den):
	""" http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
	In rare instance, we may not have weights, so just return the mean. Customize this if your business case
	should return otherwise.
	"""
	num = group[num]
	den = group[den]
	try:
		return (num.sum() / den.sum())
	except ZeroDivisionError:
		print('Zero division error in divisionGroup')
		return (num.sum() / den.sum())

def yoydivisionGroup(num, den):
	num=float(num)
	den=float(den)
	return (den/num)-1

def dFgroupBy_3(sourceDFList, groupByList, renameList, dropList, joinKeys,newCol,num,den):
	temp=[]
	counter=0
	for x in sourceDFList:
		x = x.groupby(groupByList,as_index = False).sum()
		x = x.drop_duplicates()
		temp.append(x[x['DealerProgram']=='Non-Prog'])
		counter+=1
	df_2016=temp[0]
	df_2017=temp[1]
	df_2018=temp[2]
	keys=joinKeys
	tempDF=dfMerger(df_2016, df_2017, keys, howJoin='inner')
	tempDF_2=dfMerger(df_2017, df_2018, keys, howJoin='inner')
	tempDF=pd.concat([tempDF, tempDF_2])
	
	tempDF[newCol]=np.vectorize(yoydivisionGroup)(tempDF[num],tempDF[den])

	tempDF = tempDF.drop(dropList, 1)
	tempDF=tempDF.rename(columns=renameList)
	return tempDF

def dFgroupBy_2(sourceDF, groupByList, avg_name, weight_name, renameList):
	tempDF_2=sourceDF
	tempDF_2=tempDF_2.groupby(groupByList, as_index=True).apply(divisionGroup, avg_name, weight_name)
	tempDF_2=tempDF_2.reset_index()
	tempDF_2=tempDF_2[tempDF_2['DealerProgram']=='Non-Prog']
	tempDF_2=tempDF_2.rename(columns=renameList)
	#tempDF_2['AvgRating_Delta_1st_y']=tempDF['AvgRating_Delta_1st_y']

	print(tempDF_2.head(n=1))
	return tempDF_2

def dFgroupBy(sourceDF, groupByList, avg_name, weight_name, renameList):
	tempDF_2=sourceDF
	tempDF_2=tempDF_2.groupby(groupByList, as_index=True).apply(wavg, avg_name, weight_name)
	tempDF_2=tempDF_2.reset_index()
	tempDF_2=tempDF_2[tempDF_2['DealerProgram']=='Non-Prog']
	tempDF_2=tempDF_2.rename(columns=renameList)
	#tempDF_2['AvgRating_Delta_1st_y']=tempDF['AvgRating_Delta_1st_y']

	print(tempDF_2.head(n=1))
	return tempDF_2

def todaysDateString():
	return datetime.datetime.today().strftime('%Y%m%d_%H%M')

def timeStampedLog(inputString):
	stringTime=str(todaysDateString())
	print('\n['+stringTime+'] ['+inputString+']\n')

def excelToDf(path):
	temp=pd.DataFrame()
	try:
		try:
			temp=pd.read_excel(path, 'Sheet1')
		except:
			temp=pd.read_csv(path)
	except:
		print('\n\n\nMissing Input File -- Goodbye!\n\n\n')
		sys.exit()
	#manipulate source DF
	return temp

def consultantKeyDF(sourceDF, groupbyKeys):
	consultBaseDF=sourceDF
	consultBaseDF=consultBaseDF[groupbyKeys].drop_duplicates()
	consultBaseDF=consultBaseDF[consultBaseDF['CONSULTANT']!='Non-Prog']
	print(consultBaseDF.head(n=1))
	timeStampedLog('^ consultant keys generated')
	return consultBaseDF

def dfToExcel(dFList, path, sheetsList):
	counter=0
	writer=pd.ExcelWriter(path)  
	for x in dFList:
	    x.to_excel(writer, sheet_name=sheetsList[counter], index=False)
	    counter+=1
	    print('.')
	writer.save()

def consultantDealerKeysGen(tempDF, fiLter):
	consultantDealerKeysDF=tempDF[tempDF['CONSULTANT']!='Non-Prog']
	consultantDealerKeysDF=consultantDealerKeysDF[fiLter].drop_duplicates()
	print(consultantDealerKeysDF)
	timeStampedLog('^ consultant keys')
	return consultantDealerKeysDF

def mergewithConsultantKeys(sourceDF,consultantDealerKeysDF, fiLters):
	sourceDF=dfMerger(sourceDF, consultantDealerKeysDF, fiLters)
	sourceDF['CONSULTANT']=sourceDF['CONSULTANT'].fillna('Non-Prog')
	print(sourceDF[sourceDF['CONSULTANT']!='Non-Prog'].head(n=1))
	timeStampedLog('^ consultant check')
	return sourceDF

def reformatPercentage(percentageFormat):
	try:
		temp=str(percentageFormat)[:-1]
		temp=float(temp)/100
		return temp
	except:
		return percentageFormat

def main():
	timeStampedLog('Tableau to Excel Version 2 Initializing...')
	inputFileDict=ifd.inputFileDict()
	timeStampedLog('Input Dictionary Loaded')

	for key,value in inputFileDict.items():

		outDir=value['outputFileDir']+'/'+value['outputFileName']
		outSheet=value['outputFileSheet']
		inDir=value['inputFileDir']+'\\PDPI_in\\'+key

		print('Input Full Directory: ' + inDir)
		print('Output Directory: '+outDir)
		print('Output Sheet: '+ outSheet[0])

		print(value['baseRenameKeys'])

		sourceDF=excelToDf(inDir)
		sourceDF=sourceDF.rename(columns=value['baseRenameKeys'])

		#combine date columns for consistency of operations
		#there are dependent downstream dt operations for best
		#time-series graphing practices
		if key[0]=='3':
			pass
			sourceDF[value['oldDT']]=sourceDF[value['monthColumn']].astype(str)+' '+\
										sourceDF[value['yearColumn']].astype(str)
			sourceDF[value['yearColumn']]=sourceDF[value['yearColumn']].astype(int)
			sourceDF[value['year+1Column']]=sourceDF[value['yearColumn']]+1
			print(sourceDF.head(n=1))
			timeStampedLog('^ date operations complete')
		
		#standard datetime operations, converting from
		#example old format: January 2017 (string)
		#example new format: 201701 (numeric)
		sourceDF[value['newDT']]=\
		sourceDF[value['oldDT']].\
		apply(lambda s: int(datetime.datetime.strptime(s,"%B %Y").\
			strftime('%Y%m')))
		print(sourceDF.head(n=1))
		timeStampedLog('^ datetime modified to yyyymm')

		#special operations for YOY calculations
		if key[0]=='3':
			df_2016=sourceDF[sourceDF['yearVar']==2016]
			df_2017=sourceDF[sourceDF['yearVar']==2017]
			df_2018=sourceDF[sourceDF['yearVar']==2018]
			sourceDF=sourceDF[(sourceDF['yearVar']==2018) | (sourceDF['yearVar']==2017)]

			#dfToExcel([df_2016, df_2017], 'test_init.xlsx', ['sheet1','sheet2'])

			df_2018[value['reformatColumn']]=np.vectorize(reformatPercentage)\
															(df_2018[value['reformatColumn']])
			
			df_2017[value['reformatColumn']]=np.vectorize(reformatPercentage)\
															(df_2017[value['reformatColumn']])
			sourceDF[value['reformatColumn']]=np.vectorize(reformatPercentage)\
															(sourceDF[value['reformatColumn']])
			consultantDealerKeysDF=consultantDealerKeysGen(tempDF, value['consultantDealerKeysDF'])

			natlDf=dFgroupBy_3([df_2016, df_2017, df_2018],\
				value['natlGroupBy'],\
				value['regionDistrictNatlRenameDict'],\
				value['regionDistrictNatlDropList'],\
				value['20162017_NATL_JoinKeys'],\
				value['reformatColumn'],
				value['num'],
				value['den'])
			timeStampedLog('^ national YOY complete')
			districtDf=dFgroupBy_3([df_2016, df_2017, df_2018],\
				value['districtGroupBy'],\
				value['regionDistrictNatlRenameDict'],\
				value['regionDistrictNatlDropList'],\
				value['20162017_DIST_JoinKeys'],\
				value['reformatColumn'],
				value['num'],
				value['den'])
			timeStampedLog('^ district YOY complete')
			
			regionDf=dFgroupBy_3([df_2016, df_2017, df_2018],\
				value['regionGroupBy'],\
				value['regionDistrictNatlRenameDict'],\
				value['regionDistrictNatlDropList'],\
				value['20162017_REGN_JoinKeys'],\
				value['reformatColumn'],
				value['num'],
				value['den'])
			timeStampedLog('^ region YOY complete')
			
			sourceDF=mergewithConsultantKeys(sourceDF,consultantDealerKeysDF, value['consultantDealerJoins'])
			consultBaseDF=consultantKeyDF(sourceDF, value['consultKeyGroupBy'])

			#dfToExcel([natlDf,districtDf,regionDf, sourceDF,consultantDealerKeysDF, consultBaseDF], 'test_init.xlsx', ['nat','dis','reg', 'source','consultants','consultBaseDF'])
			#break

		#special operations for retention calculations
		if key[0]=='2':
			#we need consultants in sourceDF, since they're missing
			consultantDealerKeysDF=consultantDealerKeysGen(tempDF, value['consultantDealerKeysDF'])

			timeStampedLog('^ initiating retention calculations')
			sourceDF_1=sourceDF[np.isfinite(sourceDF['Num'])]
			sourceDF_2=sourceDF[np.isfinite(sourceDF['Den'])]
			print(sourceDF_2.head(n=1))
			timeStampedLog('^ removed NULL Den')
			print(sourceDF_1.head(n=1))
			timeStampedLog('^ removed NULL Num')

			inDF=dfMerger(sourceDF_1, sourceDF_2, value['initialJoinGroup'])
			inDF=mergewithConsultantKeys(inDF,consultantDealerKeysDF, value['consultantDealerJoins'])

			inDF = inDF.drop(value['inDfDropList'], 1)
			inDF=inDF.rename(columns=value['baseRenameKeys'])

			print(inDF[inDF['CONSULTANT']=='Non-Prog'].head(n=1))
			timeStampedLog('^ ready for operations')

			sourceDF=inDF[inDF['CONSULTANT']!='Non-Prog']
			inDF=inDF[inDF['CONSULTANT']=='Non-Prog']

			num='Num'
			den='Den'

			districtDf=dFgroupBy_2(inDF,value['districtGroupBy'], num, den, value['districtRenameKeys'])
			timeStampedLog('^ district non program averages complete')
			regionDf=dFgroupBy_2(inDF,value['regionGroupBy'], num, den, value['regionRenameKeys'])
			timeStampedLog('^ region non program averages complete')
			natlDf=dFgroupBy_2(inDF,value['natlGroupBy'], num, den, value['natlRenameKeys'])
			timeStampedLog('^ national non program averages complete')

			consultBaseDF=consultantKeyDF(sourceDF, value['consultKeyGroupBy'])

		#special operations for hcr weighted averages
		if key[0]=='1':
			consultBaseDF=consultantKeyDF(sourceDF, value['consultKeyGroupBy'])
			
			inDF=sourceDF
			tempDF=sourceDF
			inDF=inDF[inDF['CONSULTANT']=='Non-Prog']

			timeStampedLog('^ initiating HCR calculations')
			avg_name=value['avg_name']
			weight_name=value['weight_name']

			districtDf=dFgroupBy(inDF,value['districtGroupBy'], avg_name, weight_name, value['districtRenameKeys'])
			timeStampedLog('^ district non program averages complete')
			regionDf=dFgroupBy(inDF,value['regionGroupBy'], avg_name, weight_name, value['regionRenameKeys'])
			timeStampedLog('^ region non program averages complete')
			natlDf=dFgroupBy(inDF,value['natlGroupBy'], avg_name, weight_name, value['natlRenameKeys'])
			timeStampedLog('^ national non program averages complete')

		regionDf['DEALER']=np.vectorize(createDealerNamePH)\
			(regionDf['RegionCode'])
		print(regionDf.head(n=1))
		timeStampedLog('^ regionDf dealer placeholder added')

		districtDf['DEALER']=np.vectorize(createDealerNamePH)\
			(districtDf['DIST'])
		print(districtDf.head(n=1))
		timeStampedLog('^ districtDf dealer placeholder added')

		natlDf['DEALER']=np.vectorize(createDealerNamePH)\
			(natlDf['NATL'])
		print(natlDf.head(n=1))
		timeStampedLog('^ natlDf dealer placeholder added')

		regionDf=dfMerger(consultBaseDF, regionDf, value['regionJoinKeys'])
		print(regionDf.head(n=1))
		timeStampedLog('^ regionDf All merged up')

		districtDf=dfMerger(consultBaseDF, districtDf, value['distJoinKeys'])
		print(districtDf.head(n=1))
		timeStampedLog('^ districtDf All merged up')

		natlDf=dfMerger(consultBaseDF, natlDf, value['natlJoinKeys'])
		print(natlDf.head(n=1))
		timeStampedLog('^ natlDf All merged up')

		finalDf=pd.concat([regionDf\
					, districtDf\
					, natlDf\
					, sourceDF[sourceDF['DealerProgram']=='Prog']])

		print(finalDf.head(n=1))

		finalDf = finalDf.drop(value['baseDfDropList'], 1)
		print(finalDf[finalDf['DealerProgram']=='Non-Prog'].drop_duplicates())
		timeStampedLog(key+' ^ Done!')

		dFToSave=[finalDf.apply(pd.to_numeric, errors='ignore')]
		sheetsToSaveOn=value['outputFileSheet']
		outFullDir=outDir
		dfToExcel(dFToSave, outFullDir, sheetsToSaveOn)

main()