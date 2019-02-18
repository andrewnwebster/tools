# -*- coding: utf-8 -*-

from func_forAll import func_forAll as func

import roDataScheduled_class as rodsc
import roDataScheduled_settings as dataSettings

import os

def historyLogParams():
	historyLogDict={
		'location':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/CCE Program/Database',
		'filename':'dataLog.txt'
	}
	return historyLogParams

def main():
	importDict=dataSettings.getDataPullDictionary()
	for k,v in importDict.items():
		x=rodsc.roDataScheduled(v)
		x.monthRangeArray=x.getYyyyMmList(x.reportYearMonth, x.reportMonthsPast)
		for y in x.monthRangeArray:
			if x.className=='cpRO':
				x.sqlData=x.cpRODataQuery(y)
			elif x.className=='cceRO':
				x.sqlData=x.cceRODataQuery(y)
			elif x.className=='allRO':
				x.sqlData=x.allRODataQuery(y)
			else:
				break
			x.as400Print(y)

#test main
def main_2():
	#print(func.getUpdatedThisMonthResults(os.getcwd(), 'cceRO_main.py'))
	func.errorLog('//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/CCE Program/Database'\
					,'test.txt'\
					,'hello world!')	
main()

