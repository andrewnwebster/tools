from func_forAll import func_forAll as func
import pandas as pd

#dictionaries that define roDataScheduled_main.py
def getDataPullDictionary():
	##ENTER MONTHS BACK HERE (0 for current month only)
	##monthsInPast=13 for CCE and Exec summaries
	monthsPast=15
	dataPullDict={
		'surveys':{
			'monthsInPast':monthsPast,
			'filePreFix':'surveys',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData',
		},
	}
	
	return dataPullDict