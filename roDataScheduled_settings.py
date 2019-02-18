from func_forAll import func_forAll as func
import pandas as pd

#dictionaries that define roDataScheduled_main.py
def getDataPullDictionary():

	monthsBack=15
	dataPullDict={
		'cpRO':{
			'monthsInPast':monthsBack,
			'filePreFix':'cpRO',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData',
		},

		'cceRO':{
			'monthsInPast':monthsBack,
			'filePreFix':'cceRO',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData',
		},
		'allRO':{
			'monthsInPast':monthsBack,
			'filePreFix':'allRO',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData',
		},

	}
	return dataPullDict