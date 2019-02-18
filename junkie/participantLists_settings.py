from func_forAll import func_forAll as func
import pandas as pd

#dictionaries that define participantLists
def getDataPullDictionary():
	dataPullDict={
		'CCE':{
			'filePreFix':'cce',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData/programs',
			'columnNames':['DEALER_CODE','CONSULTANT', 'INSTALL_DT','CERTIFY_DT','MOSTRECENTVISIT'],
			#kinda useless right now -- unless we want to track participation by month
			'monthsInPast':13,
		},

		'FOS':{
			'filePreFix':'fos',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData/programs',
			'columnNames':['DEALER_CODE','CONSULTANT', 'INSTALL_DT','MOSTRECENTVISIT'],
			#kinda useless right now -- unless we want to track participation by month
			'monthsInPast':13,
		},
		'PDPI':{
			'filePreFix':'pdpi',
			'outputFileLocation':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/PS_Merchandising/Contractor/Database/CCE DB Source Files/pyData/programs',
			'columnNames':['DEALER_CODE','CONSULTANT', 'INSTALL_DT','MOSTRECENTVISIT'],
			#kinda useless right now -- unless we want to track participation by month
			'monthsInPast':13,
		},

	}
	return dataPullDict