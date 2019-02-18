# -*- coding: utf-8 -*-

import smtplib
from importlib.machinery import SourceFileLoader

from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders

from func_forAll import func_forAll as func
from queries import dealers

import pandas as pd
import os

class ccePdfAutosendClass():
	def __init__(self):
		self.odbcVar='DSN=as400'
		self.credentialsLocation='H:/pw/OutlookCreds.py'
		self.reportRoot='//hke.local/HMA/Dept/Customer_Satisfaction/'\
							'Service Business Development/Service Business Improvement/'\
							'_Programs and Vendors/MSXi/_CCE/Reports/_CCE Dealer Performance/'
		
		self.reportingTimePeriods=func.reportYYYYmmStats()
		self.reportYYYYmm=self.reportingTimePeriods['reportYYYYmm']
		self.reportYYYY=self.reportingTimePeriods['reportYYYY']
		self.report_prevYr=self.reportingTimePeriods['report_prevYr']
		self.reportMonth=self.reportingTimePeriods['reportMonth']

	def getCCEsendList():
		pass

	#filters in parameters
	def getAS400Dealers():
		getQueryText=dealers.dealersQuery(dlrCity, dlrState, dlrZip, dlrRegion, dlrDistrict, dlrCode)
		return runAS400Query(getQueryText, self.odbcVar)
	def getReportMonth():
		pass
		#accept userinput if we don't like automatic number

	def getNetworkFileList():
		pass

class iteratedCompleteInputFile():
	def __init__(self):
		pass

	def joinInputs(self, DF1, DF2, keys1, keys2):
		framesDict={
				'left':DF1,
				'right':DF2,
		}

		joinKeys={
				'left':keys1,
				'right':keys2,
		}
		return func.joinDataFrames(framesDict, joinKeys)
	#params needed
		#dealer code
		#receipient name

	def textemail(self):
		return 'Sample TEXT email'
	def htmlemail(self):
		return 'SAMPLE HTML email'


class cceEmailListInputClass():
	def __init__(self):
		self.inputFileLocation='//hke.local/HMA/Dept/Customer_Satisfaction/'\
								'Service Business Development/Service Business Improvement/'\
								'_Programs and Vendors/MSXi/_CCE/Reports/'\
								'_CCE Dealer Performance/cce_monthly_emails/1_CCE_Contact_List.xlsx'
		self.inputFileSheet='DealerList_Test'

		self.inputFileKeys=['Dealer_Code']

	def importCCESendList(self):
		return pd.read_excel(self.inputFileLocation, self.inputFileSheet, skiprows=1)

class folderTraverse():
	def __init__(self):
		self.rootDirName='//hke.local/HMA/Dept/Customer_Satisfaction/'\
								'Service Business Development/Service Business Improvement/'\
								'_Programs and Vendors/MSXi/_CCE/Reports/'\
								'_CCE Dealer Performance/'

		self.reportingTimePeriods=func.reportYYYYmmStats()
		self.reportYYYYmm=self.reportingTimePeriods['reportYYYYmm']
		self.reportYYYY=self.reportingTimePeriods['reportYYYY']
		self.report_prevYr=self.reportingTimePeriods['report_prevYr']
		self.reportMonth=self.reportingTimePeriods['reportMonth']

		self.traversedFolders=['CE','EA','SC','SO','WE']
		self.acceptableFileExtensions=['PDF']

		self.folderTraverseKeys=['Dealer_Code']

		#self.rootDirsubName=str(reportYYYYmm)+'/'
		self.rootDirsubName='test/'

	def rootDirTraverse(self):
		tempDF=pd.DataFrame()

		for dirName, subdirList, fileList in os.walk(self.rootDirName+self.rootDirsubName):
			
			for fname in fileList:
				if dirName.split('/')[-1] in self.traversedFolders:
					if fname.split('.')[-1] in self.acceptableFileExtensions:
						#print(fname)
						singleFileDict={
							'Dealer_Code':fname.split('.')[0].split('_')[-2],
							'District':fname.split('.')[0].split('_')[-4],
							'Region':fname.split('.')[0].split('_')[-3],
							'filename':fname,
							'filepath':dirName+'/',
							'subject':  fname.split('.')[0].split('_')[-2]+\
										' - CCE Dealer Performance Report - '+\
										fname.split('.')[0].split('_')[-1]
						}
						singleFileDictDF=pd.DataFrame(singleFileDict, index=[0])
						tempDF=pd.concat([tempDF, singleFileDictDF])
		tempDF=tempDF.reset_index(drop=True)

		return tempDF

class finalStatistics():
	def __init__(self):
		pass	

class undeliverableProcess():
	def __init__(self):
		pass
