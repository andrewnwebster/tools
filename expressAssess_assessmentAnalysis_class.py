import pandas as pd

#child of assessmentSheet
#tuples represent single data-point values 
#tuples also represent column data-point header locations -- should be labeled below
class actionPointVariablesCls:
	def __init__(self, name):
		self.className=name 
		self.finalResultsRow_Dict={
			'DLR_CD'		:'',	#taken from directory name, no tuple to display
			'DLR_NM'		:(2,1),	#(row,col) -- value
			'DATE'			:(3,5),	#(row,col) -- value
			'TASK_NO'		:(0,1),	#nothing to display
			'ITEM'			:(4,2),	#(row,col) -- value of header!
			'WHO'			:(4,3),	#(row,col) -- value of header!
			'DOES WHAT'		:(4,4),	#(row,col) -- value of header!
			'BY WHEN'		:(4,5),	#(row,col) -- value of header!
			'STATUS'		:(4,6),	#(row,col) -- value of header!
			'UPDATED'		:(4,7),	#(row,col) -- value of header!
		}

class assessmentSheetVariablesCls(actionPointVariablesCls):
	def __init__(self, name):
		actionPointVariablesCls.__init__(self, name)
		self.className=name
		self.finalVisitResults_DF=pd.DataFrame()
		self.varLocations_Dict={
			'visit_date_title':(3,4),#(row,col)
		}
		
class masterReportVariablesCls:
	def __init__(self):
		self.finalMasterResults_DF=pd.DataFrame()
		self.errorLogResults_DF=pd.DataFrame()
		self.currentFolder=''
		self.currentFile=''


	def printStatus(self, textString):
		tempDict={
			'FOLDER':self.currentFolder,
			'FILE':self.currentFile,
			'STATUS':textString,
		}
		tempString="{0} ::: {1} ::: {2}".format(self.currentFolder,self.currentFile,textString)
		tempDF=pd.DataFrame([tempDict])
		self.errorLogResults_DF=pd.concat([self.errorLogResults_DF, tempDF])
		print(tempString)