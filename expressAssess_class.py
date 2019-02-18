from func_forAll import func_forAll as func
from ioFuncs import IOfromFile as iof
import historyLog_settings as historySettings
import pandas as pd
import numpy as np

class assessmentSheetVariablesCls:
	def __init__(self):
		self.observationCompletionFlag=0 #prompts file save
		self.test2Flag=0
		self.testKey_1='Vehicle #1 Observation'
		self.testKey_2='Vehicle #2 Observation'

		self.taskDF=pd.DataFrame()
		self.testDF=pd.DataFrame()
		self.sheetDF=pd.DataFrame()

		self.totalTimeStr='Total Time'
		self.totalTestTimeTemp=0
		self.sheetTotalScoreFromTasks=0
		self.sheetTotalScoreFromTotalTime=0

		self.taskRenameDict={
			'Customer Arrived Until Greeted':'1. Customer Arrived Until Greeted',
			'Reception & Write-Up':'2. Reception & Write-Up',
			'Vehicle Waiting In Service Drive After Write-Up':'3. Vehicle Waiting In Service Drive After Write-Up',
			'Vehicle Parked In Staging Area':'4. Vehicle Parked In Staging Area',
			'Vehicle Moved Into Express Bay And MPI Completed':'5. Vehicle Moved Into Express Bay And MPI Completed',
			'Time From MPI Completion To Vehicle Service Completion':'6. Time From MPI Completion To Vehicle Service Completion',
			'Vehicle staged for wash':'7. Vehicle staged for wash',
			'Vehicle wash completed':'8. Vehicle wash completed',
			'Vehicle Moved To Express Delivery Area':'9. Vehicle Moved To Express Delivery Area',
			'Invoicing / Payment - Cashier':'10. Invoicing / Payment - Cashier',
			'Service Delivery And Customer Leaves':'11. Service Delivery And Customer Leaves',
		}

		self.acceptableYears=['2015','2016','2017','2018']
