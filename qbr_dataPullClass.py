from func_forAll import func_forAll as func
from ioFuncs import IOfromFile as iof
import historyLog_settings as historySettings
import pandas as pd
import numpy as np
import os


class historicalDataPull:
	def __init__(self):
		self.observationCompletionFlag=0 #prompts file save
		self.dataOutputFileLoc='/qbr_data'
		self.dataInputFileLoc='/qbr_data'
		self.outputFile='/output.csv'
		self.inputFile='/input.csv'
		self.inputDF=pd.DataFrame()
		self.outputDF=pd.DataFrame()
		self.whatwePullin=''
		self.queryTypes={
			'hcr':1,
			#'rotime':2,
			#'oilFilter':3,
			#'engFilter':4,
			#'cabinFilter':5,
			#'wipers':6,
			#'sameDayROs':7,
			#'cceRO':8,
		}
		self.intervalBenchmarks=[
									'preInstall',
									#'firstSA',
									#'certified_firstSA',
									#'certified_lastSA',
									#'installed',
									#'lastSA',
									'nonCCE'
								]
	def finalSave(self, saveName, dFrameName):
		iof.toCSV(pathList=[os.getcwd()+self.dataOutputFileLoc], fileName=saveName, dFrame=dFrameName, index=False)

	def queryWriter(self, startPeriod, endPeriod, dealerCode, cceInterval, queryTypeInt):
		if queryTypeInt==1:#hcr
			data='''
			select 
				AVG(case when CAPDAT.CSUREL00.SURRTE <> 0 then (CAPDAT.CSUREL00.SURRTE + CAPDAT.CSUREL00.SURATE)/2
					else CAPDAT.CSUREL00.SURATE end) as RATING
				, COUNT(CAPDAT.CSUREL00.SURATE ) as RATING_COUNT
				, CAPDAT.CSUREL00.SUDELR AS DEALER_CODE
				, CAPDAT.CSUREL00.SUSADV AS ADVISOR_HMA_NO
				, CAPDAT.CSUREL00.SUSTEC AS TECHNICIAN_HMA_NO
				, INT(CAPDAT.CSUREL00.SUSDAT/100) AS SURVEY_DATE
				, '{3}' as INTERVAL
			from capdat.csurel00
			where 1=1
				and INT(CAPDAT.CSUREL00.SUSDAT/100) > {0}
				and INT(CAPDAT.CSUREL00.SUSDAT/100) <= {1}
				and CAPDAT.CSUREL00.SUDELR ='{2}'
				and CAPDAT.CSUREL00.SURATE<>0
			GROUP BY CAPDAT.CSUREL00.SUDELR
				, INT(CAPDAT.CSUREL00.SUSDAT/100)
				, CAPDAT.CSUREL00.SUSTEC
				, CAPDAT.CSUREL00.SUSADV'''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==2:#rotime
			data='''
				select 
					AVG(roTable.roMinutes) as roMinutes
					,count(roTable.roMinutes) as countRoMinutes
					,roTable.RHRDLR as DEALER_CODE
					,INT(roTable.RHWODT/100) AS roMinutes_DATE  
					, '{3}' as interval
				from (
				select 
					CAPDAT.CAROHP.RHRDLR
					, CAPDAT.CAROHP.RHWODT
					, CAPDAT.CAROHP.RHWCDT
					, CAPDAT.CAROHP.RHSEQN
					, CAPDAT.CAROHP.RHTIM2
					, CAPDAT.CAROHP.RHTIM1
					, (CAPDAT.CAROHP.RHTIM2-CAPDAT.CAROHP.RHTIM1)/100 as roMinutes
					, CAPDAT.CAROHP.RHFVIN
				 from capdat.carohp
				where cast(VARCHAR_FORMAT(CAPDAT.CAROHP.RHTIM2, 'YYYYMMDD') as int) = CAPDAT.CAROHP.RHWCDT
				and CAPDAT.CAROHP.RHRDLR= '{2}'
				and INT(CAPDAT.CAROHP.RHWODT/100) > {0}
				and INT(CAPDAT.CAROHP.RHWODT/100) <= {1}
				and CAPDAT.CAROHP.RHCLBA + CAPDAT.CAROHP.RHCPTA + CAPDAT.CAROHP.RHCSBA < 150
				and CAPDAT.CAROHP.RHWODT=CAPDAT.CAROHP.RHWCDT
				) roTable

				join(
				Select distinct capdat.caropp.RPRDLR
				    , capdat.caropp.RPFVIN
				    , capdat.caropp.RPSEQN
				    , capdat.caropp.RPWODT
				From capdat.caropp 
				Where (capdat.caropp.RPCQPT Like '263%' 
				    Or capdat.caropp.RPDPRT Like 'H263%' 
				    Or capdat.caropp.RPDPRT Like 'KI263%' 
				    Or capdat.caropp.RPDPRT Like 'QL263%' 
				    Or capdat.caropp.RPDPRT Like 'Q263%' 
				    Or capdat.caropp.RPDPRT Like 'H-263%' 
				    Or capdat.caropp.RPDPRT Like 'K-263%' 
				    Or capdat.caropp.RPDPRT Like 'Q-263%' 
				    Or capdat.caropp.RPDPRT Like 'HY263%')
				    and capdat.caropp.RPRDLR= '{2}'
				    and INT(capdat.caropp.RPWODT/100) > {0}
				    and INT(capdat.caropp.RPWODT/100) <= {1}
				) roOps On roTable.RHRDLR = roOps.RPRDLR 
				    And roTable.RHFVIN = roOps.RPFVIN 
				    And roTable.RHSEQN = roOps.RPSEQN 
				    And roTable.RHWODT = roOps.RPWODT
				
				GROUP BY roTable.RHRDLR
					,INT(roTable.RHWODT/100) '''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==3:#oilFilter
			data='''
				Select 
				          SUM(CAPDAT.CAROPP.RPCPQT*CAPDAT.CAROPP.RPCPMT) as oilFilSales
				        , SUM(CAPDAT.CAROPP.RPCPQT) as oilFilCount
				        , capdat.caropp.RPRDLR as Dealer_Code
				        , INT(capdat.caropp.RPWODT/100) as oilFilYM
				        , '{3}' as INTERVAL
				From capdat.caropp 
				Where (
				        capdat.caropp.RPCQPT Like '263%' 
				        Or capdat.caropp.RPDPRT Like 'H263%' 
				        Or capdat.caropp.RPDPRT Like 'KI263%' 
				        Or capdat.caropp.RPDPRT Like 'QL263%' 
				        Or capdat.caropp.RPDPRT Like 'Q263%' 
				        Or capdat.caropp.RPDPRT Like 'H-263%' 
				        Or capdat.caropp.RPDPRT Like 'K-263%' 
				        Or capdat.caropp.RPDPRT Like 'Q-263%' 
				        Or capdat.caropp.RPDPRT Like 'HY263%'
				                )
				        and capdat.caropp.RPRDLR= '{2}'
				        and INT(capdat.caropp.RPWODT/100) >{0}
				        and INT(capdat.caropp.RPWODT/100) <= {1}
				        and CAPDAT.CAROPP.RPCPQT > 0
				GROUP BY
				        capdat.caropp.RPRDLR
				        , INT(capdat.caropp.RPWODT/100)'''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==4:#engFilter
			data='''
				Select 
		        SUM(CAPDAT.CAROPL0J.RPCPMT * CAPDAT.CAROPL0J.RPCPQT) as engFilSales
		        , SUM(CAPDAT.CAROPL0J.RPCPQT) as engFilCount
		        , CAPDAT.CAROPL0J.RPRDLR as dealer_Code
		        , INT(CAPDAT.CAROPL0J.RPWODT/100) as engFilYM
		        ,'{3}' as INTERVAL
				FROM CAPDAT.CAROPL0J
				WHERE 
				(
				(CAPDAT.CAROPL0J.rpcpqt)>0
				AND
				(
				(
				(CAPDAT.CAROPL0J.rpcqpt) Like '28113%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281134Z200%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281136A500%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%2811369000%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281137D100%') 
				OR  (
				(CAPDAT.CAROPL0J.rpcqpt) Like '28127B1000%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281134Z200%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281136A500%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%2811369000%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281137D100%')
				OR (
				(CAPDAT.CAROPL0J.rpcqpt) Like '28128B1000%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281134Z20%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281136A500%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%2811369000%' 
				And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%281137D100%')
				))
				And CAPDAT.CAROPL0J.RPRDLR = '{2}'
				And INT(CAPDAT.CAROPL0J.RPWODT/100) > {0}
				And INT(CAPDAT.CAROPL0J.RPWODT/100) <= {1}
				group by CAPDAT.CAROPL0J.RPRDLR
		        , INT(CAPDAT.CAROPL0J.RPWODT/100)
				'''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==5:#cabinFilter
			data='''
				select 
		        SUM(CAPDAT.CAROPL0J.RPCPMT * CAPDAT.CAROPL0J.RPCPQT) as cabFilSales
		        , SUM(CAPDAT.CAROPL0J.RPCPQT) as cabFilCount
		        , CAPDAT.CAROPL0J.RPRDLR as dealer_Code
		        , INT(CAPDAT.CAROPL0J.RPWODT/100) as cabFilYM
		        ,'{3}' as INTERVAL
				FROM CAPDAT.CAROPL0J
				WHERE ((
		        (
		            CAPDAT.CAROPL0J.rpcqpt Like '08790%' 
		            And (CAPDAT.CAROPL0J.rpcqpt) Not Like '%0879001120%'
		        )
		        Or (CAPDAT.CAROPL0J.rpcqpt) Like 'C2H79AP000%' 
		        Or (CAPDAT.CAROPL0J.rpcqpt) Like '2SF79%' 
		        Or (CAPDAT.CAROPL0J.rpcqpt) Like '3SF79%' 
		        Or (CAPDAT.CAROPL0J.rpcqpt) Like '97133%' 
		        Or (CAPDAT.CAROPL0J.rpcqpt) Like '2BF79%')
		        and CAPDAT.CAROPL0J.RPCPQT > 0)
		        and CAPDAT.CAROPL0J.RPRDLR = '{2}'
		        and INT(CAPDAT.CAROPL0J.RPWODT/100) > {0}
		        and INT(CAPDAT.CAROPL0J.RPWODT/100) <= {1}
				GROUP BY CAPDAT.CAROPL0J.RPRDLR
		        , INT(CAPDAT.CAROPL0J.RPWODT/100)
				'''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==6:#wipers
			data='''
				select 
				SUM(CAPDAT.CAROPL0J.RPCPMT * CAPDAT.CAROPL0J.RPCPQT) as wiperSales
				, SUM(CAPDAT.CAROPL0J.RPCPQT) as wiperCount
				, CAPDAT.CAROPL0J.RPRDLR as dealer_Code
				, INT(CAPDAT.CAROPL0J.RPWODT/100) as wiperYM
				, '{3}' as INTERVAL
				FROM CAPDAT.CAROPL0J
				WHERE CAPDAT.CAROPL0J.rpcpqt>0
				And CAPDAT.CAROPL0J.RPRDLR = '{2}'
				And INT(CAPDAT.CAROPL0J.RPWODT/100) > {0}
				And INT(CAPDAT.CAROPL0J.RPWODT/100) <= {1}
				AND (CAPDAT.CAROPL0J.rpcqpt LIKE '%00009ADU00%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%1GH09AK014R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%1HH09AK012R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%1RH09AK011R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%26H09AK013R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%2BH09AK014R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%2EH09AK012R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%2VH09AK009R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%2WH09AK013R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%3JH09AK014R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%4DH09AK011R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983502M010%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983502M050%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983502V500%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983502W000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%9835034001%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983503J000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983503S000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983503S300%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983503X500%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983503X550%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98350B1000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98350G2000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983601G000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983601G001%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983602M010%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983602M050%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983602V500%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983602W000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603J000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603S000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603V000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603X000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603X100%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%983603X800%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98360A9500%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98360B1000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98360G2000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988201C000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%9882029600%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988202B000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988203J000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988501H000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988501R000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988502V000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988502W000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%988504D001%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98850A5000%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%98850C5100%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK014C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK016C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK016H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK017C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK018C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK018H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK019C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK020C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK020H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK022C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK024C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK024H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK026C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK026H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%99H09AK028C%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%A5H09AK013R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%C5H09AK012R%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000012HR%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000013%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000014%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000014HR%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000016%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000016H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000017%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000018%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000018H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000019%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000020%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000020H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000022%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000024%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000024H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000026%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000026H%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000028%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889000F24%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889001F20%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889002018%'
				OR CAPDAT.CAROPL0J.rpcqpt LIKE '%U889002F20%')
				group by CAPDAT.CAROPL0J.RPRDLR
				, INT(CAPDAT.CAROPL0J.RPWODT/100)
				'''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==7:#sameDayCPROs
			data='''
				SELECT 
					-- CAN BE REMOVED or CHANGED -- average of RO minutes by dealer/YM
					-- AVG(roTable.roMinutes) as roMinutes
					-- CAN BE REMOVED or CHANGED -- count of RO minute ROs
					-- ,count(roTable.roMinutes) as countRoMinutes

					-- KEEP
					roTable.RHRDLR as DEALER_CODE
					-- KEEP
					,INT(roTable.RHWODT/100) AS roDATE  
					-- KEEP
					, '{3}' as interval
					, count(roTable.RHRDLR) as roCount
					, sum(roTable.RHCTTA) as roSUM_$
				from (
				select 
					CAPDAT.CAROHP.RHRDLR
					, CAPDAT.CAROHP.RHWODT
					, CAPDAT.CAROHP.RHWCDT
					, CAPDAT.CAROHP.RHSEQN
					, CAPDAT.CAROHP.RHCTTA
					, CAPDAT.CAROHP.RHFVIN

					-- CAN BE REMOVED, RO time OUT
					, CAPDAT.CAROHP.RHTIM2
					-- CAN BE REMOVED, RO time IN
					, CAPDAT.CAROHP.RHTIM1
					-- CAN BE REMOVED, RO timer
					, (CAPDAT.CAROHP.RHTIM2-CAPDAT.CAROHP.RHTIM1)/100 as roMinutes
					
				from capdat.carohp
				where 1=1

				-- CAN BE REMOVED ro closed date must be the same date as ro closed timestamp
				-- and cast(VARCHAR_FORMAT(CAPDAT.CAROHP.RHTIM2, 'YYYYMMDD') as int) = CAPDAT.CAROHP.RHWCDT

				-- for the fiven dealer
				and CAPDAT.CAROHP.RHRDLR= '{2}'
				-- within the given tiemframe
				and INT(CAPDAT.CAROHP.RHWODT/100) > {0}	and INT(CAPDAT.CAROHP.RHWODT/100) <= {1}
				-- under $150 CP
				and CAPDAT.CAROHP.RHCLBA + CAPDAT.CAROHP.RHCPTA + CAPDAT.CAROHP.RHCSBA < 150
				-- same dayRO
				and CAPDAT.CAROHP.RHWODT=CAPDAT.CAROHP.RHWCDT
				) roTable

				join(
				Select distinct capdat.caropp.RPRDLR
				    , capdat.caropp.RPFVIN
				    , capdat.caropp.RPSEQN
				    , capdat.caropp.RPWODT
				From capdat.caropp 
				Where (capdat.caropp.RPCQPT Like '263%' 
				    Or capdat.caropp.RPDPRT Like 'H263%' 
				    Or capdat.caropp.RPDPRT Like 'KI263%' 
				    Or capdat.caropp.RPDPRT Like 'QL263%' 
				    Or capdat.caropp.RPDPRT Like 'Q263%' 
				    Or capdat.caropp.RPDPRT Like 'H-263%' 
				    Or capdat.caropp.RPDPRT Like 'K-263%' 
				    Or capdat.caropp.RPDPRT Like 'Q-263%' 
				    Or capdat.caropp.RPDPRT Like 'HY263%')
				    and capdat.caropp.RPRDLR= '{2}'
				    and INT(capdat.caropp.RPWODT/100) > {0}
				    and INT(capdat.caropp.RPWODT/100) <= {1}
				) roOps On roTable.RHRDLR = roOps.RPRDLR 
				    And roTable.RHFVIN = roOps.RPFVIN 
				    And roTable.RHSEQN = roOps.RPSEQN 
				    And roTable.RHWODT = roOps.RPWODT
				
				GROUP BY roTable.RHRDLR
					,INT(roTable.RHWODT/100) '''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		if queryTypeInt==8:#cceRO
			data='''
				SELECT 
					-- CAN BE REMOVED or CHANGED -- average of RO minutes by dealer/YM
					-- AVG(roTable.roMinutes) as roMinutes
					-- CAN BE REMOVED or CHANGED -- count of RO minute ROs
					-- ,count(roTable.roMinutes) as countRoMinutes

					-- KEEP
					roTable.RHRDLR as DEALER_CODE
					-- KEEP
					,INT(roTable.RHWODT/100) AS cceRODATE  
					-- KEEP
					, '{3}' as interval
					, count(roTable.RHRDLR) as cceROCount
					, sum(roTable.RHCTTA) as cceROSUM_$
				from (
				select 
					CAPDAT.CAROHP.RHRDLR
					, CAPDAT.CAROHP.RHWODT
					, CAPDAT.CAROHP.RHWCDT
					, CAPDAT.CAROHP.RHSEQN
					, CAPDAT.CAROHP.RHCTTA
					, CAPDAT.CAROHP.RHFVIN

					-- CAN BE REMOVED, RO time OUT
					, CAPDAT.CAROHP.RHTIM2
					-- CAN BE REMOVED, RO time IN
					, CAPDAT.CAROHP.RHTIM1
					-- CAN BE REMOVED, RO timer
					, (CAPDAT.CAROHP.RHTIM2-CAPDAT.CAROHP.RHTIM1)/100 as roMinutes
					
				from capdat.carohp
				where 1=1

				-- CAN BE REMOVED ro closed date must be the same date as ro closed timestamp
				-- and cast(VARCHAR_FORMAT(CAPDAT.CAROHP.RHTIM2, 'YYYYMMDD') as int) = CAPDAT.CAROHP.RHWCDT

				-- for the fiven dealer
				and CAPDAT.CAROHP.RHRDLR= '{2}'
				-- within the given tiemframe
				and INT(CAPDAT.CAROHP.RHWODT/100) > {0}	and INT(CAPDAT.CAROHP.RHWODT/100) <= {1}
				-- under $150 CP
				and CAPDAT.CAROHP.RHCLBA + CAPDAT.CAROHP.RHCPTA + CAPDAT.CAROHP.RHCSBA < 150
				-- same dayRO
				and CAPDAT.CAROHP.RHWODT=CAPDAT.CAROHP.RHWCDT
				) roTable

				join(
					Select distinct CAPDAT.CAROOP.RORDLR RORDLRO
					, CAPDAT.CAROOP.ROSEQN ROSEQNO
					, CAPDAT.CAROOP.ROWODT ROWODTO
					, CAPDAT.CAROOP.ROFVIN ROFVINO
					From CAPDAT.CAROOP 
					Where 
						(CAPDAT.CAROOP.RODOPC Like '%HE400%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE402%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE304%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE100%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE301%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE200%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE99P%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE600%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE500%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE700%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE800%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE603%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE401%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE303%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE1000%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HQ902%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE602%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HQ900%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HQ901%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE601%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE302%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE501%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE201%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE204%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE203%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE202%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE205%' 
						Or CAPDAT.CAROOP.RODOPC Like '%HE206%'
						) 
						and CAPDAT.CAROOP.RORDLR= '{2}'
					    and INT(CAPDAT.CAROOP.ROWODT/100) > {0}
					    and INT(CAPDAT.CAROOP.ROWODT/100) <= {1}
					UNION 
					SELECT distinct CAPDAT.CAROOP.RORDLR RORDLRO
						, CAPDAT.CAROOP.ROSEQN ROSEQNO
						, CAPDAT.CAROOP.ROWODT ROWODTO
						, CAPDAT.CAROOP.ROFVIN ROFVINO
					FROM CAPDAT.CAROOP 
					WHERE 
						CAPDAT.CAROOP.RORDLR In ('AZ046', 'GA065', 'TX181', 'GA075') 
						And (CAPDAT.CAROOP.RODOPC Like '%11HYZ%' 
							Or CAPDAT.CAROOP.RODOPC Like '%MA83%' 
							Or CAPDAT.CAROOP.RODOPC Like '%09HYZ%' 
							Or CAPDAT.CAROOP.RODOPC Like '%01HYZ005%' 
							Or CAPDAT.CAROOP.RODOPC Like '%MA40%' 
							Or CAPDAT.CAROOP.RODOPC Like '%MULTI-A%' 
							Or CAPDAT.CAROOP.RODOPC Like '%00HYZ10%') 
						and CAPDAT.CAROOP.RORDLR= '{2}'
					    and INT(CAPDAT.CAROOP.ROWODT/100) > {0}
					    and INT(CAPDAT.CAROOP.ROWODT/100) <= {1}
					UNION 
					SELECT distinct CAPDAT.CAROOP.RORDLR RORDLRO
						, CAPDAT.CAROOP.ROSEQN ROSEQNO
						, CAPDAT.CAROOP.ROWODT ROWODTO
						, CAPDAT.CAROOP.ROFVIN ROFVINO
					FROM CAPDAT.CAROOP 
					WHERE 
						CAPDAT.CAROOP.RORDLR In ('NM011', 'NM012', 'AZ019') 
						And CAPDAT.CAROOP.ROOPGP Like 'CCE%'
					    and CAPDAT.CAROOP.RORDLR = '{2}'
					    and INT(CAPDAT.CAROOP.ROWODT/100) > {0}
					    and INT(CAPDAT.CAROOP.ROWODT/100) <= {1}
				) roParts On roTable.RHRDLR = roParts.RORDLRO 
				    And roTable.RHFVIN = roParts.ROFVINO
				    And roTable.RHSEQN = roParts.ROSEQNO
				    And roTable.RHWODT = roParts.ROWODTO
				GROUP BY roTable.RHRDLR
					,INT(roTable.RHWODT/100) '''.format(startPeriod, endPeriod, dealerCode, cceInterval) 
		return data

	def query(self, startPeriod, endPeriod, dealerCode, cceInterval):
		queryTypeDict=self.queryTypes
		queryType=queryTypeDict[self.whatwePullin]
		data=self.queryWriter(startPeriod, endPeriod, dealerCode, cceInterval, queryType)
		tempDF=func.runAS400Query(queryText=data, odbcVarName='DSN=as400')
		print(tempDF.head())
		self.outputDF=pd.concat([self.outputDF, tempDF])

	def pullData(self, inputDF):
		startPeriod='Start'
		endPeriod='End'
		dealerCode='DlrCd'
		dealerCodecceInterval='Interval'
		np.vectorize(self.query)(inputDF[startPeriod]
									,inputDF[endPeriod]
									,inputDF[dealerCode]
									,inputDF[dealerCodecceInterval])

		return self.outputDF