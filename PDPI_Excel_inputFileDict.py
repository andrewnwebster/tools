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

def inputFileDict():
	inputFileDict={
		#-----------------------------------------------------------------------------------------
		'1_hcrOverall.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. Rating':'AvgRating',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of Rating':'ratingCount',
			},
			'regionRenameKeys':{
				0:'AvgRating'
			},
			'districtRenameKeys':{
				0:'AvgRating'
			},
			'natlRenameKeys':{
				0:'AvgRating'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','ratingCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'AvgRating',
			'weight_name':'ratingCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrOverallOut.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrRtn.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. RTN_SCORE':'RtnRating',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of RTN_SCORE':'ratingCount',
			},
			'regionRenameKeys':{
				0:'RtnRating'
			},
			'districtRenameKeys':{
				0:'RtnRating'
			},
			'natlRenameKeys':{
				0:'RtnRating'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','ratingCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'RtnRating',
			'weight_name':'ratingCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrRtn.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrTimely.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. TIMELY_SCORE':'TmlyRating',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of TIMELY_SCORE':'timelyCount',
			},
			'regionRenameKeys':{
				0:'TmlyRating'
			},
			'districtRenameKeys':{
				0:'TmlyRating'
			},
			'natlRenameKeys':{
				0:'TmlyRating'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','timelyCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'TmlyRating',
			'weight_name':'timelyCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrTimely.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrTimeliness.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. TIMELINESS_RATING_SCORE':'Timeliness',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of TIMELINESS_RATING_SCORE':'timelinessCount',
			},
			'regionRenameKeys':{
				0:'Timeliness'
			},
			'districtRenameKeys':{
				0:'Timeliness'
			},
			'natlRenameKeys':{
				0:'Timeliness'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','timelinessCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'Timeliness',
			'weight_name':'timelinessCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrTimeliness.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrRecBrnd.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. RECO_BRAND_SCORE':'Reco_Brand',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of RECO_BRAND_SCORE':'Reco_Brand_Count',
			},
			'regionRenameKeys':{
				0:'Reco_Brand'
			},
			'districtRenameKeys':{
				0:'Reco_Brand'
			},
			'natlRenameKeys':{
				0:'Reco_Brand'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','Reco_Brand_Count'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'Reco_Brand',
			'weight_name':'Reco_Brand_Count',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrRecBrnd.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrWr.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. WREC_SCORE':'RecRating',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of WREC_SCORE':'wrecCount',
			},
			'regionRenameKeys':{
				0:'RecRating'
			},
			'districtRenameKeys':{
				0:'RecRating'
			},
			'natlRenameKeys':{
				0:'RecRating'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','wrecCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'RecRating',
			'weight_name':'wrecCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrWr.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'1_hcrValue.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Avg. VALUE_RATING':'ValRating',
				'REGION CODE':'RegionCode',
				'Month, Year of SurveyDate':'YYmmmmDD',
				'Count of VALUE_RATING':'ValCount',
			},
			'regionRenameKeys':{
				0:'ValRating'
			},
			'districtRenameKeys':{
				0:'ValRating'
			},
			'natlRenameKeys':{
				0:'ValRating'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'sourceDF':pd.DataFrame(),
			'baseDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','ValCount'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			'inDfDropList':[],
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'ValRating',
			'weight_name':'ValCount',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'hcrVal.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'2_gpSales.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Cp Gross':'Num',
				'REGION CODE':'RegionCode',
				'Month, Year of Rpt Month_Cal':'YYmmmmDD',
				'Cp Sales':'Den',
				'Gr Profit - CP':'gpPct',
				'Num_x':'Num',
				'Den_y':'Den',
				#'CONSULTANT_y':'CONSULTANT',
				'DealerProgram_y':'DealerProgram',
				'DIST_y':'DIST',
				'YYmmmmDD_y':'YYmmmmDD',
				'NATL_y':'NATL',
				'RegionCode_y':'RegionCode',
				'gpPct_y':'gpPct',
			},
			'regionRenameKeys':{
				0:'gpPct'
			},
			'districtRenameKeys':{
				0:'gpPct'
			},
			'natlRenameKeys':{
				0:'gpPct'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			'inDfDropList':[
							'DealerProgram_x',\
							'DIST_x',\
							'Measure Names_x',\
							'Measure Names_y',\
							'YYmmmmDD_x',\
							'NATL_x',\
							'RegionCode_x',\
							'gpPct_x',\
							'Num_y',\
							'Den_x',		
							],

			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','Num','Den'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'',
			'weight_name':'',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'gpSales.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'2_rtnCmc.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'2ymc Num Ct':'Num',
				'REGION CODE':'RegionCode',
				'Month, Year of Rpt Month_Cal':'YYmmmmDD',
				'2ymc Den Ct':'Den',
				'2YMC Ret %':'cmcPct',
				'Num_x':'Num',
				'Den_y':'Den',
				#'CONSULTANT_y':'CONSULTANT',
				'DealerProgram_y':'DealerProgram',
				'DIST_y':'DIST',
				'YYmmmmDD_y':'YYmmmmDD',
				'NATL_y':'NATL',
				'RegionCode_y':'RegionCode',
				'cmcPct_y':'cmcPct',
			},
			'regionRenameKeys':{
				0:'cmcPct'
			},
			'districtRenameKeys':{
				0:'cmcPct'
			},
			'natlRenameKeys':{
				0:'cmcPct'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			'inDfDropList':[
							'DealerProgram_x',\
							'DIST_x',\
							'Measure Names_x',\
							'Measure Names_y',\
							'YYmmmmDD_x',\
							'NATL_x',\
							'RegionCode_x',\
							'cmcPct_x',\
							'Num_y',\
							'Den_x',		
							],

			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','Num','Den'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'',
			'weight_name':'',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'rtnCmc.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'2_rtnDealer.csv':{
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'Brnd Num Ct':'Num',
				'REGION CODE':'RegionCode',
				'Month, Year of Rpt Month_Cal':'YYmmmmDD',
				'Brnd Den Ct':'Den',
				'Dlr Ret %':'retPct',
				'Num_x':'Num',
				'Den_y':'Den',
				#'CONSULTANT_y':'CONSULTANT',
				'DealerProgram_y':'DealerProgram',
				'DIST_y':'DIST',
				'YYmmmmDD_y':'YYmmmmDD',
				'NATL_y':'NATL',
				'RegionCode_y':'RegionCode',
				'retPct_y':'retPct',
			},
			'regionRenameKeys':{
				0:'retPct'
			},
			'districtRenameKeys':{
				0:'retPct'
			},
			'natlRenameKeys':{
				0:'retPct'	
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode'],
				'right':['yyyyMM','RegionCode'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST'],
				'right':['yyyyMM','DIST'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			'inDfDropList':[
							'DealerProgram_x',\
							'DIST_x',\
							'Measure Names_x',\
							'Measure Names_y',\
							'YYmmmmDD_x',\
							'NATL_x',\
							'RegionCode_x',\
							'retPct_x',\
							'Num_y',\
							'Den_x',		
							],

			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['YYmmmmDD','Num','Den'],

			#inDf is a copy needed to create dist/reg/natl groups, which will
			#concat back to baseDF
			
			'regionDropList':[],
			'districtDropList':[],
			'natlDropList':[],
			'finalDropList':[],
			'districtGroupBy':['yyyyMM','DIST','DealerProgram'],
			'regionGroupBy':['yyyyMM','RegionCode','DealerProgram'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram'],
			'avg_name':'',
			'weight_name':'',
			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'rtnDealer.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
		},
#-----------------------------------------------------------------------------------------
		'3_roCt.csv':{
			'reformatColumn':'Totalroct_YOY_pct',
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'REGION CODE':'RegionCode',
				'Year of Rpt Month_Cal':'yearVar',
				'Month of Rpt Month_Cal':'monthVar',
				'% Difference in Totalroct':'Totalroct_YOY_pct',
				'Totalroct_YOY_pct_x':'Totalroct_YOY_pct',
				'yyyyMM_x':'yyyyMM',
			},

			'yearColumn':'yearVar',
			'year+1Column':'yearVar+1',
			'monthColumn':'monthVar',

			'20162017_NATL_JoinKeys':{
			#2016
				'left':['monthVar','yearVar+1','NATL'],
			#2017
				'right':['monthVar','yearVar','NATL'],
			},

			'20162017_REGN_JoinKeys':{
			#2016
				'left':['monthVar',	'yearVar+1','NATL','RegionCode'],
			#2017
				'right':['monthVar', 'yearVar','NATL','RegionCode'],
			},

			'20162017_DIST_JoinKeys':{
			#2016
				'left':['monthVar',	'yearVar+1','NATL','RegionCode','DIST'],
			#2017
				'right':['monthVar', 'yearVar','NATL','RegionCode','DIST'],
			},

			'districtGroupBy':['yyyyMM','NATL','RegionCode','DIST','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'regionGroupBy':['yyyyMM','NATL','RegionCode','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],

			

			'inDfDropList':[
							'Measure Names_x',\
							'Measure Names_y',\
							'Totalroct_YOY_pct_y',\
							'yearVar_x',\
							'YYmmmmDD_x',\
							'yearVar+1_x',\
							'yearVar_y',\
							'YYmmmmDD_y',\
							'yearVar+1_y',\
							],

			'num':'Totalroct_x',
			'den':'Totalroct_y',

			'regionDistrictNatlDropList':['Measure Names_x',\
								'YYmmmmDD_x',\
								'yearVar+1_x',\
								'yearVar_x',\
								'monthVar',\
								'Totalroct_x',\
								'yyyyMM_x',\
								'DealerProgram_y',\
								'Measure Names_y',\
								'YYmmmmDD_y',\
								'yearVar+1_y',\
								'yearVar_y',\
								'Totalroct_y'],

			'finalDropList':[
				'Measure Names',\
				'Totalroct',\
				'YYmmmmDD',\
				'monthVar',\
				'yearVar',\
				'yearVar+1',\
			],

			'regionDistrictNatlRenameDict':{
				0:'Totalroct_YOY_pct',
				'yyyyMM_y':'yyyyMM',
				'DealerProgram_x':'DealerProgram',
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode','NATL'],
				'right':['yyyyMM','RegionCode','NATL'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST','RegionCode','NATL'],
				'right':['yyyyMM','DIST','RegionCode','NATL'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			
			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			'baseDfDropList':['Measure Names','Totalroct','YYmmmmDD','monthVar','yearVar','yearVar+1'],

			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			'outputFileName':'roCt.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
			'columnsToChange':{
				'Totalroct_YOY_pct_x':'0%'
			}
		},
#-----------------------------------------------------------------------------------------
		'3_roCpCt.csv':{
			##CHANGE
			'reformatColumn':'Cproct_YOY_pct',
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'REGION CODE':'RegionCode',
				'Year of Rpt Month_Cal':'yearVar',
				'Month of Rpt Month_Cal':'monthVar',
				'% Difference in Cproct':'Cproct_YOY_pct',
				'Cproct_YOY_pct_x':'Cproct_YOY_pct',
				'yyyyMM_x':'yyyyMM',
			},


			'yearColumn':'yearVar',
			'year+1Column':'yearVar+1',
			'monthColumn':'monthVar',

			'20162017_NATL_JoinKeys':{
				'left':['monthVar','yearVar+1','NATL'],
				'right':['monthVar','yearVar','NATL'],
			},

			'20162017_REGN_JoinKeys':{
				'left':['monthVar',	'yearVar+1','NATL','RegionCode'],
				'right':['monthVar', 'yearVar','NATL','RegionCode'],
			},

			'20162017_DIST_JoinKeys':{
				'left':['monthVar',	'yearVar+1','NATL','RegionCode','DIST'],
				'right':['monthVar', 'yearVar','NATL','RegionCode','DIST'],
			},

			##CHANGE
			'districtGroupBy':['yyyyMM','NATL','RegionCode','DIST','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'regionGroupBy':['yyyyMM','NATL','RegionCode','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],

			##CHANGE
			'inDfDropList':[
							'Measure Names_x',\
							'Measure Names_y',\
							'Cproct_YOY_pct_y',\
							'yearVar_x',\
							'YYmmmmDD_x',\
							'yearVar+1_x',\
							'yearVar_y',\
							'YYmmmmDD_y',\
							'yearVar+1_y',\
							],
			##CHANGE
			'num':'Cproct_x',
			'den':'Cproct_y',
			##CHANGE
			'regionDistrictNatlDropList':['Measure Names_x',\
								'YYmmmmDD_x',\
								'yearVar+1_x',\
								'yearVar_x',\
								'monthVar',\
								'Cproct_x',\
								'yyyyMM_x',\
								'DealerProgram_y',\
								'Measure Names_y',\
								'YYmmmmDD_y',\
								'yearVar+1_y',\
								'yearVar_y',\
								'Cproct_y'],
			##CHANGE
			'finalDropList':[
				'Measure Names',\
				'Cproct',\
				'YYmmmmDD',\
				'monthVar',\
				'yearVar',\
				'yearVar+1',\
			],
			##CHANGE
			'regionDistrictNatlRenameDict':{
				0:'Cproct_YOY_pct',
				'yyyyMM_y':'yyyyMM',
				'DealerProgram_x':'DealerProgram',
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode','NATL'],
				'right':['yyyyMM','RegionCode','NATL'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST','RegionCode','NATL'],
				'right':['yyyyMM','DIST','RegionCode','NATL'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			
			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			##CHANGE
			'baseDfDropList':['Measure Names','Cproct','YYmmmmDD','monthVar','yearVar','yearVar+1'],

			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			##CHANGE
			'outputFileName':'roCpCt.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
			'columnsToChange':{
				'Cproct_YOY_pct_x':'0%'
			}
		},
#-----------------------------------------------------------------------------------------
		'3_roCpDol.csv':{
			##CHANGE
			'reformatColumn':'Cpdol_YOY_pct',
			'baseRenameKeys':{
				'DEALER PROGRAM':'DealerProgram',
				'REGION CODE':'RegionCode',
				'Year of Rpt Month_Cal':'yearVar',
				'Month of Rpt Month_Cal':'monthVar',
				'% Difference in Cpdol':'Cpdol_YOY_pct',
				'Cpdol_YOY_pct_x':'Cpdol_YOY_pct',
				'yyyyMM_x':'yyyyMM',
			},


			'yearColumn':'yearVar',
			'year+1Column':'yearVar+1',
			'monthColumn':'monthVar',

			'20162017_NATL_JoinKeys':{
				'left':['monthVar','yearVar+1','NATL'],
				'right':['monthVar','yearVar','NATL'],
			},

			'20162017_REGN_JoinKeys':{
				'left':['monthVar',	'yearVar+1','NATL','RegionCode'],
				'right':['monthVar', 'yearVar','NATL','RegionCode'],
			},

			'20162017_DIST_JoinKeys':{
				'left':['monthVar',	'yearVar+1','NATL','RegionCode','DIST'],
				'right':['monthVar', 'yearVar','NATL','RegionCode','DIST'],
			},

			##CHANGE
			'districtGroupBy':['yyyyMM','NATL','RegionCode','DIST','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'regionGroupBy':['yyyyMM','NATL','RegionCode','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],
			'natlGroupBy':['yyyyMM','NATL','DealerProgram','Measure Names','YYmmmmDD','yearVar+1', 'yearVar', 'monthVar'],

			##CHANGE
			'inDfDropList':[
							'Measure Names_x',\
							'Measure Names_y',\
							'Cpdol_YOY_pct_y',\
							'yearVar_x',\
							'YYmmmmDD_x',\
							'yearVar+1_x',\
							'yearVar_y',\
							'YYmmmmDD_y',\
							'yearVar+1_y',\
							],
			##CHANGE
			'num':'Cpdol_x',
			'den':'Cpdol_y',
			##CHANGE
			'regionDistrictNatlDropList':['Measure Names_x',\
								'YYmmmmDD_x',\
								'yearVar+1_x',\
								'yearVar_x',\
								'monthVar',\
								'Cpdol_x',\
								'yyyyMM_x',\
								'DealerProgram_y',\
								'Measure Names_y',\
								'YYmmmmDD_y',\
								'yearVar+1_y',\
								'yearVar_y',\
								'Cpdol_y'],
			##CHANGE
			'finalDropList':[
				'Measure Names',\
				'Cpdol',\
				'YYmmmmDD',\
				'monthVar',\
				'yearVar',\
				'yearVar+1',\
			],
			##CHANGE
			'regionDistrictNatlRenameDict':{
				0:'Cpdol_YOY_pct',
				'yyyyMM_y':'yyyyMM',
				'DealerProgram_x':'DealerProgram',
			},
			'natlJoinKeys':{
				'left':['yyyyMM','NATL'],
				'right':['yyyyMM','NATL'],
			},
			'regionJoinKeys':{
				'left':['yyyyMM','RegionCode','NATL'],
				'right':['yyyyMM','RegionCode','NATL'],
			},
			'distJoinKeys':{
				'left':['yyyyMM','DIST','RegionCode','NATL'],
				'right':['yyyyMM','DIST','RegionCode','NATL'],
			},
			'initialJoinGroup':{
				'left':['DEALER','yyyyMM'],
				'right':['DEALER','yyyyMM']
			},
			#cleanup needed for joining the same table to itself (with alternating nulls missing)
			
			'sourceDF':pd.DataFrame(),
			'inDF': pd.DataFrame(),
			'districtDF':pd.DataFrame(),
			'regionDF':pd.DataFrame(),
			'nationalDF':pd.DataFrame(),
			##CHANGE
			'baseDfDropList':['Measure Names','Cpdol','YYmmmmDD','monthVar','yearVar','yearVar+1'],

			'outputFileDir':'//hke.local/hma/Dept/Customer_Satisfaction/Service Business Development/Service Business Improvement/_Programs and Vendors/MSXi/_PDPI/Reports/FOS_Trend_Timeline/dataSource',
			'outputFileSheet':['Sheet 1'],
			##CHANGE
			'outputFileName':'roCpDol.xlsx',
			'inputFileDir':os.getcwd(),
			'oldDT':'YYmmmmDD',
			'newDT':'yyyyMM',
			'consultantDealerKeysDF':['CONSULTANT','DEALER'],
			'consultantDealerJoins':{
				'left':['DEALER'],
				'right':['DEALER']
			},
			'consultKeyGroupBy':['CONSULTANT','DIST','NATL','RegionCode','yyyyMM'],
			'columnsToChange':{
				'Cpdol_YOY_pct_x':'0%'
			}
		},
	}
	return inputFileDict