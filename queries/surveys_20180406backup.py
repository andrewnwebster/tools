def surveysQuery(yyyyMMIn, additionalParams=0):
	additionalParamsQry='''
			, dealerdata.DTREGN as REGION
			, dealerdata.DTDSCD as DISTRICT
			, 'NL' as NATL
			'''
	additionalParamsQry_1='''
		, dealerdata.DTREGN
		, dealerdata.DTDSCD
		, 'NL'
		'''
	additionalParamsQry_2='JOIN dlpdat.dldltp dealerdata on dealerdata.dtdelr=repairOrders.rhrdlr'

	if additionalParams==0:
		additionalParamsQry='''		'''
		additionalParamsQry_1='''	'''
		additionalParamsQry_2=''

	queryTemp='''
		SELECT 
			int(MAX(CREATE_DATE)) as CREATE_DATE
			, MAX(COMPLETE_DATE) as COMPLETE_DATE
			, MAX(VIN) as VINSURV
			, trim(REPAIR_ORDER) as REPAIR_ORDER
			, DEALER_CODE
			, SUM(case when SURVEY_AGE_KEY=0 then 1 else 0 end) as OLD_SURVEY_COUNT
			, SUM(case when SURVEY_AGE_KEY=1 then 1 else 0 end) as NEW_SURVEY_COUNT
			, COUNT(*) as CT_SURVEY
			, MAX(RATING) as RATING
			, case when MAX(CREATE_DATE)< 20180108 then MAX(TIMELY_SERVICE) else NULL end as TIMELY_SERVICE
			, case when MAX(CREATE_DATE)>=20171004 then NULL else MAX(RECO_DEALER) end as RECO_DEALER
			, case when MAX(RECO_BRAND)=0 then NULL else MAX(RECO_BRAND) end as RECO_BRAND
			, MAX(RETURN_DEALER) as RETURN_DEALER 
			, case when MAX(TIMELINESS_RATING)!=0 and MAX(CREATE_DATE)>20180108 then MAX(TIMELINESS_RATING) else NULL end as TIMELINESS_RATING
			, case when MAX(VALUE_RATING)!=0 and MAX(CREATE_DATE)>20171004 then MAX(VALUE_RATING) else NULL end as VALUE_RATING
			, case when MAX(COMMUNICATION)!=0 and MAX(CREATE_DATE)>20171004 then MAX(COMMUNICATION) else NULL end as COMMUNICATION
			, case when MAX(FACILITY_AMENITIES)!=0 and MAX(CREATE_DATE)>20171004 then MAX(FACILITY_AMENITIES) else NULL end as FACILITY_AMENITIES
			, case when MAX(SERVICE_QUALITY)!=0 and MAX(CREATE_DATE)>20171004 then MAX(SERVICE_QUALITY) else NULL end as SERVICE_QUALITY
			, INT(MAX(CREATE_DATE)/100) as CREATE_DATE_YM
			, INT(MAX(HR)) as HR
			{1}
		FROM (
			SELECT *
			FROM (
				VALUES (
		                                    {0} --enter date
		                                    ,'KS003' --enter dealer code
		                                    ,'00000' --enter RO NUMBER
		                                ) 
			) t1 (REPORT_DATE, dealerCode, roNumber)
		) PARAMS 
		JOIN CAPDAT.CAROHP repairOrders on INT(repairOrders.RHWCDT/100)=PARAMS.REPORT_DATE
		{3}
		JOIN (
			SELECT
			SURVEYS.SURRDT as CREATE_DATE
			, SURVEYS.SURCDT as COMPLETE_DATE
			, SURVEYS.SUVINN as VIN
			, SURVEYS.SUROQN as REPAIR_ORDER
			, SURVEYS.SUDELR as DEALER_CODE
			, case when SURVEYS.SURSDT<>0 and SURVEYS.SURRTE<>0  then (SURVEYS.SURRTE+SURVEYS.SURATE)/2.0
				else SURVEYS.SURATE end RATING
			, case when SURVEYS.SURSDT<>0 and SURVEYS.SURRTE<>0  then SURVEYS.SURRMD 
				else SURVEYS.SURRCM end RECO_DEALER
			, cast(NULL as int) as RECO_BRAND
			, case when SURVEYS.SNQUE2 ='' then NULL 
						when SURVEYS.SURRDT>=20171004 then NULL
			            when SURVEYS.SNQUE2 ='1' then 5 
			            else 1 end RETURN_DEALER
			, case 
				when SURVEYS.SURRDT>=20161101 and SURVEYS.SURRDT<20171004 then 
				(
					case 
						when SURVEYS.SNQUE5='0' then 0
						when SURVEYS.SNQUE5='1' then 1
					else NULL end
				)
				when SURVEYS.SURRDT>=20171004 and SURVEYS.SURRDT<20180108 then 
				(
					case 
						when SURVEYS.SNQUS3='0' then 0
						when SURVEYS.SNQUS3='1' then 1
					else NULL end
				)
				when SURVEYS.SURRDT>=20180108 then NULL
				else NULL 
			end TIMELY_SERVICE
			, case when SURVEYS.SURRDT>=20180108 then 
				(
					case 
						when SURVEYS.SNQUS7='1' then 1
						when SURVEYS.SNQUS7='2' then 2
						when SURVEYS.SNQUS7='3' then 3
						when SURVEYS.SNQUS7='4' then 4
						when SURVEYS.SNQUS7='5' then 5
					else NULL end
				)
				else NULL 
			end as TIMELINESS_RATING
			, cast(NULL as int) as VALUE_RATING
			, cast(NULL as int) as COMMUNICATION
			, cast(NULL as int) as FACILITY_AMENITIES
			, cast(NULL as int) as SERVICE_QUALITY
			, SURVEYS.SNQUE3 as HR
			, 0 as SURVEY_AGE_KEY
			FROM CAPDAT.CSUREL00 SURVEYS
			WHERE SURVEYS.SURATE>0
			UNION ALL
			SELECT
			INT(REPLACE(substr(char(CS6TIM ),0,11),'-','')) as CREATE_DATE
			, INT(REPLACE(substr(char(CS6CIM ),0,11),'-','')) as COMPLETE_DATE
			, CS4VIN as VIN
			, CS4SOR as REPAIR_ORDER
			, CS4DLR AS DEALER_CODE
			, max(case when CS7QID = '8780' and CS7OID='36134' then 1 
                when CS7OID='36135' then 2 
                when CS7OID='36136' then 3 
                when CS7OID='36137' then 4  
                when CS7OID='36138' then 5  end) as RATING
			, cast(NULL as int) as RECO_DEALER
			, max(case when CS7QID = '8784' and CS7OID='36151' then 1 
                when CS7OID='36152' then 2 
                when CS7OID='36153' then 3 
                when CS7OID='36154' then 4 
                when CS7OID='36155' then 5 end) as RECO_BRAND
			, max(case when CS7QID = '8783' and CS7OID='36146' then 1 
                when CS7OID='36147' then 2 
                when CS7OID='36148' then 3 
                when CS7OID='36149' then 4 
                when CS7OID='36150' then 5 end) as RETURN_DEALER
			, max(case when CS7QID = '8785' and CS7OID='36156' then 1 when CS7OID='36157' then 0 end) as TIMELY_SERVICE
			, max(case when CS7QID = '8790' and CS7OID='36162' then 1 
				when CS7QID = '8790' and CS7OID='36163' then 2 
				when CS7QID = '8790' and CS7OID='36164' then 3 
				when CS7QID = '8790' and CS7OID='36165' then 4 
				when CS7QID = '8790' and CS7OID='36166' then 5 end) as TIMELINESS_RATING
			, max(case when CS7QID = '8792' and CS7OID='36162' then 1 
				when CS7QID = '8792' and CS7OID='36163' then 2 
				when CS7QID = '8792' and CS7OID='36164' then 3 
				when CS7QID = '8792' and CS7OID='36165' then 4 
				when CS7QID = '8792' and CS7OID='36166' then 5 end) as VALUE_RATING
			, max(case when CS7QID = '8788' and CS7OID='36162' then 1 
				when CS7QID = '8788' and CS7OID='36163' then 2 
				when CS7QID = '8788' and CS7OID='36164' then 3 
				when CS7QID = '8788' and CS7OID='36165' then 4 
				when CS7QID = '8788' and CS7OID='36166' then 5 end) as COMMUNICATION
			, max(case when CS7QID = '8789' and CS7OID='36162' then 1 
				when CS7QID = '8789' and CS7OID='36163' then 2 
				when CS7QID = '8789' and CS7OID='36164' then 3 
				when CS7QID = '8789' and CS7OID='36165' then 4 
				when CS7QID = '8789' and CS7OID='36166' then 5 end) as FACILITY_AMENITIES
			, max(case when CS7QID = '8791' and CS7OID='36162' then 1 
				when CS7QID = '8791' and CS7OID='36163' then 2 
				when CS7QID = '8791' and CS7OID='36164' then 3 
				when CS7QID = '8791' and CS7OID='36165' then 4 
				when CS7QID = '8791' and CS7OID='36166' then 5 end) as SERVICE_QUALITY
			, cast(NULL as int) as HR
			, 1 as SURVEY_AGE_KEY
			from capdat.CSUR07P
			LEFT JOIN capdat.CSUR06P  ON (CS6PID=CS7FID) 
			LEFT JOIN capdat.CSUR04P ON (CS4PID=CS7FID)
			where CS7QID IN (
				'8780',   
				'8783',   
				'8784',   
				'8785',   
				'8786',
				'8788',
				'8797',   
				'8789',   
				'8790',   
				'8791',   
				'8792',   
				'8793',   
				'8794',   
				'8795',   
				'8796',                   
				'8799',   
				'8800',   
				'8801',   
				'8802',   
				'8803',   
				'8804',   
				'8813',   
				'8806',   
				'8807',   
				'8808',   
				'8809',   
				'8810',   
				'8811',
				'8812'     
			)
			and CS4SOR is not null
			and INT(REPLACE(substr(char(CS6TIM ),0,11),'-',''))>20171004
			group by CS7FID,CS6PID, CS6TIM, CS6CIM, CS4VIN,CS4SOR,CS4DLR
		) SURVEYS ON trim(SURVEYS.REPAIR_ORDER)=trim(repairOrders.RHSEQN)
						AND SURVEYS.DEALER_CODE=repairOrders.RHRDLR
						AND INT(SURVEYS.CREATE_DATE/100)>=PARAMS.REPORT_DATE

		WHERE SURVEYS.REPAIR_ORDER <> ''  and SURVEYS.RATING<>0
		GROUP BY 
		                            REPAIR_ORDER
		                            ,DEALER_CODE
		                            {2}
	'''.format(yyyyMMIn, additionalParamsQry, additionalParamsQry_1, additionalParamsQry_2) 
	return queryTemp