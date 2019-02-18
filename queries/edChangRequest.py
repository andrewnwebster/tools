def edChangQuery(yyyyMMIn, additionalParams=0):
	queryTemp='''
		SELECT                               
		DEALER_CODE as DEALER
		, trim(REPAIR_ORDER) as RONUM
		, max(SURVEYS .RODATE) as RODATE
		, MAX(VIN) as VIN
		, MAX(RATING) as RATING
	                , case when MAX(CREATE_DATE) >=20171004 then NULL else MAX(RECO_DEALER) end as WREC
		, MAX(RETURN_DEALER) as RTN
	                , case when MAX(HR)=0 then NULL else MAX(HR) end as HR
		, MAX(TIMELY_SERVICE) as TIMELY
		, MAX(CREATE_DATE) as SURCDT
	    , case when MAX(RECO_BRAND)=0 then NULL else MAX(RECO_BRAND) end as RECO_BRAND
		FROM (
				SELECT *
				FROM (
					VALUES (
			                                    {0} --enter date
			                                    ,'KS003' --enter dealer code
			                                    ,'' --enter RO NUMBER
			                                ) 
				) t1 (REPORT_DATE, dealerCode, roNumber)
			) PARAMS 
		JOIN CAPDAT.CAROHP repairOrders on INT(repairOrders.RHWCDT/100)=PARAMS.REPORT_DATE
		JOIN (
		SELECT
			SURVEYS.SURRDT as CREATE_DATE
			, SURVEYS.SURCDT as COMPLETE_DATE
			, SURVEYS.SUVINN as VIN
			, SURVEYS.SUROQN as REPAIR_ORDER
	                                , SURVEYS.SUSDAT as RODATE
			, SURVEYS.SUDELR as DEALER_CODE
			, case when SURVEYS.SURSDT<>0 and SURVEYS.SURRTE<>0  then (SURVEYS.SURRTE+SURVEYS.SURATE)/2
				else SURVEYS.SURATE end RATING
			, case when SURVEYS.SURSDT<>0 and SURVEYS.SURRTE<>0  then SURVEYS.SURRMD 
				else SURVEYS.SURRCM end RECO_DEALER
			, 0 as RECO_BRAND
			, case when SURVEYS.SNQUE2 ='' then NULL 
			when SURVEYS.SURRDT>=20171004 then NULL
			when SURVEYS.SNQUE2 ='1' then 5 
			else 1 end RETURN_DEALER
			, case 
			            when SURVEYS.SNQUS7='1' then 1
			            when SURVEYS.SNQUS7='2' then 2
			            when SURVEYS.SNQUS7='3' then 3
			            when SURVEYS.SNQUS7='4' then 4
			            when SURVEYS.SNQUS7='5' then 5
			            when SURVEYS.SNQUE5='1' then 5
			            when SURVEYS.SNQUE5='0' then 1
			            else NULL end TIMELY_SERVICE
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
	                                , CS4SVD as  RODATE
			, CS4DLR AS DEALER_CODE
			, max(case when CS7QID = '8780' and CS7OID='36134' then 1 
			when CS7OID='36135' then 2 
			when CS7OID='36136' then 3 
			when CS7OID='36137' then 4  
			when CS7OID='36138' then 5  end) as RATING
			, 0 as RECO_DEALER
			, max(case when CS7QID = '8784' and CS7OID='36151' then 1 
			when CS7OID='36152' then 2 
			when CS7OID='36153' then 3 
			when CS7OID='36154'  then 4 
			when CS7OID='36155' then 5 end) as RECO_BRAND
			, max(case when CS7QID = '8783' and CS7OID='36146' then 1 
			when CS7OID='36147' then 2 
			when CS7OID='36148' then 3 
			when CS7OID='36149' then 4 
			when CS7OID='36150' then 5 end) as RETURN_DEALER
			, max(case when CS7QID = '8785' and CS7OID='36156' then 5 
			                        when CS7OID='36157' then 1 end) as TIMELY_SERVICE
			           , 0 as HR
			, 1 as SURVEY_AGE_KEY
		from CAPDAT.CSUR07P
		LEFT JOIN CAPDAT.CSUR06P  ON (CS6PID=CS7FID) 
		LEFT JOIN CAPDAT.CSUR04P ON (CS4PID=CS7FID)
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
		group by CS7FID,CS6PID, CS6TIM, CS6CIM, CS4VIN,CS4SOR,CS4DLR, CS4SVD
		) SURVEYS ON trim(SURVEYS.REPAIR_ORDER)=trim(repairOrders.RHSEQN)
						AND SURVEYS.DEALER_CODE=repairOrders.RHRDLR
		WHERE SURVEYS.REPAIR_ORDER <> '' and SURVEYS.RATING<>0
		GROUP BY 
			REPAIR_ORDER
		,DEALER_CODE
	'''.format(yyyyMMIn)
	return queryTemp