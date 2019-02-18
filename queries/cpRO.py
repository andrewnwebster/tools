def cpROquery(yyyyMMIn, additionalParams=0):
	additionalParamsQry='''
		, capdat.carohp.RHCLBA as LABORPAY
		, capdat.carohp.RHCPTA as PARTPAY 
		, capdat.carohp.RHCSBA as MISCPAY
		, case when capdat.carohp.RHTIM1 ='0001-01-01 00:00:00' then NULL else capdat.carohp.RHTIM1 end as ROOPENTIME
		, case when capdat.carohp.RHTIM2 ='0001-01-01 00:00:00' then NULL else capdat.carohp.RHTIM2 end as ROCLOSETIME
	'''
	additionalParamsQry_1='''
	GROUP BY capdat.carohp.RHRDLR, 
		capdat.carohp.RHSEQN,
		capdat.carohp.RHFVIN,
		capdat.carohp.RHWODT,
		capdat.carohp.RHWCDT,
		t1.VAL,
		capdat.carohp.RHCLBA,
		capdat.carohp.RHCPTA,
		capdat.carohp.RHCSBA,
		CAPDAT.CAROHP.RHTIM1,
		CAPDAT.CAROHP.RHTIM2

	'''
	if additionalParams==0:
		additionalParamsQry=''
		additionalParamsQry_1=''
	queryTemp='''
		SELECT capdat.carohp.RHRDLR DEALER_CD
			, trim(capdat.carohp.RHSEQN) REPAIR_ORDER_NUM
			, capdat.carohp.RHFVIN VIN
			, SUBSTRING(capdat.carohp.RHWODT, 5, 2) || '/' || SUBSTRING(capdat.carohp.RHWODT, 7, 2) || '/' || SUBSTRING(capdat.carohp.RHWODT, 1, 4) RO_OPEN_DATE
			, SUBSTRING(capdat.carohp.RHWCDT, 5, 2) || '/' || SUBSTRING(capdat.carohp.RHWCDT, 7, 2) || '/' || SUBSTRING(capdat.carohp.RHWCDT, 1, 4) RO_CLOSE_DATE
			, t1.VAL VAL 
			{1}
		From capdat.carohp 
		Left Join (
			Select capdat.caropp.RPRDLR
			, capdat.caropp.RPFVIN
			, capdat.caropp.RPSEQN
			, capdat.caropp.RPWODT
			, '1' val 
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
			Group By capdat.caropp.RPRDLR
			, capdat.caropp.RPFVIN
			, capdat.caropp.RPSEQN
			, capdat.caropp.RPWODT
		) t1 On capdat.carohp.RHRDLR = t1.RPRDLR 
					And capdat.carohp.RHFVIN = t1.RPFVIN 
					And capdat.carohp.RHSEQN = t1.RPSEQN 
					And capdat.carohp.RHWODT = t1.RPWODT 
		Where (INT((capdat.carohp.RHWCDT)/100)={0}) 
			And (((capdat.carohp.RHCLBA + capdat.carohp.RHCPTA + capdat.carohp.RHCSBA) > 5) 
		Or t1.VAL = 1)
		{2}
	'''.format(yyyyMMIn, additionalParamsQry, additionalParamsQry_1)
	return queryTemp