def allRO(yyyyMMIn, additionalParams=0):
	if additionalParams==0:
		additionalParamsQry=''
		additionalParamsQry_1=''

	queryTemp='''
		SELECT 
			CAPDAT.CAROHP.RHRDLR as DEALER_CD
			, trim(CAPDAT.CAROHP.RHSEQN) as REPAIR_ORDER_NUM
		    , capdat.carohp.RHFVIN VIN
		    , SUBSTRING(capdat.carohp.RHWODT, 5, 2) || '/' || SUBSTRING(capdat.carohp.RHWODT, 7, 2) || '/' || SUBSTRING(capdat.carohp.RHWODT, 1, 4) RO_OPEN_DATE
			, SUBSTRING(capdat.carohp.RHWCDT, 5, 2) || '/' || SUBSTRING(capdat.carohp.RHWCDT, 7, 2) || '/' || SUBSTRING(capdat.carohp.RHWCDT, 1, 4) RO_CLOSE_DATE
		from capdat.CAROHP
		where INT(CAPDAT.CAROHP.RHWCDT/100)={0}
		GROUP BY CAPDAT.CAROHP.RHRDLR 
		    , CAPDAT.CAROHP.RHSEQN
		    , CAPDAT.CAROHP.RHWCDT
		    , capdat.carohp.RHWODT
		    , capdat.carohp.RHFVIN
	'''.format(yyyyMMIn)

	return queryTemp