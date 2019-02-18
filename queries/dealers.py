def dealersQuery(dlrCity=None
					, dlrState=None
					, dlrZip=None
					, dlrRegion=None
					, dlrDistrict=None
					, dlrCode=None
				):
	queryTemp='''
		SELECT 
		CAST(dealers.DTDELR as VARCHAR(10)) as Dealer_Code
		, CAST(dealers.DTDBAN as VARCHAR(30)) as Dealer_Name
		, CAST(dealers.DTREGN as VARCHAR(10)) as Region
		, CAST(dealers.DTSVCD as VARCHAR(10)) as District
		, INT(dealers.DTOPER) as Start_Date_Int
		, CAST(dealers.DTMANG as VARCHAR(40)) as GM_Name
		, CAST(dealers.DTSRDR as VARCHAR(30)) as Showroom_Address
		, CAST(dealers.DTSRCT as VARCHAR(20)) as Showroom_City
		, CAST(dealers.DTSRST as VARCHAR(3)) as Showroom_State
		, INT(dealers.DTSRZP) as Showroom_Zip

		From dlpdat.dldltp dealers
		Where 1=1
		AND dealers.DTSTAF='A'
	'''
	
	if dlrCity is not None :
		queryTemp+=' AND dealers.DTSRCT ='+str(dlrCity)
	if dlrState is not None :
		queryTemp+=' AND dealers.DTSRST ='+str(dlrState)
	if dlrZip is not None :
		queryTemp+=' AND dealers.DTSRZP ='+str(dlrZip)
	if dlrRegion is not None :
		queryTemp+=' AND dealers.DTREGN ='+str(dlrRegion)
	if dlrDistrict is not None :
		queryTemp+=' AND dealers.DTSVCD ='+str(dlrDistrict)
	if dlrCode is not None :
		queryTemp+=' AND dealers.DTDELR ='+str(dlrCode)
	return queryTemp