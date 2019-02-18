def mpisQuery(yyyyMMIn, additionalParams=0):
	additionalParamsQry='''
		pass
	'''
	additionalParamsQry_1='''
		pass
	'''
	additionalParamsQry_2='''
		pass
	'''

	if additionalParams==0:
		additionalParamsQry='''		'''
		additionalParamsQry_1='''	'''
		additionalParamsQry_2=''

	queryTemp='''
	'''.format() 
	return queryTemp