def df_tocsv(dFrame, pathList=[], fileName='', index=False):
	if pathList==[]:
		return 0
	elif fileName=='':
		return 0
	else:
		for x in pathList:
			if isinstance(fileName, str):
				tempPath=x+fileName
				df_tocsv_extension(tempPath, index)
			elif isinstance(fileName, list):
				for y in fileName:
					tempPath=x+y
					df_tocsv_extension(tempPath, index)
			else:
				return 0

def df_tocsv_extension(tempPath, index):
	try:
		dFrame.to_csv(tempPath, index=index)
	except:
		print('SAVE ERROR: '+tempPath+' ...\n...not a valid path!')

