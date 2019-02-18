import qbr_dataPullClass as dpc
import pandas as pd
import os


from func_forAll import func_forAll as func


def main():

	masterClass=dpc.historicalDataPull()
	#1=		hcr
	#2=		rotime
	#3=		oilFilter
	#4=		engFilter
	#5=		cabinFilter
	#6=		wipers

	intervalBenchmarks=masterClass.intervalBenchmarks
	whatwePullinList=list(masterClass.queryTypes.keys())

	for y in intervalBenchmarks:
		for x in whatwePullinList:
			masterClass=dpc.historicalDataPull()
			masterClass.inputFile='/'+y+'.csv'
			masterClass.outputFile='/'+x+'_'+y+'.csv'

			print(x)
			print(masterClass.outputFile)

			masterClass.whatwePullin=x
			#import inputlist
			inputDF=masterClass.inputDF
			inputDF=pd.read_csv(os.getcwd()+masterClass.dataInputFileLoc+masterClass.inputFile)

			print('...inputDF...')
			print(inputDF.head())
			print('...endinputDF...')

			#vectorized data Pull
			masterClass.pullData(inputDF)

			print(masterClass.inputFile)
			print(masterClass.outputFile)

			print('...outputDF...')
			print(masterClass.outputDF.head())
			print('...endoutputDF...')

			#export outputFile
			masterClass.finalSave(masterClass.outputFile,masterClass.outputDF)
			del masterClass

main()