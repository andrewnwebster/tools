# -*- coding: utf-8 -*-
import querySamples_class as pyClass
from func_forAll import func_forAll as func
import pandas as pd

def main():
	testClass=pyClass.databaseColumnQueryClass()

	testClass.queryText=testClass.getColumnQueryText(
			databaseName=testClass.databaseName,\
			tableName=testClass.tableName
		)

	testClass.runSamplesQuery()
	testClass.getDataFrameHeaders()
	testClass.saveHeadersToExcel()
	testClass.openOutputFile()

main()
