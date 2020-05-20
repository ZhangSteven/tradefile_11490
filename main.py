# coding=utf-8

"""
Read a Bloomberg AIM trade file, then prodeuce the below 2 files:

1. a trade file in CL trustee format;

2. an accumulated trade file for settlement's own records.

Check the 'samples' directory for samples of the input and two output files.

1) input file: 11490_1.xlsx
2) CL trustee trade file: Order Record of A-HK Equity 200515.xlsx
3) Settlement's own records: Equities_15052020.xlsx
"""

from tradefile_11490.trade import getDatenPositions
from clamc_datafeed.feeder import fileToLines, mergeDictionary
from utils.iter import pop
from utils.file import getFiles
from utils.utility import fromExcelOrdinal
from utils.utility import writeCsv
from toolz.functoolz import compose
from functools import partial
from itertools import chain, count, takewhile
from datetime import datetime
from shutil import copyfile
from os.path import join, dirname, abspath
import logging, csv
logger = logging.getLogger(__name__)



"""
	[String] file => ( [String] date (yyyy-mm-dd)
					 , [Iterator] positions
					 )

	Read a 11490 trade file, get its date and positions
"""
readDatenPositions = compose(
	getDatenPositions
  , fileToLines
)



"""
	Get the absolute path to the directory where this module is in.

	This piece of code comes from:

	http://stackoverflow.com/questions/3430372/how-to-get-full-path-of-current-files-directory-in-python
"""
getCurrentDirectory = lambda: dirname(abspath(__file__))



def writeTrusteeTradeFile(outputDir, date, positions):
	"""
	[String] outputDir (the directory to write the csv file)
	[String] date (yyyy-mm-dd)
	[Iterator] positions (the positions from the trade file) 
		=> [String] csv file

	From the date and positions of the Bloomberg AIM trade file, write the CL 
	trustee trade file.
	"""
	updatePosition = lambda p: \
		mergeDictionary(p, {'Broker Long Name': p['FACC Long Name']})

	headers = [ 'Fund', 'Ticker & Exc', 'ISIN',	'Shrt Name', 'B/S',	'Yield'
			  ,	'As of Dt',	'Stl Date',	'Amount Pennies', 'Price', 'Bkr Comm'
			  ,	'Stamp Duty', 'Exch Fee', 'Trans. Levy', 'Misc Fee', 'Crcy'
			  ,	'Broker Long Name',	'Accr Int', 'Settle Amount', 'L1 Tag Nm'
			  ]

	positionToValues = lambda position: map(lambda key: position[key], headers)


	getOutputFileName = lambda date, outputDir: \
		join( outputDir
			, 'Order Record of A-HK Equity ' + \
				datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%y%m%d') + \
				'.csv'
			)


	getOutputRows = lambda date, positions: \
		chain( [ ['China Life Franklin - CLT-CLI HK BR (CLASS A-HK) TRUST FUND']
			   , ['11490 Equity FOR 11490 ON ' + \
			   		datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%m/%d/%y')
			   	 ]
			   , ['']	# an empty row
			   , headers
			   ]
			 , map(positionToValues, map(updatePosition, positions))
			 )


	return writeCsv( getOutputFileName(date, outputDir)
				   , getOutputRows(date, positions)
				   , delimiter=','
				   )



def writeAccumulateTradeFile(outputDir, date, positions):
	"""
	[String] outputDir (the directory to write the csv file)
	[String] date (yyyy-mm-dd)
	[Iterator] positions (the positions from the trade file) 
		=> [String] csv file

	From the date and positions of the Bloomberg AIM trade file, write the
	accumulated trade file.

	Here we assume that accumulated trade files of previous days are also located
	on the outputDir.
	"""
	outputFile = join( outputDir
					 , 'Equities_' + datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%d%m%Y') + '.csv'
					 )

	copyfile( getNearestAccumulateFile(outputDir, date)
			, outputFile
			)


	toNewPostion = lambda position: mergeDictionary( 
		position
	  , { 'FundName': 'CLT-CLI HK BR (CLASS A-HK) Trust Fund' \
	  					if position['Fund'].startswith('11490') else \
	  					lognRaise('toNewPostion(): invalid fund name {0}'.format(position['Fund']))
		, '': ''
		, 'BuySell': 'Buy' if position['B/S'] == 'B' else 'Sell'
		}
	)


	headers = [ 'FundName', '', 'Ticker & Exc', 'Shrt Name', 'Amount Pennies'
			  , 'BuySell', 'FACC Long Name', 'As of Dt', 'Stl Date', 'Price']

	positionToValues = lambda position: map(lambda key: position[key], headers)


	# [Iterator] positions => [String] output string to be written to the file
	toOutputString = compose(
		lambda rows: '\n'.join(rows)
	  , partial(map, lambda values: ','.join(values))
	  , partial(map, lambda values: map(str, values))
	  , partial(map, positionToValues)
	  , partial(map, toNewPostion) 
	)


	with open(outputFile, 'a') as newFile:
		newFile.write(toOutputString(positions))
		return 0



def getNearestAccumulateFile(outputDir, date):
	"""
	[String] outputDir,
	[String] date (yyyy-mm-dd)
		=> [String] file

	Search for all files in the output dir, then:

	1) Find all files that begins with 'Equities' (accumulate trade files)
	2) Take out those whose date is equal or equal to the date
	3) Sort the remaining files by date, find the file with the latest date.
	4) Return the file name with full path.
	"""
	# [String] fn => [String] date (yyyy-mm-dd)
	getDateFromFilename = compose(
		lambda s: datetime.strftime(datetime.strptime(s, '%d%m%Y'), '%Y-%m-%d')
	  , lambda s: s.split('_')[-1].strip()
	  , lambda fn: fn.split('.')[0]
	)

	isAccumulateTradeFile = lambda fn: fn.startswith('Equities_') and fn.endswith('.csv')

	fileOfLatestDate = lambda filesWithDate: max(filesWithDate, key=lambda t: t[0])[1]


	return compose(
		lambda fn: join(outputDir, fn)
	  , fileOfLatestDate
	  , partial(filter, lambda t: t[0] < date)
	  , partial(map, lambda fn: (getDateFromFilename(fn), fn))
	  , partial(filter, isAccumulateTradeFile)
	  , lambda outputDir, _: getFiles(outputDir)
	)(outputDir, date)



# def mergePositionsToAccumulateTradeFile(file, positions):
# 	"""
# 	[String] file, [Iterator] positions
# 		=> [Iterator] rows

# 	read the accumulate trade csv file, append the positions from the AIM trade 
# 	file to form the rows of the new accumulate trade csv file
# 	"""
# 	logger.debug('mergePositionsToAccumulateTradeFile(): {0}'.format(file))

# 	headers = [ 'FundName', '', 'Security Code', 'Shrt Name', 'Amount Pennies'
# 			  , 'BuySell', 'FACC Long Name', 'As of Dt', 'Stl Date', 'Price']

# 	positionToValues = lambda position: map(lambda key: position[key], headers)


# 	toNewPostion = lambda position: mergeDictionary( 
# 		position
# 	  , { 'FundName': 'CLT-CLI HK BR (CLASS A-HK) Trust Fund' \
# 	  					if position['Fund'].startswith('11490') else \
# 	  					lognRaise('toNewPostion(): invalid fund name {0}'.format(position['Fund']))
# 		, '': ''
# 		, 'Security Code': position['Ticker & Exc'].split()[0]
# 		, 'BuySell': 'Buy' if position['B/S'] == 'B' else 'Sell'
# 		}
# 	)


# 	with open(file, newline='') as csvfile:
# 		csvreader = csv.reader(csvfile, delimiter=',')
# 		return chain( [row for row in csvreader]
# 					, map(positionToValues, map(toNewPostion, positions))
# 					)



def convertAccumulateExcelToCSV(file):
	"""
	[String] file => [String] file

	Read an accmulative trade excel file, write it as csv. We need to make sure:
	make sure dates as yyyy-mm-dd, so that it's consistent with a daily addon
	from the bloomberg aim trade file.

	The csv file name is the same as the excel file, except that its file
	extension is '.csv' instead of '.xlsx'

	This is an utility function that needs to run only once, to convert the 
	excel version accmulate trade file into csv format. After that, we just
	need to add daily trades to that csv file.
	"""
	getOutputFileName = lambda fn: \
		fn[0:-4] + 'csv' if fn.endswith('.xlsx') else \
		fn[0:-3] + 'csv' if fn.endswith('.xls') else \
		lognRaise('convertAccumulateExcelToCSV(): invalid input file {0}'.format(fn))
	

	"""
		[List] line => [List] headers
		Note the second header is an empty string, but we need to keep it. All
		other empty strings in the list are ignored
	"""
	getHeaders = compose(
		list
	  , partial(map, lambda t: t[1])
	  , partial(takewhile, lambda t: t[0] < 2 or t[1] != '')
	  , lambda line: zip(count(), line)
	)


	def toDatetimeString(value):
		if isinstance(value, float):
			return datetime.strftime(fromExcelOrdinal(value), '%Y-%m-%d')
		else:
			try:
				return datetime.strftime(datetime.strptime(value, '%m/%d/%Y'), '%Y-%m-%d')
			except ValueError:
				return datetime.strftime(datetime.strptime(value, '%d/%m/%Y'), '%Y-%m-%d')


	getLineItems = lambda headers, line: compose(
		partial( map
			   , lambda t: toDatetimeString(t[1]) \
			   		if t[0] in ['Trade Date', 'Settlement Date'] else t[1]
			   )
	  , lambda headers, line: zip(headers, line)
	)(headers, line)


	return compose(
		lambda rows: writeCsv( getOutputFileName(file)
							 , rows
							 , delimiter=','
							 )
	  , lambda t: chain( [t[0]]
	  				   , map(partial(getLineItems, t[0]), t[1])
	  				   )
	  , lambda lines: (getHeaders(pop(lines)), lines)
	  , fileToLines
	)(file)




def lognRaise(msg):
	logger.error(msg)
	raise ValueError




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	file = join(getCurrentDirectory(), 'samples', '11490_1_20200519.xls')

	# compose(
	# 	lambda t: writeTrusteeTradeFile('', t[0], t[1])
	#   , readDatenPositions
	# )(file)

	date, positions = readDatenPositions(file)
	outputDir = join(getCurrentDirectory(), 'samples')
	print(writeAccumulateTradeFile(outputDir, date, positions))

	# Convert an accumulate excel trade file to csv
	# file = join(getCurrentDirectory(), 'samples', 'Equities_15052020.xlsx')
	# print(convertAccumulateExcelToCSV(file))