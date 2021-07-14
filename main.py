# coding=utf-8

"""
Read a Bloomberg THRP trade file, then prodeuce the below 2 files:

1. a trade file in CL trustee format;

2. an accumulated trade file for settlement's own records.

Check the 'samples' directory for samples of the input and two output files.

1) input file: 11490_1.xlsx
2) CL trustee trade file: Order Record of A-HK Equity 200515.xlsx
3) Settlement's own records: Equities_15052020.xlsx
"""

from tradefile_11490.trade import getDatenPositions
from tradefile_11490.utility import getDataDirectory, getMailSender, getMailServer \
									, getMailRecipients, getMailTimeout
from clamc_datafeed.feeder import fileToLines, mergeDictionary
from utils.iter import pop
from utils.file import getFiles
from utils.utility import fromExcelOrdinal, writeCsv
from utils.mail import sendMail
from toolz.functoolz import compose
from functools import partial
from itertools import chain, count, takewhile
from datetime import datetime
from os.path import join, dirname, abspath
import logging, csv, shutil, sys
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



def writeTradenAccumulateFiles(inputFile, portfolio, dataDirectory):
	"""
	[String] inputFile (AIM trade file)
	[String] portfolio
	[String] dataDirectory
		=> ( [String] output trade file
		   , [String] accumulate trade file
		   )

	Assume that the accumulate trade files will be stored in the output dir
	and don't move. Because when producing the accumulate trade file for the
	current day, we need accumulate trade files of previous days.
	"""
	logger.debug('writeTradenAccumulateFiles(): {0}'.format(inputFile))

	return \
	compose(
		lambda t: ( writeTrusteeTradeFile(dataDirectory, portfolio, t[0], t[1])
				  , writeAccumulateTradeFile(dataDirectory, portfolio, t[0], t[1])
				  )
	  , lambda t: (t[0], list(t[1]))
	  , readDatenPositions
	)(join(dataDirectory, inputFile))



def writeTrusteeTradeFile(outputDir, portfolio, date, positions):
	"""
	[String] outputDir (the directory to write the csv file)
	[String] portfolio
	[String] date (yyyy-mm-dd)
	[Iterator] positions (the positions from the trade file) 
		=> [String] csv file

	From the date and positions of the Bloomberg AIM trade file, write the CL 
	trustee trade file.
	"""
	updatePosition = lambda p: \
		mergeDictionary(p, {'Broker Long Name': p['Firm Account Long Name']})

	headers = [ 'Fund', 'Ticker & Exc', 'ISIN',	'Shrt Name', 'B/S',	'Yield'
			  ,	'As of Dt',	'Stl Date',	'Amount Pennies', 'Price', 'Bkr Comm'
			  ,	'Stamp Duty', 'Exch Fee', 'Trans. Levy', 'Misc Fee', 'Crcy'
			  ,	'Broker Long Name',	'Accr Int', 'Settle Amount', 'L1 Tag Nm'
			  ]

	positionToValues = lambda position: map(lambda key: position[key], headers)


	def getOutputFileName(portfolio, date, outputDir):
		prefix = 'Order Record of A-HK Equity ' if portfolio == '11490' \
					else 'Order Record of A-HK Equity_BOC ' if portfolio == '11500' \
					else 'Order Record of A-MC-P Equity' # 13006
		
		return join( outputDir
				   , prefix	+ datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%y%m%d') + '.csv'
				   )


	getOutputRows = lambda portfolio, date, positions: \
		chain( [ ['China Life Franklin - CLT-CLI HK BR (CLASS A-HK) TRUST FUND' \
					if portfolio == '11490' else \
					'China Life Franklin - CLT-CLI HK BR (CLASS A-HK) TRUST FUND_BOC' \
					if portfolio == '11500' else \
					'CLT-CLI Macau BR (Class A-MC) Trust Fund-Par' #13006
				 ]
			   , ['{0} Equity FOR {0} ON '.format(portfolio) \
			   		+ datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%m/%d/%y')
			   	 ]
			   , ['']	# an empty row
			   , headers
			   ]
			 , map( positionToValues
			 	  , map( updatePosition
			 	  	   , filter(lambda p: p['Fund'].startswith(portfolio), positions)))
			 )


	return writeCsv( getOutputFileName(portfolio, date, outputDir)
				   , getOutputRows(portfolio, date, positions)
				   , delimiter=','
				   )



def writeAccumulateTradeFile(outputDir, portfolio, date, positions):
	"""
	[String] outputDir (the directory to write the csv file)
	[String] portfolio
	[String] date (yyyy-mm-dd)
	[Iterator] positions (the positions from the trade file) 
		=> [String] csv file

	From the date and positions of the Bloomberg AIM trade file, write the
	accumulated trade file.

	Here we assume that accumulated trade files of previous days are also located
	on the outputDir.
	"""
	prefix = 'Equities_' if portfolio == '11490' \
				else 'Equities_BOC_' if portfolio == '11500' \
				else 'Equities_A-MC-P_' # 13006

	outputFile = join( outputDir
					 , prefix + datetime.strftime(datetime.strptime(date, '%Y-%m-%d'), '%d%m%Y') + '.csv'
					 )

	shutil.copyfile( getNearestAccumulateFile(outputDir, portfolio, date)
				   , outputFile
				   )


	toNewPostion = lambda position: mergeDictionary( 
		position
	  , { 'FundName': 'CLT-CLI HK BR (CLASS A-HK) Trust Fund' if position['Fund'].startswith('11490') \
	  					else 'CLT-CLI HK BR (CLASS A-HK) Trust Fund_BOC' if position['Fund'].startswith('11500') \
	  					else 'CLT-CLI Macau BR (Class A-MC) Trust Fund-Par' if position['Fund'].startswith('13006') \
	  					else lognRaise('toNewPostion(): invalid fund name {0}'.format(position['Fund']))
		, '': ''
		, 'BuySell': 'Buy' if position['B/S'] == 'B' else 'Sell'
		}
	)


	headers = [ 'FundName', '', 'Ticker & Exc', 'Shrt Name', 'Amount Pennies'
			  , 'BuySell', 'FACC Long Name', 'As of Dt', 'Stl Date', 'Price']

	positionToValues = lambda position: map(lambda key: position[key], headers)


	# [Iterator] positions => [String] output string to be written to the file
	toOutputString = compose(
		lambda rows: '\n'.join(rows) + '\n'
	  , partial(map, lambda values: ','.join(values))
	  , partial(map, lambda values: map(str, values))
	  , partial(map, positionToValues)
	  , partial(map, toNewPostion)
	  , partial(filter, lambda p: p['Fund'].startswith(portfolio))
	)


	with open(outputFile, 'a') as newFile:
		newFile.write(toOutputString(positions))
		return 0



def getNearestAccumulateFile(outputDir, portfolio, date):
	"""
	[String] outputDir,
	[String] portfolio,
	[String] date (yyyy-mm-dd)
		=> [String] file

	Search for all files in the output dir, then:

	1) Find all the accumulate trade files for the portfolio; 
	2) Find those whose date is less than the date;
	3) Find the file with the latest date.
	4) Return the file name with full path.
	"""
	logger.debug('getNearestAccumulateFile(): start')

	# [String] fn => [String] date (yyyy-mm-dd)
	getDateFromFilename = compose(
		lambda s: datetime.strftime(datetime.strptime(s, '%d%m%Y'), '%Y-%m-%d')
	  , lambda s: s.split('_')[-1].strip()
	  , lambda fn: fn.split('.')[0]
	)

	isAccumulateTradeFile = lambda fn: \
		fn.startswith('Equities_BOC_') if portfolio == '11500' \
		else fn.startswith('Equities_') and not fn.startswith('Equities_BOC_') if portfolio == '11490' \
		else fn.startswith('Equities_A-MC-P_') # 13006

	fileOfLatestDate = lambda filesWithDate: max(filesWithDate, key=lambda t: t[0])[1]


	return compose(
		lambda fn: join(outputDir, fn)
	  , lambda fn: lognContinue('getNearestAccumulateFile(): {0}'.format(fn), fn)
	  , fileOfLatestDate
	  , partial(filter, lambda t: t[0] < date)
	  , partial(map, lambda fn: (getDateFromFilename(fn), fn))
	  , partial(filter, isAccumulateTradeFile)
	  , lambda outputDir, _: getFiles(outputDir)
	)(outputDir, date)



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



"""
	[String] inputDir, [String] portfolio 
		=> [String] trade files for the portfolio
"""
getTradeFilesFromDirectory = lambda inputDir, portfolio: \
compose(
	list
  , partial(filter, lambda fn: fn in [portfolio+'_1.xls', portfolio+'_1.xlsx'])
  , getFiles
)(inputDir)



def moveTradeFile(file, inputDir):
	"""
	[String] file, [String] inputDir 
		=> [Int] 0 if successful

	Side effect: Move file in the input directory to its subdirectory
	'processed files'
	"""
	logger.debug('moveTradeFile(): start')

	dtString =  datetime.strftime(datetime.now(), '_%Y%m%d_%H%M%S')
	
	# [String] fn => [String] fn (new file name with date time string attached)
	toNewFilename = compose(
		lambda L: L[0] + dtString + '.' + L[1]
	  , lambda L: lognRaise('moveTradeFile(): invalid file name') \
	  				if len(L) != 2 else L
	  , lambda fn: fn.split('.')
	  , lambda fn: lognContinue('toNewFilename(): {0}'.format(fn), fn)
	)

	shutil.move( join(inputDir, file)
			   , join(inputDir, 'processed files', toNewFilename(file)))

	return 0



def sendNotification(subject):
	"""
	input: [String] result (file name)
	
	side effect: send notification email to recipients about the results.
	"""
	try:
		sendMail( '', subject, getMailSender(), getMailRecipients()
				, getMailServer(), getMailTimeout()
				)

	except:
		logger.exception('sendNotification()')
		


def lognRaise(msg):
	logger.error(msg)
	raise ValueError



def lognContinue(msg, x):
	logger.debug(msg)
	return x




if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	import argparse
	parser = argparse.ArgumentParser(description='Process CL Trustee THRP File')
	parser.add_argument( 'portfolio', metavar='portfolio', type=str
					   , help='for which portfolio')

	"""
		Convert a trade file, do

		$python main.py <portfolio code>

		E.g., convert 11490 trade file, do

		$python main.py 11490
	"""
	portfolio = parser.parse_args().portfolio
	files = getTradeFilesFromDirectory(getDataDirectory(), portfolio)

	import sys
	if not portfolio in ('11490', '11500', '13006'):
		logger.error('invalid portfolio code: {0}'.format(portfolio))
		sys.exit(1)

	elif len(files) == 0:
		logger.debug('no input file found for {0}'.format(portfolio))
		sys.exit(0)

	elif len(files) > 1:
		logger.error('{0} files found for {1}'.format(len(files), portfolio))
		sys.exit(1)
	
	else:
		inputFile = files[0]


	try:
		writeTradenAccumulateFiles(inputFile, portfolio, getDataDirectory())
		moveTradeFile(inputFile, getDataDirectory())
		sendNotification('Successfully performed CL trustee {0} trade conversion'.format(portfolio))

	except:
		logger.exception('__main__')
		sendNotification('Error occurred in performing CL trustee {0} trade conversion'.format(portfolio))