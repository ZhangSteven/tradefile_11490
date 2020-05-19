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
from utils.utility import writeCsv
from toolz.functoolz import compose
from itertools import chain
from datetime import datetime
from os.path import join, dirname, abspath
import logging
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



def writeTrusteeTradeFile(file, outputDir):
	"""
	[String] file => [String] csv file

	Read the Bloomberg AIM trade file, write the CL trustee trade file
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


	return compose(
		lambda t: writeCsv( getOutputFileName(t[0], outputDir)
	  					  , getOutputRows(t[0], t[1])
	  					  , delimiter=','
	  					  )
	  , lambda file, _: readDatenPositions(file)
	)(file, outputDir)



if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	file = join(getCurrentDirectory(), 'samples', '11490_1.xlsx')

	# def showList(L):
	# 	for x in L:
	# 		print(x)

	# 	return 0


	# compose(
	# 	lambda t: showList(t[1])
	#   , getDatenPositions
	#   , fileToLines
	# )(file)

	writeTrusteeTradeFile(file, '')