# coding=utf-8

"""
Read a Bloomberg AIM trade file, get its date and all the positions (trades).

"""
from clamc_datafeed.feeder import mergeDictionary
from utils.iter import firstOf, pop
from utils.utility import fromExcelOrdinal
from toolz.functoolz import compose
from functools import partial
from itertools import takewhile
from datetime import datetime
import logging
logger = logging.getLogger(__name__)



def getDatenPositions(lines):
	"""
	[Iterable] lines => ( [String] date (yyyy-mm-dd)
						, [Iterable] positions
						)

	Read the lines from the input Excel file, then produce 
	"""
	logger.debug('getDatenPositions(): start')

	return ( getDateFromLines(lines)
		   , map(updatePosition, getPositionsFromLines(lines))
		   )



def getDateFromLines(lines):
	"""
	[Iterable] lines => [String] date (yyyy-mm-dd)

	Read date from the second line.
	"""
	pop(lines)	# skip the first line

	return compose(
		lambda s: datetime.strftime( datetime.strptime(s, '%d/%m/%y')
								   , '%Y-%m-%d')
	  , lambda line: line[0].split()[-1]
	  , pop
	)(lines)



def getPositionsFromLines(lines):
	"""
	[Iterator] lines => [Iterator] positions
	"""
	getHeaderLine = lambda lines: \
		firstOf(lambda line: len(line) > 0 and line[0] == 'Trader Name', lines)


	getHeaderFromLine = compose(
		list
	  , partial(takewhile, lambda s: s != '')
	)


	toPosition = lambda headers, line: \
		dict(zip(headers, line))


	return compose(
		lambda t: map(partial(toPosition, getHeaderFromLine(t[0])), t[1])
	  , lambda t: lognRaise('getPositionsFromLines(): failed to get header line') \
	  				if t[0] == None else t
	  , lambda lines: (getHeaderLine(lines), lines)
	)(lines)



toStringIfFloat = lambda x: \
	str(int(x)) if isinstance(x, float) else x



"""
	[Dictionary] p => [Dictionary] p

	Change two date fields (As of Dt, Stl Date) into a string format (yyyy-mm-dd)
"""
updatePosition = lambda p: \
	mergeDictionary( p
				   , { 'Fund': toStringIfFloat(p['Trader Name'])
				     , 'Ticker & Exc': p['Ticker and Exchange Code']
				     , 'ISIN': p['ISIN Number']
				     , 'Shrt Name': p['Short Name']
				     , 'Crcy': p['Currency']
				     , 'B/S': p['Buy/Sell']
				     , 'Amount Pennies': p['Amount (Pennies)']
				     , 'Price': p['Trade price']
				     , 'Settle Amount': p['Settlement Total in Settlemen']
				     , 'Bkr Comm': p['Transaction Cost 1 Amount']
				     , 'Stamp Duty': p['Transaction Cost 2 Amount']
				     , 'Exch Fee': p['Transaction Cost 3 Amount']
				     , 'Trans. Levy': p['Transaction Cost 4 Amount']
				     , 'Misc Fee': p['Transaction Cost 5 Amount']
				   	 , 'As of Dt': datetime.strftime( fromExcelOrdinal(p['As of Date'])
											 		, '%Y-%m-%d')
				   	 , 'Stl Date': datetime.strftime( fromExcelOrdinal(p['Settlement Date'])
											 		, '%Y-%m-%d')
				   	 , 'Accr Int': p['Accrued Interest']
				   	 , 'FACC Long Name': p['Firm Account Long Name']
				   	 , 'L1 Tag Nm': 'Trading' if p['Level 1 Tag Name'] == 'AFS' and p['Trader Name'] == '11490-B' \
				   	 				else p['Level 1 Tag Name']
				   	 }
				   )



def lognContinue(msg, x):
	logger.debug(msg)
	return x


def lognRaise(msg):
	logger.error(msg)
	raise ValueError
