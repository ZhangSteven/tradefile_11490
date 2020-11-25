# coding=utf-8

"""
Read a Bloomberg AIM trade file, get its date and all the positions (trades).

"""
from clamc_datafeed.feeder import mergeDictionary
from utils.iter import firstOf
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

	Search for the line that contains the date, then extract the date from it.
	"""
	convertDate = lambda s: datetime.strftime( datetime.strptime(s, '%m/%d/%y')
											 , '%Y-%m-%d')

	return compose(
		convertDate
	  , lambda line: line[0].split()[-1]
	  , lambda line: lognRaise('getDateFromLines(): could not find the date line') \
	  					if line == None else line
	  , partial( firstOf
	  		   , lambda line: len(line) > 0 and isinstance(line[0], str) \
	  							and	'Fundcode ON' in line[0])
	)(lines)



def getPositionsFromLines(lines):
	"""
	[Iterator] lines => [Iterator] positions
	"""
	getHeaderLine = lambda lines: \
		firstOf(lambda line: len(line) > 0 and line[0] == 'Fund', lines)


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
				   , { 'Fund': toStringIfFloat(p['Fund'])
				   	 , 'As of Dt': datetime.strftime( fromExcelOrdinal(p['As of Dt'])
											 		, '%Y-%m-%d')
				   	 , 'Stl Date': datetime.strftime( fromExcelOrdinal(p['Stl Date'])
											 		, '%Y-%m-%d')
				   	 , 'L1 Tag Nm': 'Trading' if p['L1 Tag Nm'] == 'AFS' and p['Fund'] == '11490-B' \
				   	 				else p['L1 Tag Nm']
				   	 }
				   )




def lognContinue(msg, x):
	logger.debug(msg)
	return x


def lognRaise(msg):
	logger.error(msg)
	raise ValueError
