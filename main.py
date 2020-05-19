# coding=utf-8

"""
Since 2020 Apr, BOCHK revised its cash report format, holding report not affected.

Therefore we will use bochk_revised package to handle holding report, but handle 
cash with a new set of logic here.
"""
from tradefile_11490.trade import getDatenPositions
from clamc_datafeed.feeder import fileToLines
from toolz.functoolz import compose
from os.path import join, dirname, abspath
import logging
logger = logging.getLogger(__name__)



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



if __name__ == '__main__':
	import logging.config
	logging.config.fileConfig('logging.config', disable_existing_loggers=False)

	file = join(getCurrentDirectory(), 'samples', '11490_1.xlsx')

	def showList(L):
		for x in L:
			print(x)

		return 0


	compose(
		lambda t: showList(t[1])
	  , getDatenPositions
	  , fileToLines
	)(file)