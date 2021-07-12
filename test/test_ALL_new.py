# coding=utf-8
# 

import unittest2
from os.path import join
from utils.iter import firstOf
from tradefile_11490.trade import getDatenPositions
from tradefile_11490.utility import getCurrentDirectory
from tradefile_11490.main import readDatenPositions, getNearestAccumulateFile



class TestAllNew(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestAllNew, self).__init__(*args, **kwargs)

	def testGetDatenPositions(self):
		file = join(getCurrentDirectory(), 'samples', '11490_new.xlsx')
		date, positions = readDatenPositions(file)
		self.assertEqual('2021-07-09', date)
		positions = list(positions)
		self.assertEqual(6, len(positions))
		self.verifyPosition(positions[0])
		self.verifyPosition2(positions[5])

	# def testGetDatenPositions2(self):
	# 	file = join(getCurrentDirectory(), 'samples', '11500_1.xlsx')
	# 	date, positions = readDatenPositions(file)
	# 	self.assertEqual('2020-05-22', date)
	# 	positions = list(positions)
	# 	self.assertEqual(8, len(positions))
	# 	p = positions[0]
	# 	self.assertEqual('11500', p['Fund'])
	# 	self.assertEqual('2020-05-22', p['As of Dt'])

	# def testGetNearestAccumulateFile(self):
	# 	outputDir = join(getCurrentDirectory(), 'samples')
	# 	self.assertEqual( join(outputDir, 'Equities_13052020.csv')
	# 					, getNearestAccumulateFile(outputDir, '11490', '2020-05-15'))

	# 	self.assertEqual( join(outputDir, 'Equities_BOC_14052020.csv')
	# 					, getNearestAccumulateFile(outputDir, '11500', '2020-05-18'))
	# # End of testGetNearestAccumulateFile

	def verifyPosition(self, p):
		self.assertEqual('11490-D', p['Fund'])
		self.assertEqual('MU US', p['Ticker & Exc'])
		self.assertEqual('US5951121038', p['ISIN'])
		self.assertEqual('S', p['B/S'])
		self.assertEqual('2021-07-08', p['As of Dt'])
		self.assertEqual('2021-07-12', p['Stl Date'])
		self.assertEqual(4161, p['Amount Pennies'])
		self.assertAlmostEqual(76.8836, p['Price'])
		self.assertAlmostEqual(319786.19, p['Settle Amount'])
		self.assertEqual('AFS', p['L1 Tag Nm'])

	def verifyPosition2(self, p):
		self.assertEqual('11490-B', p['Fund'])
		self.assertEqual('MU US', p['Ticker & Exc'])
		self.assertEqual('US5951121038', p['ISIN'])
		self.assertEqual('S', p['B/S'])
		self.assertEqual('2021-07-08', p['As of Dt'])
		self.assertEqual('2021-07-12', p['Stl Date'])
		self.assertEqual(4467, p['Amount Pennies'])
		self.assertAlmostEqual(76.8836, p['Price'])
		self.assertAlmostEqual(343303.27, p['Settle Amount'])
		self.assertEqual('Trading', p['L1 Tag Nm'])