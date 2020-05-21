# coding=utf-8
# 

import unittest2
from os.path import join
from utils.iter import firstOf
from tradefile_11490.trade import getDatenPositions
from tradefile_11490.utility import getCurrentDirectory
from tradefile_11490.main import readDatenPositions, getNearestAccumulateFile



class TestAll(unittest2.TestCase):

	def __init__(self, *args, **kwargs):
		super(TestAll, self).__init__(*args, **kwargs)



	def testGetDatenPositions(self):
		file = join(getCurrentDirectory(), 'samples', '11490_1_20200515.xlsx')
		date, positions = readDatenPositions(file)
		self.assertEqual('2020-05-15', date)
		positions = list(positions)
		self.assertEqual(5, len(positions))
		self.verifyPosition(positions[0])
		self.verifyPosition2(positions[1])



	def testGetNearestAccumulateFile(self):
		outputDir = join(getCurrentDirectory(), 'samples')
		self.assertEqual( join(outputDir, 'Equities_13052020.csv')
						, getNearestAccumulateFile(outputDir, '2020-05-15'))



	def verifyPosition(self, p):
		self.assertEqual('11490-B', p['Fund'])
		self.assertEqual('9996 HK', p['Ticker & Exc'])
		self.assertEqual('KYG6981F1090', p['ISIN'])
		self.assertEqual('B', p['B/S'])
		self.assertEqual('2020-05-15', p['As of Dt'])
		self.assertEqual('2020-05-19', p['Stl Date'])
		self.assertEqual(167000, p['Amount Pennies'])
		self.assertAlmostEqual(26.3641, p['Price'])
		self.assertAlmostEqual(4416352.33, p['Settle Amount'])
		self.assertEqual('Trading', p['L1 Tag Nm'])



	def verifyPosition2(self, p):
		self.assertEqual('11490-D', p['Fund'])
		self.assertEqual('941 HK', p['Ticker & Exc'])
		self.assertEqual('HK0941009539', p['ISIN'])
		self.assertEqual('B', p['B/S'])
		self.assertEqual('2020-05-15', p['As of Dt'])
		self.assertEqual('2020-05-19', p['Stl Date'])
		self.assertEqual(250500, p['Amount Pennies'])
		self.assertAlmostEqual(56.8313, p['Price'])
		self.assertAlmostEqual(14270080.95, p['Settle Amount'])
		self.assertEqual('AFS', p['L1 Tag Nm'])