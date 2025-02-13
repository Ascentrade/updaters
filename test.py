"""
 @copyright Copyright (C) 2024 Dennis Greguhn <dev@greguhn.de>
 
 @author Dennis Greguhn <dev@greguhn.de>
 
 @license AGPL-3.0-or-later
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

# https://docs.python.org/3/library/unittest.html

import unittest

from utils import *

class TestSplitParse(unittest.TestCase):

	def test_valid1(self):
		self.assertEqual(parseSplit('10/20'), (Decimal('10'), Decimal('20')))

	def test_valid2(self):
		self.assertEqual(parseSplit('42.123/21.000'), (Decimal('42.123'), Decimal('21.000')))

	def test_splitError(self):
		self.assertRaises(Exception, parseSplit, '10,20')
		
		with self.assertRaises(Exception):
			parseSplit('A/20')

if __name__ == '__main__':
	# Do not run this code by using Visual Studio, instead run ```python test.py```
	unittest.main(verbosity=2)