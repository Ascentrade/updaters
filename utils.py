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

import json
from typing import Any
from jsonpath_ng.ext import parse
from datetime import date
from decimal import Decimal, InvalidOperation

def parseSplit(splitStr:str) -> tuple[Decimal, Decimal]:
	"""Parses Decimal numbers from a string which represents a stock split like '10.0/1.0'

	Args:
		splitStr (str): String to parse

	Raises:
		Exception: If split parsing fails.

	Returns:
		tuple[Decimal, Decimal]: (new, old) number of shares.
	"""
	try:
		strings = splitStr.split('/')
		new = Decimal(strings[0])
		old = Decimal(strings[1])
		return (new, old)
	except InvalidOperation as e:
		raise Exception(f'Unable to parse split {splitStr}')


def getObject(dictIn:dict, key:str, default=None):
	"""Save getting of dict values

	Args:
		dictIn (dict): Dictionary in
		key (str): Key to get from the dict

	Returns:
		_type_: Value from dict or default value
	"""
	try:
		return dictIn[key] if key in dictIn else default
	except:
		return default


def renameListDictKeys(listIn: list, names: dict) -> list:
	"""Rename all keys from a list of dicts.

	Args:
		listIn (list): List of dict objects like quotes or dividends
		names (dict): Mapping dict {'oldName':'newName'}

	Returns:
		list: List with renamed dict keys
	"""
	for o in listIn:
		for k in names.keys():
			if k in o.keys():
				o[names[k]] = o[k]
				del o[k]
	return listIn


def parseBoolean(value) -> bool:
	"""Parse an input for boolean True

	Args:
		value: Input to parse

	Returns:
		bool: Return parsed bool. Defaults to False.
	"""
	if isinstance(value, bool):
		return value
	elif isinstance(value, str):
		if len(value) > 0 and value.lower() in ['1', 'true', 't', 'yes', 'y']:
			return True
	return False


def parseInt(value:Any, default:int) -> int:
	"""Try to parse an integer from input (.env) or return default

	Args:
		value (Any): Any input for parsing integer
		default (int): Default if not parsable

	Returns:
		int: Parsed or default int
	"""
	try:
		if value == None:
			return default
		elif isinstance(value, int):
			return value
		elif isinstance(value, str):
			return int(value)
	except:
		pass
	return default


def getJsonPathData(obj:dict, jsonPath:str, default:Any=None) -> Any:
	"""Extract data from JSON/Dict using JSONPath

	Args:
		obj (dict): JSON/Dict input data
		jsonPath (str): JSONPath to the data
		default (Any, optional): Default value if invalid/error. Defaults to None.

	Returns:
		Any: Return data or default value
	"""
	try:
		matches = parse(jsonPath).find(obj)
		if len(matches) == 1:
			return matches[0].value
		if len(matches) > 1:
			return [m.value for m in matches]
	except:
		pass
	return default


def parseDividendPeriod(input:str) -> str:
	"""Clean detection for correct DividendPeriod type for GraphQL. Can be 'Weekly', 'Monthly', 'Quarterly', 'SemiAnnual', 'Annual', 'Other'.

	Args:
		input (str): Input string to check

	Returns:
		str: Correct type or 'Other'
	"""
	try:
		PERIODS = ['Weekly', 'Monthly', 'Quarterly', 'SemiAnnual', 'Annual', 'Other']
		if input == None or type(input) != str or len(input) == 0:
			return 'Other'

		# Lower case comparison
		input = input.lower()
		for idx, p in enumerate(PERIODS):
			if input == p.lower():
				return PERIODS[idx]
	except:
		pass
	return 'Other'


def checkDateString(dateStr:str) -> str:
	try:
		dt = date.fromisoformat(dateStr)
		return dt.isoformat()
	except:
		pass
	return None