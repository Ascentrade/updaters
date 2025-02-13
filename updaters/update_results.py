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

from log_config import getNewLogger

class UpdateResults:
	"""UpdateResults collects all performed updates from an Updater class.
	"""

	def __init__(self, name:str):
		self.name = name
		self.results = {}
		self.logger = getNewLogger(f'{name}-results')


	def add(self, method:str, ticker:str, success:bool, id:int=None):
		"""Adds the result of an update operation to the list.

		Args:
			method (str): Method used for update e.g.: 'Options', 'Fundamentals', 'Dividends',...
			ticker (str): Ticker of the updated security
			success (bool): Result of the operation
			id (int, optional): ID of the security if available.
		"""
		if method != None and len(method) > 0:
			if method not in self.results.keys():
				self.results[method] = []

			self.results[method].append({
				'ticker': ticker,
				'id': id,
				'success': success
			})
		else:
			self.logger.warning(f'Invalid method name for UpdateResults')