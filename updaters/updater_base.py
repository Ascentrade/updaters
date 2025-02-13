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

import os
import json
import httpx
import asyncio
import pandas as pd

from log_config import getNewLogger

from threading import Lock

from .queue_object import QueueObject
from .update_results import UpdateResults

from ascentrade_client import AscentradeClient


class UpdaterBase():
	def __init__(self, name:str, authToken:str):
		self.name = name
		self.authToken = authToken
		self.client = AscentradeClient(
			os.environ.get('graphql_host'),
			http_client=httpx.AsyncClient(timeout=60.0),
			headers={'x-auth-token':authToken}
		)
		self.logger = getNewLogger(name)
		self.results = UpdateResults(name)
		self.queue = asyncio.Queue()
		self.lock = Lock()
		self.cancelled = False
		self.allTickers:pd.DataFrame
	

	async def updateTickers(self):
		"""Update the local copy of all available tickers
		"""
		try:
			# Get all tickers
			self.logger.info(f'Get all available tickers...')
			data = await self.client.all_security_tickers()
			tickers = []
			for t in data.securities:
				tickers.append({
					'id': t.id,
					'code': t.code,
					'last_update': t.last_update,
					'is_delisted': t.is_delisted,
					'exchange_code': t.exchange.code,
					'virtual_exchange': t.exchange.virtual_exchange
				})
			self.allTickers = pd.DataFrame(tickers)
			self.logger.info(f'Got {len(self.allTickers)} tickers')
		except Exception as e:
			self.logger.error(f'updateTickers() failed!')
			self.logger.error(e)


	def checkKnownTicker(self, symbol:str, exchangeCode:str, delisted:bool=False) -> bool:
		"""Checks if a stock does already exist.

		Args:
			symbol (str): Stock symbol
			exchangeCode (str): Exchnage code
			delisted (bool): listing state of the stocks to check

		Returns:
			bool: Returns True, if the stock is already in the database. Otherwise False.
		"""
		if symbol == None or exchangeCode == None:
			raise Exception(f'invalid input parameters for checkKnownTicker({symbol}, {exchangeCode})')

		symbol = symbol.upper()
		exchangeCode = exchangeCode.upper()
		filtered = self.allTickers.query('code == @symbol and (exchange_code == @exchangeCode or virtual_exchange == @exchangeCode) and is_delisted == @delisted')
		if len(filtered) > 0:
			return True
		return False


	async def queueObject(self, type:str, object:json, context:dict={}):
		"""Helper function to add **QueueObject** elements to the internal queue

		Args:
			type (str): Unique name for the object type, used from the consumer task
			object (json): JSON data from REST API
			context (dict, optional): Optional context data. Defaults to {}.
		"""
		await self.queue.put(QueueObject(type, object, context))


	async def dequeueObject(self) -> QueueObject:
		"""Helper function to allow correct typing

		Returns:
			QueueObject: Dataclass object from queue
		"""
		return await self.queue.get()