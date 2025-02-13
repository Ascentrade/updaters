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
import httpx
from datetime import date
from decimal import Decimal
from urllib.parse import urljoin

from .eod_user_data import EODUserData

EODHD_API_URL = 'https://eodhd.com/api/'
EODHD_IMG_URL = 'https://eodhd.com/img/'


class EODHDAsyncClient():
	def __init__(self, apiKey:str):
		self._apiKey = apiKey
		self._httpClient = httpx.AsyncClient(timeout=60)
		self.userData = EODUserData()


	async def _restGet(self, path:str, params:dict={}) -> json:
		params['api_token'] = self._apiKey
		params['fmt'] = 'json'
		response = await self._httpClient.get(url=EODHD_API_URL+path, params=params)
		if response.status_code == 200:
			return json.loads(response.content, parse_float=Decimal)
		return None


	async def downloadLogo(self, logoUrl:str) -> bytes:
		#https://eodhd.com/img/logos/US/aapl.png
		url = urljoin(EODHD_IMG_URL, logoUrl)
		response = await self._httpClient.get(url)
		if response.status_code == 200:
			return response.content
		return None


	async def getUserData(self) -> EODUserData:
		result = await self._restGet('user')
		self.userData = EODUserData(**result)
		return self.userData


	async def getExchangesList(self) -> json:
		return await self._restGet('exchanges-list')


	async def getExchangeDetails(self, exchange:str='US') -> json:
		return await self._restGet(f'exchange-details/{exchange.upper()}')


	async def getExchangeSymbolList(self, exchange:str='US', delisted:bool=False) -> json:
		params = {'delisted':'1'} if delisted else {}
		return await self._restGet(f'exchange-symbol-list/{exchange.upper()}', params)


	async def getOptions(self, ticker:str) -> json:
		return await self._restGet(f'options/{ticker.upper()}')


	async def getFundamentals(self, ticker:str) -> json:
		return await self._restGet(f'fundamentals/{ticker.upper()}')


	async def _getBulk(self, exchange:str, params:dict, date:date=None) -> json:
		if date != None:
			params['date'] = date.isoformat()
		return await self._restGet(f'eod-bulk-last-day/{exchange.upper()}', params=params)
	

	async def getBulkQuotes(self, exchange:str='US', date:date=None) -> json:
		return await self._getBulk(exchange=exchange, params={}, date=date)
	

	async def getBulkSplits(self, exchange:str='US', date:date=None) -> json:
		return await self._getBulk(exchange=exchange, params={'type':'splits'}, date=date)
	

	async def getBulkDividends(self, exchange:str='US', date:date=None) -> json:
		return await self._getBulk(exchange=exchange, params={'type':'dividends'}, date=date)
	

	async def _getWithDateRange(self, path:str, fromDate:date=None, toDate:date=None, params:dict={}) -> json:
		if fromDate != None:
			params['from'] = fromDate.isoformat()
		if toDate != None:
			params['to'] = toDate.isoformat()
		return await self._restGet(path, params=params)


	async def getSplitAdjustedQuotes(self, ticker:str, fromDate:date=None, toDate:date=None) -> json:
		# Use technical API endpoint to receive split adjusted candles
		params = {'function':'splitadjusted'}
		return await self._getWithDateRange(f'technical/{ticker.upper()}', fromDate, toDate, params)


	async def getHistoricalQuotes(self, ticker:str, fromDate:date=None, toDate:date=None) -> json:
		return await self._getWithDateRange(f'eod/{ticker.upper()}', fromDate, toDate)
	

	async def getHistoricalDividends(self, ticker:str, fromDate:date=None, toDate:date=None) -> json:
		return await self._getWithDateRange(f'div/{ticker.upper()}', fromDate, toDate)
	

	async def getHistoricalSplits(self, ticker:str, fromDate:date=None, toDate:date=None) -> json:
		return await self._getWithDateRange(f'splits/{ticker.upper()}', fromDate, toDate)