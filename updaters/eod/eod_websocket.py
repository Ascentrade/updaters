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
import asyncio
import websockets
from enum import Enum

from log_config import getNewLogger

class EodWsType(Enum):
	US_QUOTES = 0
	FOREX = 1
	CRYPTO = 2
	US_TRADES = 3

class EodWebsocket():

	WS_URL = [
		# Quotes
		'wss://ws.eodhistoricaldata.com/ws/us-quote',
		'wss://ws.eodhistoricaldata.com/ws/forex',
		'wss://ws.eodhistoricaldata.com/ws/crypto',
		# Trades
		'wss://ws.eodhistoricaldata.com/ws/us'
	]

	def __init__(self, eodApiKey:str, type:EodWsType=EodWsType.US_QUOTES, tickers:list=[]):
		self.apiToken = eodApiKey
		self.type = type
		self.tickers = tickers
		self.logger = getNewLogger('EOD-WS')

	async def websocket_handler(self):
		async for websocket in websockets.connect(EodWebsocket.WS_URL[self.type]+'?api_token='+self.apiToken, open_timeout=30):
			try:
				async for message in websocket:
					self.logger.debug('RAW: ', message)
					data = json.loads(message)
					self.logger.debug('IN: ', data)
					if 'status_code' in data.keys() and data['status_code'] == 200:
						if self.tickers != None and len(self.tickers) > 0:
							subMsg = {'action': 'subscribe', 'symbols': ','.join(self.tickers)}
							outStr = json.dumps(subMsg)
							self.logger.info('OUT: ', outStr)
							await websocket.send(outStr)
					else:
						# TODO: Do something with the data
						pass
			except websockets.ConnectionClosed:
				continue

	async def run(self):
		websocket_task = asyncio.create_task(self.websocket_handler())
		await websocket_task