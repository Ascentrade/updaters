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
import simplejson as json
import pause
import base64
import asyncio
import aiofiles
import pandas as pd
from io import StringIO
from decimal import Decimal
from datetime import date, datetime, timedelta

from .eodhd_async import EODHDAsyncClient
from .eod_websocket import EodWebsocket, EodWsType

from utils import parseSplit, parseBoolean, parseInt, getJsonPathData, parseDividendPeriod, checkDateString
from updaters import UpdaterBase

from tickers import TOP_US_STOCKS, TOP_US_ETFS


class EODUpdater(UpdaterBase):
	"""Updater for EOD Historical Data https://eodhd.com/
	"""

	def __init__(self, eodApiKey:str, authToken:str):
		# Init base class with updater name and the global auth token
		super().__init__('EOD', authToken)
		self.eodAsyncClient = EODHDAsyncClient(eodApiKey)
		self.websocket = EodWebsocket(eodApiKey)
		self.tickersToUpdate = []
		self.firstRun = True

		# Parse API usage limit
		self.eodApiLimitReserve = parseInt(os.environ.get('eod_api_limit_reserve'), 10000)

		# Create output folder
		subfolders = ['fundamentals', 'quotes', 'quotes-split-adjusted']
		self.basePath = os.path.join(os.environ.get('data_folder'), self.name)
		if os.path.exists(self.basePath) == False:
			self.logger.info(f'Create EOD output folder {self.basePath}')
			os.makedirs(self.basePath)
		for f in subfolders:
			fp = os.path.join(self.basePath, f)
			if os.path.exists(fp) == False:
				self.logger.info(f'Create folder {fp}')
				os.makedirs(fp)


	async def writeDataToFile(self, data:json, folder:str, filename:str):
		"""Dump JSON response to file

		Args:
			data (json): JSON to write
			folder (str): Sub folder name to write the file
			filename (str): Filename without .json extension
		"""
		try:
			path = os.path.join(self.basePath, folder, f'{filename}.json')
			async with aiofiles.open(path, 'w') as f:
				await f.write(json.dumps(data))
				await f.flush()
		except Exception as e:
			self.logger.error(f'Error while writing to file {folder}/{filename}')
			self.logger.error(e)


	def addUpdateTicker(self, symbol:str, exchange:str):
		"""EOD helper function to collect updateable tickers

		Args:
			symbol (str): Ticker symbol like 'AAPL'
			exchange (str): Exchange symbol like 'NASDAQ' for US stocks 'US' is also valid
		"""
		if symbol != None and exchange != None:
			o = {
				'symbol': symbol.upper(),
				'exchange': exchange.upper(),
				'ticker': f'{symbol.upper()}.{exchange.upper()}'
			}

			with self.lock:
				for e in self.tickersToUpdate:
					if e['ticker'] == o['ticker']:
						return

				self.tickersToUpdate.append(o)


	async def processLogo(self, logoUrl:str) -> str:
		"""Function to download stock logo from EODHD, generate BASE64 string and save to file

		Args:
			logoUrl (str): URL of the logo

		Returns:
			str: BASE64 representation of the logo or None
		"""
		logoBase64 = None
		try:
			imgBytes = await self.eodAsyncClient.downloadLogo(logoUrl)
			if imgBytes != None:
				logoBase64 = base64.encodebytes(imgBytes).decode("utf-8").replace('\n','').replace('\r', '')
				# Save logo PNG file
				path = os.path.join(os.environ.get('data_folder'), '.'+os.path.dirname(logoUrl))
				if not os.path.exists(path):
					os.makedirs(path)
				filename = logoUrl.split('/')[-1]
				writePath = os.path.join(path, filename)
				self.logger.debug(f'Try to write logo image to {writePath}')
				async with aiofiles.open(writePath, 'wb') as f:
					await f.write(imgBytes)
					await f.flush()
		except Exception as e:
			self.logger.warning(f'Unable to get logo {logoUrl}')
			self.logger.warning(e)
		return logoBase64


	async def fullUpdate(self, symbol:str, exchangeCode:str) -> bool:
		"""Helper function to perform a full update of a single security

		Args:
			symbol (str): Ticker symbol like 'AAPL'
			exchangeCode (str): Exchange code of this ticker ('US')

		Returns:
			bool: Return True if all updates where successful
		"""
		try:
			eodTicker = symbol + '.' + exchangeCode
			self.logger.info(f'Start full update for {eodTicker}')

			try:
				# Fundamentals update
				fundamentals = await self.eodAsyncClient.getFundamentals(eodTicker)
				if fundamentals == None:
					self.logger.warning(f'Got no fundamentals data for {symbol}.{exchangeCode}')
					return False
				
				# Try to download company logo
				logoBase64 = None
				logoUrl = getJsonPathData(fundamentals, '$.General.LogoURL', None)
				if logoUrl != None:
					logoBase64 = await self.processLogo(logoUrl)

				# Queue fundamentals
				await self.queueObject('fundamentals', fundamentals, {'logo_base64': logoBase64, 'logo_url': logoUrl})
				# Save JSON file
				await self.writeDataToFile(fundamentals, 'fundamentals', eodTicker)
			except Exception as e:
				self.logger.error(f'Error while getting fundamentals for {eodTicker}')
				self.logger.error(e)

			# Historical quotes (adjusted and raw)
			try:
				# [{'date': '2024-04-16', 'open': 171.71, 'high': 173.755, 'low': 168.27, 'close': 169.38, 'adjusted_close': 169.38, 'volume': 72646896}]
				self.logger.debug(f'Update end of day quotes for {eodTicker}')
				quotes = await self.eodAsyncClient.getHistoricalQuotes(eodTicker)
				await self.writeDataToFile(quotes, 'quotes', eodTicker)
				splitQuotes = await self.eodAsyncClient.getSplitAdjustedQuotes(eodTicker)
				await self.writeDataToFile(splitQuotes, 'quotes-split-adjusted', eodTicker)
				self.logger.debug(f'Got {len(quotes)} quotes and {len(splitQuotes)} split-adjusted quotes for {eodTicker}')
				# Put the data into the queue
				await self.queueObject('quotes', {'quotes':quotes, 'splitAdjusted':splitQuotes}, {'code': symbol, 'exchange_code': exchangeCode})
			except Exception as e:
				self.logger.error(f'Error while getting historical quotes for {eodTicker}')
				self.logger.error(e)

			# Historical Dividends
			try:
				dividends = await self.eodAsyncClient.getHistoricalDividends(eodTicker)
				await self.queueObject('dividends', dividends, {'code': symbol, 'exchange_code': exchangeCode})
			except Exception as e:
				self.logger.error(f'Error while getting historical dividends for {eodTicker}')
				self.logger.error(e)

			# Historical Splits
			try:
				splits = await self.eodAsyncClient.getHistoricalSplits(eodTicker)
				await self.queueObject('splits', splits, {'code': symbol, 'exchange_code': exchangeCode})
			except Exception as e:
				self.logger.error(f'Error while getting historical splits for {eodTicker}')
				self.logger.error(e)

			return True
		except Exception as e:
			self.logger.error(f'fullUpdate({symbol}.{exchangeCode}) failed!')
			self.logger.error(e)
		return False


	async def getExchangeTickers(self, exchange='US', delisted=False):
		"""Helper function to get all listed and delisted exchange tickers from EODHD

		Args:
			exchange (str, optional): Exchange code to use. Defaults to 'US'.
			delisted (bool, optional): True = Get delisted stocks only. Defaults to False.
		"""
		try:
			self.logger.info(f'getExchangeTickers(exchange={exchange}, delisted={delisted})')
			exSymbols = await self.eodAsyncClient.getExchangeSymbolList(delisted=delisted)
			if exSymbols != None and len(exSymbols) > 0:
				self.logger.info(f'got {len(exSymbols)} {"delisted" if delisted else ""} exchange tickers for {exchange}')
				exSymbolsDf = pd.DataFrame(exSymbols)
				# Filter relevant data (Stocks, ETFs)
				exSymbolsDf.query("Type == 'Common Stock' or Type == 'ETF'", inplace=True)
				exSymbolsDf['Type'] = exSymbolsDf['Type'].replace(['Common Stock'], 'Stock')
				exSymbolsDf['is_delisted'] = delisted
				# Map to correct column namnes
				exSymbolsDf.rename({
					'Code':'code',
					'Name':'name',
					'Exchange':'exchange_code',
					'Country':'country_alpha3',
					'Currency':'currency_iso_code',
					'Type':'type',
					'Isin':'isin'
				}, axis=1, inplace=True)
				exSymbolsDf.reset_index(drop=True, inplace=True)
				await self.queueObject('exchange-tickers', exSymbolsDf.to_json(), {'exchange':exchange})
			else:
				self.logger.warning(f'No exchange tickers!')
		except Exception as e:
			self.logger.error(f'getExchangeTickers({exchange})')
			self.logger.error(e)


	async def getBulkEodQuotes(self, updateDate:date=None):
		"""Helper function to get all end of day (eod) quotes
		"""
		try:
			self.logger.info(f'getBulkEodQuotes(updateDate={updateDate})')
			# [{'code': 'AAPL', 'exchange_short_name': 'US', 'date': '2024-04-16', 'open': 171.71, 'high': 173.755, 'low': 168.27, 'close': 169.38, 'adjusted_close': 169.38, 'volume': 72646896}]		
			bulkQuotes = await self.eodAsyncClient.getBulkQuotes(date=updateDate)
			self.logger.info(f'got {len(bulkQuotes)} bulk quotes')
			await self.queueObject('bulk-quotes', bulkQuotes)
		except Exception as e:
			self.logger.error(f'Error while getting bulk EOD quotes!')
			self.logger.error(e)


	async def getBulkEodSplits(self, updateDate:date=None):
		"""Helper function to get all end of day (eod) splits
		"""
		try:
			self.logger.info(f'getBulkEodSplits(updateDate={updateDate})')
			# [{'code': 'NCNA', 'exchange': 'US', 'date': '2024-04-16', 'split': '1.000000/25.000000'}]
			splits = await self.eodAsyncClient.getBulkSplits(date=updateDate)
			self.logger.info(f'got {len(splits)} bulk splits')
			# Mark this stock for full update
			for stock in splits:
				self.addUpdateTicker(stock['code'], stock['exchange'])
			await self.queueObject('bulk-splits', splits)
		except Exception as e:
			self.logger.error(f'Error while getting bulk EOD splits!')
			self.logger.error(e)


	async def getBulkEodDividends(self, updateDate:date=None):
		"""Helper function to get all end of day (eod) dividends
		"""
		try:
			self.logger.info(f'getBulkEodDividends(updateDate={updateDate})')
			# [{'code': 'DCOM', 'exchange': 'US', 'date': '2024-04-16', 'dividend': '0.25000', 'currency': 'USD', 'declarationDate': '2024-03-28', 'recordDate': '2024-04-17', 'paymentDate': '2024-04-24', 'period': 'Quarterly', 'unadjustedValue': '0.2500000000'}]
			dividends = await self.eodAsyncClient.getBulkDividends(date=updateDate)
			self.logger.info(f'got {len(dividends)} bulk dividends')
			# Mark this stock for full update
			for stock in dividends:
				self.addUpdateTicker(stock['code'], stock['exchange'])
			await self.queueObject('bulk-dividends', dividends)
		except Exception as e:
			self.logger.error(f'Error while getting bulk EOD dividends!')
			self.logger.error(e)


	@staticmethod
	def extractEtfTickers(data:dict) -> list:
		"""Extract all stock tickers from an EODHD ETF fundamental JSON object

		Args:
			data (dict): Fundamentals JSON data from EODHD API

		Returns:
			list: List with all tickers
		"""
		tickers = []
		try:
			holdings:dict = data['ETF_Data']['Holdings']
			for key, value in holdings.items():
				# key: AAPL.US
				# value: dict with 'Code' and 'Exchange'
				if value['Code'] not in tickers:
					tickers.append(value['Code'])
		except:
			pass
		return tickers


	async def restGetter(self):
		"""This function receives all data from the EOD REST API and put the data into the queue. Called at the beginning and once a day to do all updates.
		"""
		try:
			self.logger.debug(f' start restGetter() task')
			updatedTickers = []

			# Update user data / API limits
			await self.eodAsyncClient.getUserData()
			self.logger.info(self.eodAsyncClient.userData)

			# Get current tickers from backend
			await self.updateTickers()

			# Daily update
			try:
				if parseBoolean(os.environ.get('eod_update_daily_run')):
					today = datetime.today()
					# Monday (0) .. Sunday (6)
					if today.weekday() in [1, 2, 3, 4, 5]:
						# Update trading data, splits and dividends after a trading day
						await self.getExchangeTickers()
						# Bulk end of day quotes
						await self.getBulkEodQuotes()
						# Bulk end of day splits
						await self.getBulkEodSplits()
						# Bulk end of day dividends
						await self.getBulkEodDividends()

			except Exception as e:
				self.logger.error()
			
			# Updates at the first start
			if self.firstRun == True:
				daysToBulkUpdate = []
				try:
					strList = os.environ.get('eod_update_days').replace(' ', '').split(',')
					daysToBulkUpdate = [datetime.fromisoformat(e).date() for e in strList]
					self.logger.info(f'Updating following days: {daysToBulkUpdate}')
				except:
					pass

				for d in daysToBulkUpdate:
					try:
						# Bulk end of day quotes
						await self.getBulkEodQuotes(d)
						# Bulk end of day splits
						await self.getBulkEodSplits(d)
						# Bulk end of day dividends
						await self.getBulkEodDividends(d)
					except Exception as e:
						self.logger.warning(f'Error while processing eod_update_days: {os.environ.get("eod_update_days")}')
						self.logger.warning(e)

				if parseBoolean(os.environ.get('eod_update_delisted')):
					# Load all delisted tickers
					await self.getExchangeTickers(delisted=True)

				if parseBoolean(os.environ.get('eod_initial_run')):
					# Update SPY, QQQ and IWM stocks on the first
					self.logger.info('Start initial run, getting ETF data...')
					spy = await self.eodAsyncClient.getFundamentals('SPY.US')
					qqq = await self.eodAsyncClient.getFundamentals('QQQ.US')
					iwm = await self.eodAsyncClient.getFundamentals('IWM.US')
					etfTickers = self.extractEtfTickers(spy)
					etfTickers = etfTickers + self.extractEtfTickers(qqq)
					etfTickers = etfTickers + self.extractEtfTickers(iwm)
					etfTickers = etfTickers + ['XLE', 'XLF', 'XLU', 'XLI', 'GDX', 'XLK', 'XLV', 'XLP', 'XLB', 'XOP', 'IYR', 'XHB', 'ITB', 'VNQ', 'GDXJ', 'IYE', 'OIH', 'XME', 'XRT', 'SMH', 'IBB', 'KBE', 'KRE', 'XTL']

					self.logger.info(f'Do full update of {len(etfTickers)} ETF stocks...')
					for idx, stock in enumerate(etfTickers):
						# Skip if already updated
						if stock in updatedTickers:
							continue
						self.logger.info(f'Update ETF stock {stock} - {idx+1}/{len(etfTickers)}')
						await self.fullUpdate(stock, 'US')
						updatedTickers.append(stock)
					
					# Add the initial ETF fundamentals objects
					etfFundamentals =  {'SPY':spy, 'QQQ':qqq, 'IWM':iwm}
					for key, value in etfFundamentals.items():
						updatedTickers.append(key)
						# Queue fundamentals & save to file
						await self.queueObject('fundamentals', value)
						await self.writeDataToFile(value, 'fundamentals', f'{key}.US')

			# Do a full update of all stocks with splits or dividends
			if len(self.tickersToUpdate) > 0:
				self.logger.info(f'Do full update of {len(self.tickersToUpdate)} stocks...')
				for idx, stock in enumerate(self.tickersToUpdate):
					if parseBoolean(os.environ.get('eod_add_new_ticker')) == False and self.checkKnownTicker(stock['symbol'], stock['exchange']) == False:
						continue
					self.logger.info(f'Update stock {stock} - {idx+1}/{len(self.tickersToUpdate)}')
					await self.fullUpdate(stock['symbol'], stock['exchange'])
					updatedTickers.append(stock['symbol'])

			if parseBoolean(os.environ.get('eod_update_top_stocks')):
				self.logger.info(f'Start updating top US stocks')
				for ticker in TOP_US_STOCKS:
					await self.fullUpdate(ticker, 'US')
			
			if parseBoolean(os.environ.get('eod_update_top_etfs')):
				self.logger.info(f'Start updating top US ETFs')
				for ticker in TOP_US_ETFS:
					await self.fullUpdate(ticker, 'US')

			# Use the rest the available API calls to do more updates
			if parseBoolean(os.environ.get('eod_update_oldest')):
				# Get current tickers from backend
				await self.updateTickers()
				oldestTickers = self.allTickers.sort_values(by='last_update', ascending=True)
				for idx, row in oldestTickers.iterrows():
					stock = row.to_dict()
					# Skip if already updated or delisted
					if stock['code'] in updatedTickers or stock['is_delisted'] == True:
						continue

					# [AllSecurityTickersSecurities(id=42, code='AAPL', exchange=AllSecurityTickersSecuritiesExchange(code='NASDAQ', virtual_exchange='US'))]
					await self.fullUpdate(stock['code'], stock['virtual_exchange'])
					updatedTickers.append(stock['code'])

					# Check if enough API calls left for the next update
					await self.eodAsyncClient.getUserData()
					self.logger.debug(f'API calls {self.eodAsyncClient.userData.apiRequests}/{self.eodAsyncClient.userData.dailyRateLimit}')
					if self.eodAsyncClient.userData.dailyRateLimit - self.eodAsyncClient.userData.apiRequests < self.eodApiLimitReserve:
						break

			self.logger.info(f'Run finished')

		except Exception as e:
			self.logger.error(f'restGetter() failed, run aborted!')
			self.logger.error(e)
		finally:
			# All neccessary updates completed
			if self.firstRun:
				self.firstRun = False
			self.tickersToUpdate = []


	async def apiWriter(self):
		"""This function receives the data from the threading queue and performs uniforming and uploading to backend
		"""
		self.logger.debug(f'apiWriter() start')
		while not self.cancelled:
			try:
				qobj = await self.dequeueObject()

				if qobj.type == 'quotes':
					try:
						quotesDf = pd.DataFrame(qobj.data['quotes'])
						if len(quotesDf) == 0:
							continue
						# Take volume from split adjusted quotes
						if 'volume' in quotesDf.columns.to_list():
							del quotesDf['volume']
						df = pd.DataFrame(qobj.data['splitAdjusted'])
						df.rename(columns={
							'open':'split_adjusted_open',
							'high': 'split_adjusted_high',
							'low': 'split_adjusted_low',
							'close': 'split_adjusted_close'
						}, inplace=True)

						df = pd.merge(df, quotesDf, on='date', how='outer')
						df.replace({float('nan'): None}, inplace=True)

						# Add all together
						data = {**qobj.context, **{'quotes':df.to_dict('records')}}
						result = await self.client.update_security_quotes(data)
						self.logger.debug(f'apiWriter result for quotes: {result}')
					except Exception as e:
						self.logger.warning(f'Unable to parse quotes for {qobj.context}')
				
				elif qobj.type == 'dividends':
					data = qobj.context	# contains code and exchange_code
					if len(qobj.data) > 0:
						data['dividends'] = []
						for d in qobj.data:
							try:
								# {"date": "2012-08-09","declarationDate": "2012-07-24","recordDate": "2012-08-13","paymentDate": "2012-08-16","period": "Quarterly", "value": 0.0946,"unadjustedValue": 2.6488,"currency": "USD"}
								d['declaration_date'] = d['declarationDate']
								d['record_date'] = d['recordDate']
								d['payment_date'] = d['paymentDate']
								d['period'] = parseDividendPeriod(d['period'])
								del d['declarationDate']
								del d['recordDate']
								del d['paymentDate']
								del d['unadjustedValue']
								del d['currency']
								data['dividends'].append(d)
							except Exception as e:
								self.logger.warning(f'Unable to parse split {d} for {qobj.context}')

						result = await self.client.update_dividends(data)
						self.logger.debug(f'apiWriter result for quotes: {result}')
				
				elif qobj.type == 'splits':
					data = qobj.context	# contains code and exchange_code
					if len(qobj.data) > 0:
						data['splits'] = []
						for s in qobj.data:
							try:
								newDecimal, oldDecimal = parseSplit(s['split'])
								s['old'] = oldDecimal
								s['new'] = newDecimal
								del s['split']
								data['splits'].append(s)
							except Exception as e:
								self.logger.warning(f'Unable to parse split {s} for {qobj.context}')

						result = await self.client.update_splits(data)
						self.logger.debug(f'apiWriter result for quotes: {result}')
				
				elif qobj.type == 'exchange-tickers':
					if parseBoolean(os.environ.get('eod_add_new_ticker')) == True:
						exSymbolsDf = pd.read_json(StringIO(qobj.data))
						self.logger.info(f'received {len(exSymbolsDf)} tickers')
						# {'code': 'ACCA', 'name': 'Acacia Diversified Holdings Inc', 'country_alpha3': 'USA', 'exchange_code': 'PINK', 'currency_iso_code': 'USD', 'type': 'Stock', 'isin': 'US00389L1044'}
						for index, row in exSymbolsDf.iterrows():
							try:
								# {	'code', 'name', 'exchange_code', 'country_alpha3', 'currency_iso_code', 'type', 'isin', 'is_delisted' }
								sec = row.to_dict()
								sec['last_update'] = datetime.now().isoformat()
								# Force unknown (mostly delisted stocks) on general US market
								if sec['exchange_code'] == None or sec['exchange_code'] == '':
									sec['exchange_code'] = 'US'
								if self.checkKnownTicker(sec['code'], sec['exchange_code'], sec['is_delisted']):
									continue

								self.logger.debug(f'Update ticker {sec["code"]}.{sec["exchange_code"]}')
								result = await self.client.update_security(sec)
								if result.update_security.success != True:
									self.logger.warning(f'Ticker update not successful for {sec}')
									self.logger.warning(result.update_security.error)
							except Exception as e:
								self.logger.error(f'Unable to update security: {sec}')
								self.logger.error(e)
						# Get current tickers from backend
						await self.updateTickers()
					else:
						self.logger.warning(f'Ignore exchange tickers environment variable eod_add_new_ticker=false')

				elif qobj.type == 'bulk-quotes':
					bulkQuotes = qobj.data
					self.logger.info(f'Update {len(bulkQuotes)} end of day quotes...')
					for quote in bulkQuotes:
						try:
							if self.checkKnownTicker(quote['code'], quote['exchange_short_name']):
								result = await self.client.update_security_quotes({
									'code': quote['code'],
									'exchange_code': quote['exchange_short_name'],
									'quotes': [{
										'date': quote['date'],
										'open': quote['open'],
										'high': quote['high'],
										'low': quote['low'],
										'close': quote['close'],
										'split_adjusted_open': quote['open'],
										'split_adjusted_high': quote['high'],
										'split_adjusted_low': quote['low'],
										'split_adjusted_close': quote['close'],
										'adjusted_close': quote['adjusted_close'],
										'volume': quote['volume'],
									}]
								})
								if result.update_security_quotes.success != True:
									self.logger.warning(f'Bulk update not successful for {quote}')
									self.logger.warning(result.update_security_quotes.error)
						except Exception as e:
							self.logger.error(f'Error processing bulk quotes for {quote}')

				elif qobj.type == 'bulk-splits':
					splits = qobj.data
					self.logger.info(f'Update {len(splits)} splits...')
					for split in splits:
						try:
							if self.checkKnownTicker(split['code'], split['exchange']):
								newDecimal, oldDecimal = parseSplit(split['split'])
								
								result = await self.client.update_splits({
									'code': split['code'],
									'exchange_code': split['exchange'],
									'splits': [{
										'date': split['date'],
										'new': newDecimal,
										'old': oldDecimal
									}]
								})
								if result.update_splits.success != True:
									self.logger.warning(f'Split update not successful for {split}')
									self.logger.warning(result.update_splits.error)
						except Exception as e:
							self.logger.error(e)

				elif qobj.type == 'bulk-dividends':
					dividends = qobj.data
					self.logger.info(f'Update {len(dividends)} dividends...')
					for dividend in dividends:
						try:
							if self.checkKnownTicker(dividend['code'], dividend['exchange']):
								result = await self.client.update_dividends({
									'code': dividend['code'],
									'exchange_code': dividend['exchange'],
									'dividends': [{
										'date': dividend['date'],
										'declaration_date': dividend['declarationDate'],
										'record_date': dividend['recordDate'],
										'payment_date': dividend['paymentDate'],
										'period': parseDividendPeriod(dividend['period']),
										'adjusted_value': Decimal(dividend['dividend']),
										'value': Decimal(dividend['dividend']),
									}]
								})
								if result.update_dividends.success != True:
									self.logger.warning(f'Dividend update not successful for {dividend}')
									self.logger.warning(result.update_dividends.error)
						except Exception as e:
							self.logger.error(e)

				elif qobj.type == 'fundamentals':
					data = qobj.data
					secData = qobj.context		# already contains logo_base64 and logo_url

					# Extract other information from JSON
					secData['code'] = getJsonPathData(data, '$.General.Code', None)
					secData['type'] = getJsonPathData(data, '$.General.Type', None).replace('Common ', '')
					secData['name'] = getJsonPathData(data, '$.General.Name', None)
					secData['exchange_code'] = getJsonPathData(data, '$.General.Exchange', None)
					# Some fundamentals have no exchange_code -> skip
					if secData['exchange_code'] == None or secData['exchange_code'] == '':
						self.logger.warning(f'Skip fundamental update for ticker {secData["code"]}, no exchange_code given!')
						continue

					# Only update normal Stocks and ETFs
					if secData['type'] != 'Stock' and secData['type'] != 'ETF':
						self.logger.warning(f'Unknown security: {secData["code"]}.{secData["exchange_code"]} of type {secData["type"]}, ignore...')
						continue

					# Check if new tickers should be added
					if parseBoolean(os.environ.get('eod_add_new_ticker')) == False and self.checkKnownTicker(secData['code'], secData['exchange_code']) == False:
						self.logger.warning(f'Ignore ticker {secData["code"]}.{secData["exchange_code"]}, environment variable eod_add_new_ticker=false')
						continue

					secData['currency_iso_code'] = getJsonPathData(data, '$.General.CurrencyCode', None)
					secData['country_alpha3'] = getJsonPathData(data, '$.General.CountryName', None)

					secData['last_update'] = datetime.now().isoformat()

					secData['figi'] = getJsonPathData(data, '$.General.OpenFigi', None)
					secData['isin'] = getJsonPathData(data, '$.General.ISIN', None)
					secData['lei'] = getJsonPathData(data, '$.General.LEI', None)
					secData['cusip'] = getJsonPathData(data, '$.General.CUSIP', None)
					secData['cik'] = getJsonPathData(data, '$.General.CIK', None)

					secData['ipo_date'] = getJsonPathData(data, '$.General.IPODate', None)
					secData['is_delisted'] = getJsonPathData(data, '$.General.IsDelisted', False)
					secData['description'] = getJsonPathData(data, '$.General.Description', None)
					secData['marketcap'] = getJsonPathData(data, '$.Highlights.MarketCapitalization', None)
					secData['beta'] = getJsonPathData(data, '$.Technicals.Beta', None)
					secData['shares_outstanding'] = getJsonPathData(data, '$.SharesStats.SharesOutstanding', None)
					secData['shares_float'] = getJsonPathData(data, '$.SharesStats.SharesFloat', None)
					secData['shares_short'] = getJsonPathData(data, '$.Technicals.SharesShort', None)
					secData['short_ratio'] = getJsonPathData(data, '$.Technicals.ShortRatio', None)
					#secData['short_percent_outstanding'] = getJsonPathData(data, '$.', None)
					#secData['short_percent_float'] = getJsonPathData(data, '$.', None)
					secData['url'] = getJsonPathData(data, '$.General.WebURL', None)
					secData['sector'] = getJsonPathData(data, '$.General.Sector', None)
					secData['industry'] = getJsonPathData(data, '$.General.Industry', None)
					# TODO: Parse GICS Code
					secData['ebitda'] = getJsonPathData(data, '$.Highlights.EBITDA', None)
					secData['pe_ratio'] = getJsonPathData(data, '$.Highlights.PERatio', None)
					secData['wallstreet_target_price'] = getJsonPathData(data, '$.Highlights.WallStreetTargetPrice', None)
					secData['book_value'] = getJsonPathData(data, '$.Highlights.BookValue', None)
					secData['dividend_share'] = getJsonPathData(data, '$.Highlights.DividendShare', None)
					secData['dividend_yield'] = getJsonPathData(data, '$.Highlights.DividendYield', None)
					secData['earnings_share'] = getJsonPathData(data, '$.Highlights.EarningsShare', None)
					secData['eps_estimate_current_year'] = getJsonPathData(data, '$.Highlights.EPSEstimateCurrentYear', None)
					secData['eps_estimate_next_year'] = getJsonPathData(data, '$.Highlights.EPSEstimateNextYear', None)
					secData['eps_estimate_next_quarter'] = getJsonPathData(data, '$.Highlights.EPSEstimateNextQuarter', None)
					secData['eps_estimate_current_quarter'] = getJsonPathData(data, '$.Highlights.EPSEstimateCurrentQuarter', None)
					secData['most_recent_quarter'] = checkDateString(getJsonPathData(data, '$.Highlights.MostRecentQuarter', None))
					secData['profit_margin'] = getJsonPathData(data, '$.Highlights.ProfitMargin', None)
					secData['operating_margin_ttm'] = getJsonPathData(data, '$.Highlights.OperatingMarginTTM', None)
					secData['return_on_assets_ttm'] = getJsonPathData(data, '$.Highlights.ReturnOnAssetsTTM', None)
					secData['return_on_equity_ttm'] = getJsonPathData(data, '$.Highlights.ReturnOnEquityTTM', None)
					secData['revenue_ttm'] = getJsonPathData(data, '$.Highlights.RevenueTTM', None)
					secData['revenue_per_share_ttm'] = getJsonPathData(data, '$.Highlights.RevenuePerShareTTM', None)
					secData['quarterly_revenue_growth_yoy'] = getJsonPathData(data, '$.Highlights.QuarterlyRevenueGrowthYOY', None)
					secData['gross_profit_ttm'] = getJsonPathData(data, '$.Highlights.GrossProfitTTM', None)
					secData['diluted_eps_ttm'] = getJsonPathData(data, '$.Highlights.DilutedEpsTTM', None)
					secData['quarterly_earnings_growth_yoy'] = getJsonPathData(data, '$.Highlights.QuarterlyEarningsGrowthYOY', None)
					secData['forward_pe'] = getJsonPathData(data, '$.Valuation.ForwardPE', None)
					secData['price_sales_ttm'] = getJsonPathData(data, '$.Valuation.PriceSalesTTM', None)
					secData['price_book_mrq'] = getJsonPathData(data, '$.Valuation.PriceBookMRQ', None)
					secData['enterprise_value'] = getJsonPathData(data, '$.Valuation.EnterpriseValue', None)
					secData['enterprise_value_revenue'] = getJsonPathData(data, '$.Valuation.EnterpriseValueRevenue', None)
					secData['enterprise_value_ebitda'] = getJsonPathData(data, '$.Valuation.EnterpriseValueEbitda', None)

					result = await self.client.update_security(secData)
					if result.update_security.success != True:
						self.logger.warning(f'Ticker update not successful for {secData["code"]}.{secData["exchange_code"]}')
						self.logger.warning(result.update_security.error)

					# Outstanding shares
					try:
						annualShares = getJsonPathData(data, '$.outstandingShares.annual.*', [])
						if isinstance(annualShares, dict):
							annualShares = [annualShares]
						quarterlyShares = getJsonPathData(data, '$.outstandingShares.quarterly.*', [])
						if isinstance(quarterlyShares, dict):
							quarterlyShares = [quarterlyShares]
						outstandingShares = [ {'date':s['dateFormatted'], 'outstanding_shares':int(s['shares'])} for s in annualShares + quarterlyShares]
						if len(outstandingShares) > 0:
							result = await self.client.update_outstanding_shares({'code': secData['code'], 'exchange_code': secData['exchange_code'], 'outstanding_shares': outstandingShares})
							if result.update_outstanding_shares.success != True:
								self.logger.warning(f'Outstanding shares update not successful for {secData["code"]}.{secData["exchange_code"]}')
								self.logger.warning(result.update_outstanding_shares.error)
					except Exception as e:
						self.logger.error(f'Unable to process outstanding shares for {secData["code"]}.{secData["exchange_code"]}')
						self.logger.error(e)

				else:
					self.logger.warning(f'Unknown queue object type {qobj.type}')

			except Exception as e:
				self.logger.error(f'apiWriter() failed!')
				self.logger.error(e)


	async def run(self):
		"""Always running function to manage and schedule update tasks
		"""
		self.logger.debug(f'run() started')

		# Start both tasks for first run
		consumerTask = asyncio.create_task(self.apiWriter())
		producerTask = asyncio.create_task(self.restGetter())

		self.logger.debug(f'all tasks started')

		# Wait for the initial run to finish
		await producerTask

		try:
			while not self.cancelled:
				# Calculate day and time of next run and wait
				tomorrow = datetime.now() + timedelta(days=1)
				nextRun = datetime(tomorrow.year, tomorrow.month, tomorrow.day, hour=2)
				self.logger.info(f'next run at {nextRun.isoformat()}')
				if not await pause.pauseUntil(nextRun):
					# Exit if sleep was cancelled
					break

				# Pause finished, start EOD REST producer task again
				producerTask = asyncio.create_task(self.restGetter())
				await producerTask

		except Exception as e:
			self.logger.error(e)

		producerTask.cancel()
		consumerTask.cancel()

		self.logger.debug(f'all tasks stopped!')