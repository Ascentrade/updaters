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
import asyncio
import pandas as pd
from datetime import date
from dotenv import load_dotenv
load_dotenv()

from eodhd_async import EODHDAsyncClient


async def main():
	eodClient = EODHDAsyncClient(os.environ.get('eod_api_key'))
	print('\n--- USER DATA ---')
	print(await eodClient.getUserData())

	print('\n--- HISTORICAL DIVIDENDS ---')
	div = await eodClient.getHistoricalDividends('AAPL.US')
	print(pd.DataFrame(div))

	print('\n--- HISTORICAL SPLITS ---')
	splits = await eodClient.getHistoricalQuotes('AAPL.US')
	print(pd.DataFrame(splits))

	print('\n--- EOD QUOTES ---')
	quotes = await eodClient.getHistoricalQuotes('NVDA.US', fromDate=date(2024, 1, 1))
	print(pd.DataFrame(quotes))

	print('\n--- EOD QUOTES SPLIT ADJUSTED ---')
	quotes = await eodClient.getSplitAdjustedQuotes('NVDA.US', fromDate=date(2024, 1, 1))
	print(pd.DataFrame(quotes))

	print('\n--- EXCHANGES ---')
	exchanges = await eodClient.getExchangesList()
	print(exchanges)

	print('\n--- EXCHANGE TICKERS ---')
	tickers = await eodClient.getExchangeSymbolList()
	print(pd.DataFrame(tickers))

	print('\n--- DELISTED EXCHANGE TICKERS ---')
	tickers = await eodClient.getExchangeSymbolList(delisted=True)
	print(pd.DataFrame(tickers))

	print('\n--- BULK QUOTES ---')
	bulkQuotes = await eodClient.getBulkQuotes()
	print(pd.DataFrame(bulkQuotes))

	print('\n--- BULK SPLITS ---')
	splits = await eodClient.getBulkSplits()
	print(splits)

	print('\n--- BULK DIVIDENDS ---')
	dividends = await eodClient.getBulkDividends()
	print(pd.DataFrame(dividends))


if __name__ == '__main__':
	asyncio.run(main())