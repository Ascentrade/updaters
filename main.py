import os
import httpx
import asyncio

# For .env file
from dotenv import load_dotenv
load_dotenv()

from log_config import getNewLogger

# Updaters
import updaters

# Backend Client
from ascentrade_client import AscentradeClient


async def main():
	logger = getNewLogger('main')
	logger.info(f'Updater main() started')

	# Check data folder
	p = os.environ.get('data_folder')
	if os.path.exists(p) == False:
		logger.info(f'Create output data folter {p}')
		os.makedirs(p)

	configuredUpdaters = []

	try:
		# Try to find the auth token
		TOKEN_PATH = os.environ.get('token_path')
		authToken:str = None
		if os.path.exists(TOKEN_PATH):
			with open(TOKEN_PATH, 'r') as f:
				authToken = f.readline()

		if not authToken:
			raise Exception('No auth token found!')

		logger.info(f'found auth token {authToken[:4]}...{authToken[-4:]}')

		# Try connection to backend
		client = AscentradeClient(
			os.environ.get('graphql_host'),
			http_client=httpx.AsyncClient(timeout=60.0),
			headers={'x-auth-token':authToken}
		)

		result = await client.ping()
		if result.ping == 'pong':
			logger.info('successfully connected to backend')

		# EOD updater
		eodApiKey = os.environ.get('eod_api_key')
		if eodApiKey != None and len(eodApiKey) > 0:
			eodUpdater = updaters.EODUpdater(eodApiKey, authToken)
			configuredUpdaters.append(asyncio.create_task(eodUpdater.run()))

		# Await all configured updaters
		await asyncio.gather(*configuredUpdaters)
		logger.info(f'All updater tasks stopped')

	except (httpx.ConnectError, httpx.ReadTimeout):
		logger.error('No connection to backend client, exit!')
	except Exception as e:
		logger.error(e)
	finally:
		logger.info(f'Updater main() end')


if __name__ == '__main__':
	asyncio.run(main())