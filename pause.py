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

import time
from asyncio import sleep
from datetime import datetime, timezone


async def pauseUntil(dt:datetime, wakeupInterval=60) -> bool:
	"""Pause program execution in an async way

	Args:
		dt (datetime): datetime object how long to sleep
		wakeupInterval (int, optional): Seconds to check timestamp again. Defaults to 60.

	Returns:
		bool: True if sleep finished, otherwise False (on cancel)
	"""
	try:
		end = dt.astimezone(timezone.utc).timestamp()
		while True:
			now = time.time()
			diff = end - now
			if diff <= 0:
				break
			else:
				await sleep(wakeupInterval)
		return True
	except:
		pass
	return False