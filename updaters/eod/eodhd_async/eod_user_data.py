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

from dataclasses import dataclass

@dataclass
class EODUserData:
	"""Dataclass object to handle user data like API limits
	
	Example: {
		'name': 'username',
		'email': 'user@email.com',
		'subscriptionType': 'Annual',
		'paymentMethod': 'Card',
		'apiRequests': 101,
		'apiRequestsDate': '2020-01-01',
		'dailyRateLimit': 100000,
		'extraLimit': 0,
		'inviteToken': 'ABC123',
		'inviteTokenClicked': 0,
		'subscriptionMode': 'paid'
	}
	"""
	name:str=None
	email:str=None
	subscriptionType:str=None
	paymentMethod:str=None
	apiRequests:int=0
	apiRequestsDate:str=None
	dailyRateLimit:int=0
	extraLimit:int=0
	inviteToken:str=None
	inviteTokenClicked:int=0
	subscriptionMode:str=None