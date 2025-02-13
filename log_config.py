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
import logging

# Static log config for uvicorn matches the internal app format
UVICON_CONFIG = {
	"version": 1,
	"disable_existing_loggers": False,
	"formatters": {
		"default": {
			"()": "uvicorn.logging.DefaultFormatter",
			"fmt": "%(asctime)s - %(levelprefix)s %(message)s",
			"use_colors": None,
		},
		"access": {
			"()": "uvicorn.logging.AccessFormatter",
			"fmt": '%(asctime)s - %(levelprefix)s %(client_addr)s - %(request_line)s - %(status_code)s',
		},
	},
	"handlers": {
		"default": {
			"formatter": "default",
			"class": "logging.StreamHandler",
			"stream": "ext://sys.stderr",
		},
		"access": {
			"formatter": "access",
			"class": "logging.StreamHandler",
			"stream": "ext://sys.stdout",
		},
	},
	"loggers": {
		"uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
		"uvicorn.error": {"level": "INFO"},
		"uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
	},
}


class CustomLogFormat(logging.Formatter):
	"""Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629"""

	grey = '\x1b[38;21m'
	blue = '\x1b[38;5;39m'
	green = '\x1b[32m'
	yellow = '\x1b[38;5;226m'
	red = '\x1b[38;5;196m'
	bold_red = '\x1b[31;1m'
	reset = '\x1b[0m'

	def __init__(self, fmt):
		super().__init__()
		self.fmt = fmt

		self.FORMATS = {
			logging.DEBUG: self.fmt.replace('^COL_START^', self.grey).replace('^COL_END^', self.reset),
			logging.INFO: self.fmt.replace('^COL_START^', self.green).replace('^COL_END^', self.reset),
			logging.WARNING: self.fmt.replace('^COL_START^', self.yellow).replace('^COL_END^', self.reset),
			logging.ERROR: self.fmt.replace('^COL_START^', self.red).replace('^COL_END^', self.reset),
			logging.CRITICAL: self.fmt.replace('^COL_START^', self.bold_red).replace('^COL_END^', self.reset),
		}

	def format(self, record):
		log_fmt = self.FORMATS.get(record.levelno)
		formatter = logging.Formatter(log_fmt)
		return formatter.format(record)


def getNewLogger(name:str) -> logging.Logger:
	"""Create a new logger with a specific name

	Args:
		name (str): Name of this logger

	Returns:
		logging.Logger: The new logger instance
	"""
	log = logging.getLogger(name)
	log.setLevel(logging._nameToLevel[os.environ.get('updater_log_level').upper()])
	stdout_handler = logging.StreamHandler()
	stdout_handler.setLevel(logging._nameToLevel[os.environ.get('updater_log_level').upper()])
	# https://docs.python.org/3/library/logging.html#logrecord-attributes
	stdout_handler.setFormatter(CustomLogFormat('%(asctime)s | ^COL_START^%(levelname)s^COL_END^	| %(name)s:	%(message)s'))
	log.addHandler(stdout_handler)
	return log