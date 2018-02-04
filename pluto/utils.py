# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Settings and Utilities """

import csv
import datetime
import io
import pandas as pd
from decimal import Decimal, getcontext, InvalidOperation

# DataFrame to CSV parameters
STAGE_FORMAT = {'index': False, 'quoting': csv.QUOTE_ALL, \
'float_format': '%.15g', 'encoding': 'utf-8'}


class Date(object):
	"""
	Date tools. Default current date is today.

	:variable date: Current date. `datetime.date`.
	:variable last_bday: Most recent business day before current date. `datetime.date`.
	:variable last_week_bdays: Business days within 7 days before current date. `ndarray`.
	"""

	def __init__(self, date=datetime.date.today()):
		self.date = date
		if self.date >= datetime.date.today():
			self.date_elapsed = datetime.date.today() - datetime.timedelta(days=1)
		else:
			self.date_elapsed = self.date

		self.last_bday = pd.date_range(end=self.date_elapsed, periods=7, freq='B')[-1].date()
		self.last_week_bdays = pd.date_range(start=self.date_elapsed + pd.DateOffset(weeks=-1), end=self.date_elapsed, freq='B').date

	@property
	def date(self):
		return self._date

	@date.setter
	def date(self, value):
		self._date = value

	@property
	def last_bday(self):
		return self._last_bday

	@last_bday.setter
	def last_bday(self, value):
		self._last_bday = value

	@property
	def last_week_bdays(self):
		return self._last_week_bdays

	@last_week_bdays.setter
	def last_week_bdays(self, value):
		self._last_week_bdays = value

	def start_of_month_bdays(self, months):
		start = (self.date_elapsed + pd.DateOffset(months)).replace(day=1)
		return pd.date_range(start=start, end=self.date_elapsed, freq='B').date

	@classmethod
	def hk_holidays(self):
		try:
			df = pd.read_csv('data/master-data/holiday/holiday-hk.csv', parse_dates=['date'])
		except FileNotFoundError:
			return []
		else:
			holidays = df.loc[df['public'] == True, ['observed']]['observed'].values
			formatted_holidays = [datetime.datetime.strptime(str(ts), "%Y-%m-%d").date() for ts in holidays]
			return formatted_holidays


class Normalizer(object):
	"""
	Unit conversion tools.

	:variable prec: Significant digit. `int`.
	"""

	def __init__(self, prec=15):
		self.prec = prec

	@property
	def prec(self):
		return self._prec

	@prec.setter
	def prec(self, value):
		""" Set significance to 15 digits silently if if is out of bound. """
		self._prec = value if 1 <= value <= 15 else 15

	def times1000(self, data=None):
		""" Multiply value by 1 thousand. Return original value if it is not numeric. """
		try:
			getcontext().prec = self.prec
			return float(Decimal(data) * Decimal(1000))
		except (InvalidOperation, TypeError):
			return data

	def times1000000(self, data=None):
		""" Multiply value by 1 million. Return original value if it is not numeric. """
		try:
			getcontext().prec = self.prec
			return float(Decimal(data) * Decimal(1000000))
		except (InvalidOperation, TypeError):
			return data

	def times1000000000(self, data=None):
		""" Multiply value by 1 billion. Return original value if it is not numeric. """
		try:
			getcontext().prec = self.prec
			return float(Decimal(data) * Decimal(1000000000))
		except (InvalidOperation, TypeError):
			return data

	def divide100(self, data=None):
		""" Divide value by 1 hundred. Return original value if it is not numeric. """
		try:
			getcontext().prec = self.prec
			return float(Decimal(data) / Decimal(100))
		except (InvalidOperation, TypeError):
			return data

	def sqmeter2sqfeet(self, data=None):
		""" Convert per square metre to per square feet. Return original value if it is not numeric. """
		try:
			getcontext().prec = self.prec
			return float(Decimal(data) / Decimal(10.7639))
		except (InvalidOperation, TypeError):
			return data


def chunk_hash(bytestream, hash_function, chunk_size=2**20):
	"""
	`io.read()` and hash in chunks to avoid heavy RAM use.

	Return hexdigest. Raise on `TypeError`.
	"""
	try:
		fin = io.BytesIO(bytestream)
	except TypeError:
		fin.close()
		raise
	else:
		while True:
			chunk = fin.read(chunk_size)
			if not chunk:
				break
			hash_function.update(chunk)
	finally:
		fin.close()

	return hash_function.hexdigest()
