# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Holiday pipeline orchestrators. """

import csv
import datetime
import os
import json
import pandas as pd
import luigi
from .task import MkDir, ExtractHttp, Parse, ListDirectory, MergeCsv, Load, GenericTask
from .utils import Date

class holidayapi(luigi.Config):
	domain = luigi.Parameter(default="https://holidayapi.com/v1/holidays")
	key = luigi.Parameter(default='')
	file_format = luigi.Parameter(default='json')

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickHolidayApi(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='holidayapi')
	def requires(self):
		yield KickHolidayApiUS(**self.givedir)
		yield KickHolidayApiHK(**self.givedir)
		yield KickHolidayApiCN(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickHolidayApiUS(MkDir):
	workdir = luigi.Parameter(default='us')
	def requires(self):
		pastdays = SlidingWindowHolidayApi(end=(datetime.date.today() - datetime.timedelta(days=2)))
		for pastday in pastdays:
			qry = {'key': holidayapi().key, 'country': 'US', 'year': pastday.year, 'month': pastday.month, 'day': pastday.day, 'format': holidayapi().file_format}
			extract = ExtractHolidayApi(**self.givedir, date=pastday, urlpath="?key={key}&country={country}&year={year}&month={month}&day={day}&format={format}".format(**qry))
			parse = ParseHolidayApi(**self.givedir, date=pastday, upstream=extract)
			yield parse

		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)
		
		cwd = self.dirname.lstrip(os.sep)
		root = cwd[:cwd.index(os.sep)] if os.sep in cwd else cwd
		yield LoadHolidayApi(fullpath='{}/master-data/holiday/holiday-{}.csv'.format(root, self.workdir), upstream=merge, force=True)

class KickHolidayApiHK(MkDir):
	workdir = luigi.Parameter(default='hk')
	def requires(self):
		pastdays = SlidingWindowHolidayApi(end=(datetime.date.today() - datetime.timedelta(days=2)))
		for pastday in pastdays:
			qry = {'key': holidayapi().key, 'country': 'HK', 'year': pastday.year, 'month': pastday.month, 'day': pastday.day, 'format': holidayapi().file_format}
			extract = ExtractHolidayApi(**self.givedir, date=pastday, urlpath="?key={key}&country={country}&year={year}&month={month}&day={day}&format={format}".format(**qry))
			parse = ParseHolidayApi(**self.givedir, date=pastday, upstream=extract)
			yield parse

		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)

		cwd = self.dirname.lstrip(os.sep)
		root = cwd[:cwd.index(os.sep)] if os.sep in cwd else cwd
		yield LoadHolidayApi(fullpath='{}/master-data/holiday/holiday-{}.csv'.format(root, self.workdir), upstream=merge, force=True)

class KickHolidayApiCN(MkDir):
	workdir = luigi.Parameter(default='cn')
	def requires(self):
		pastdays = SlidingWindowHolidayApi(end=(datetime.date.today() - datetime.timedelta(days=2)))
		for pastday in pastdays:
			qry = {'key': holidayapi().key, 'country': 'CN', 'year': pastday.year, 'month': pastday.month, 'day': pastday.day, 'format': holidayapi().file_format}
			extract = ExtractHolidayApi(**self.givedir, date=pastday, urlpath="?key={key}&country={country}&year={year}&month={month}&day={day}&format={format}".format(**qry))
			parse = ParseHolidayApi(**self.givedir, date=pastday, upstream=extract)
			yield parse

		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)

		cwd = self.dirname.lstrip(os.sep)
		root = cwd[:cwd.index(os.sep)] if os.sep in cwd else cwd
		yield LoadHolidayApi(fullpath='{}/master-data/holiday/holiday-{}.csv'.format(root, self.workdir), upstream=merge, force=True)

# Implement execution of job at leaf nodes.

class ExtractHolidayApi(ExtractHttp):
	""" Get public holidays from https://holidayapi.com API """
	domain = luigi.Parameter(default=holidayapi().domain)
	def filename(self, custom=None, default='data{}.dat'.format('')):
		default = 'data{}.dat'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

class ParseHolidayApi(Parse):
	workdir = luigi.Parameter(default='map')

	def filename(self, custom=None, default='data{}.csv'.format('')):
		default = 'data{}.csv'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

	def run(self):
		super().run()
		with open(self.input().path, 'r') as fin, open(self.output().path, 'w', newline='', encoding='utf-8') as fout:
			holidays = json.loads(fin.read())['holidays']
			writer = csv.DictWriter(fout, fieldnames=['date', 'name', 'observed', 'public'], quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for row in holidays:
				writer.writerow(row)

class LoadHolidayApi(GenericTask):
	# Master data priority
	priority = 999
	def run(self):
		luigi.local_target.LocalFileSystem().copy(self.input().path, self.output().path)

def SlidingWindowHolidayApi(start=None, end=datetime.date.today()):
	""" Return list of `datetime` dates in recent 2 months by default. """
	if start is None:
		return list(Date(end).start_of_month_bdays(-1))
	else:
		return list(pd.date_range(start, end, freq='D').date)
