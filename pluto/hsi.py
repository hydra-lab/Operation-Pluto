# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" HSI Company pipeline orchestrators. """

import datetime
import luigi
import pandas as pd
from .utils import STAGE_FORMAT, Date, Normalizer
from .task import MkDir, Parse, ListFile, ListDirectory, ExtractHttp, MergeCsv, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickHsi(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='hsi')
	def requires(self):
		yield KickHsiDaily(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickHsiDaily(MkDir):
	workdir = luigi.Parameter(default='constituent-daily-performance')
	def requires(self):
		for pastday in SlidingWindowHsi(end=self.date):
			extract = ExtractHsi(**self.givedir, urlpath="/HSI-Net/static/revamp/contents/en/indexes/report/hsi/con{}.csv".format(pastday.strftime("_%#d%b%y")), date=pastday)
			parse = ParseHsiDaily(**self.givedir, date=pastday, upstream=extract)
			yield parse

		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)

class KickHsiHistorical(MkDir):
	workdir = luigi.Parameter(default='constituent-daily-performance')
	def requires(self):
		for pastday in SlidingWindowHsi(start=datetime.date(2017, 5, 4), end=datetime.date(2017, 10, 31)):
			parse = ParseHsiHistorical(**self.givedir, date=pastday, upstream=ListHsiHistorical(**self.givedir, date=pastday))
			yield Load(date=pastday, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractHsi(ExtractHttp):
	domain = luigi.Parameter(default="http://www.hsi.com.hk")
	def filename(self, custom=None, default='data{}.dat'.format('')):
		default = 'data{}.dat'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

class ListHsiHistorical(ListFile):
	workdir = luigi.Parameter(default='source')
	def filename(self, custom=None, default='data{}.xls'.format('')):
		default = 'data{}.xls'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

class ParseHsiDaily(Parse):
	workdir = luigi.Parameter(default='map')

	def filename(self, custom=None, default='data{}.csv'.format('')):
		default = 'data{}.csv'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

	def run(self):
		super().run()
		d100 = Normalizer().divide100
		df = pd.read_csv(self.input().path, sep='\t', parse_dates=['date'], skiprows=2, usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], names=['date', 'index', 'stock_code', 'stock_name', 'stock_name_chi', 'exchange', 'industry', 'denomination', 'close_price', 'pct_change', 'index_contribution', 'index_weight'], converters={'pct_change': d100, 'index_weight': d100}, encoding='utf-16')
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHsiHistorical(Parse):
	workdir = luigi.Parameter(default='map')

	def filename(self, custom=None, default='data{}.csv'.format('')):
		default = 'data{}.csv'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

	def run(self):
		super().run()
		d100 = Normalizer().divide100
		df = pd.read_excel(self.input().path, skiprows=7, skip_footer=9, usecols=[0, 1, 3, 4, 8, 10], converters={0: str}, names=['stock_code', 'stock_name', 'close_price', 'pct_change', 'index_contribution', 'index_weight'])
		df = df[df['stock_code'].notnull()]
		df['date'] = self.date.strftime("%Y-%m-%d")
		df['index'] = 'Hang Seng Index 恒生指數'
		df['stock_name_chi'] = ''
		df['exchange'] = 'Hong Kong 香港'
		df['industry'] = ''
		df['denomination'] = 'HKD'
		df = df[["date", "index", "stock_code", "stock_name", "stock_name_chi", "exchange", "industry", "denomination", "close_price", "pct_change", "index_contribution", "index_weight"]]
		df['stock_code'] = df.stock_code.str.rjust(4, '0') + '.HK'
		df[['pct_change', 'index_weight']] = df[['pct_change', 'index_weight']].applymap(d100)
		df.to_csv(self.output().path, **STAGE_FORMAT)

def SlidingWindowHsi(start=None, end=datetime.date.today()):
	""" Return list of `datetime.date` Hong Kong working day in recent 2 months. """
	if start is None:
		weekdays = list(Date(end).start_of_month_bdays(-1))
	else:
		weekdays = list(pd.date_range(start, end, freq='B').date)

	workingdays = [day for day in weekdays if day not in Date(end).hk_holidays()]
	return workingdays
