# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" HKGB pipeline orchestrators. """

import luigi
import numpy as np
import pandas as pd
from .utils import Date, STAGE_FORMAT
from .task import MkDir, Parse, ExtractHttp, ListDirectory, MergeCsv, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickHkgb(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='hkgb')
	def requires(self):
		yield KickHkgbDaily(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickHkgbDaily(MkDir):
	workdir = luigi.Parameter(default='daily-closing-reference-rates')
	def requires(self):
		extract = ExtractHkgb(**self.givedir, urlpath='/en/others/documents/DailyClosings.xls')
		parse = ParseHkgbDaily(**self.givedir, date=self.date, upstream=extract, force=True)
		yield parse
	
		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)

# Implement execution of job at leaf nodes.

class ExtractHkgb(ExtractHttp):
	domain = luigi.Parameter(default="http://www.hkgb.gov.hk")
	def filename(self, custom=None, default='data{}.dat'.format('')):
		default = 'data{}.dat'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

class ParseHkgbDaily(Parse):
	workdir = luigi.Parameter(default='map')

	def filename(self, custom=None, default='data{}.dat'.format('')):
		default = 'data{}.csv'.format(self.date.strftime("-%Y-%m"))
		return super().filename(custom=custom, default=default)
	
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="Daily GB Closings - Benchmark", skiprows=14, skipfooter=11, usecols=[0, 2, 4, 6, 8], names=['date', '3_year', '5_year', '10_year', '15_year'], parse_dates=[0])
		self.date = np.max(df.date).date()
		df.to_csv(self.output().path, **STAGE_FORMAT)
