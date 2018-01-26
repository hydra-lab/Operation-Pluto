# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" RVD pipeline orchestrators. """

import luigi
import pandas as pd
from .utils import STAGE_FORMAT, Normalizer
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickRvd(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='rvd')
	def requires(self):
		yield KickRvdDomesticPrice(**self.givedir)
		yield KickRvdDomesticRent(**self.givedir)
		yield KickRvdDomesticYield(**self.givedir)
		yield KickRvdDomesticSales(**self.givedir)
		yield KickRvdDomesticStock(**self.givedir)
		yield KickRvdDomesticVacancy(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickRvdDomesticPrice(MkDir):
	workdir = luigi.Parameter(default='private-domestic-price')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/his_data_2.xls')
		parse = ParseRvdDomesticPrice(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickRvdDomesticRent(MkDir):
	workdir = luigi.Parameter(default='private-domestic-rent')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/his_data_3.xls')
		parse = ParseRvdDomesticRent(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickRvdDomesticYield(MkDir):
	workdir = luigi.Parameter(default='private-domestic-yield')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/his_data_15.xls')
		parse = ParseRvdDomesticYield(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickRvdDomesticSales(MkDir):
	workdir = luigi.Parameter(default='private-domestic-sales')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/his_data_16.xls')
		parse = ParseRvdDomesticSales(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickRvdDomesticStock(MkDir):
	workdir = luigi.Parameter(default='private-domestic-stock')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/private_domestic.xls')
		parse = ParseRvdDomesticStock(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickRvdDomesticVacancy(MkDir):
	workdir = luigi.Parameter(default='private-domestic-vacancy')
	def requires(self):
		extract = ExtractRvd(**self.givedir, urlpath='/private_domestic.xls')
		parse = ParseRvdDomesticVacancy(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractRvd(ExtractHttp):
	domain = luigi.Parameter(default="http://www.rvd.gov.hk/doc/en/statistics")

class ParseRvdDomesticPrice(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		names = ['year', 'month']
		datafields = ['a_hk', 'a_kln', 'a_nt', 'b_hk', 'b_kln', 'b_nt', 'c_hk', 'c_kln', 'c_nt', 'd_hk', 'd_kln', 'd_nt', 'e_hk', 'e_kln', 'e_nt']
		names.extend(datafields)
		df = pd.read_excel(self.input().path, sheet_name="Monthly  按月", skiprows=9, skip_footer=8, na_values=[' ', '-'], usecols=[1, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50], names=names, converters={0: int})
		df['year'].fillna(method='ffill', inplace=True)
		df = df[df['month'].notnull()]
		df[datafields] = df[datafields].applymap(Normalizer().sqmeter2sqfeet)
		df[datafields] = df[datafields].apply(lambda x: pd.Series.round(x, 0))
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseRvdDomesticRent(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="Monthly  按月", skiprows=5, skip_footer=5, na_values=[' '], usecols=[1, 5, 8, 11, 14, 17, 20, 23, 26, 29], names=['year', 'month', 'a', 'b', 'c', 'd', 'e', 'a_b_c', 'd_e', 'all'])
		df['year'].fillna(method='ffill', inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df['month'].astype('int')
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseRvdDomesticYield(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="Monthly(Domestic) 按月(住宅)", skiprows=8, skip_footer=2, na_values=[' '], usecols=[1, 5, 8, 11, 14, 17, 20], names=['year', 'month', 'a', 'b', 'c', 'd', 'e'])
		df['year'].fillna(method='ffill', inplace=True)
		df = df[df['month'].notnull()]
		df['year'] = df['year'].astype('int')
		df['month'] = df['month'].astype('int')
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseRvdDomesticSales(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="Monthly  按月", skiprows=7, skip_footer=8, usecols=[1, 5, 9, 13], names=['year', 'month', 'number_of_transactions', 'consideration'], na_values=[' '])
		df['year'].fillna(method="ffill", inplace=True)
		df['year'] = df['year'].astype('int')
		df['month'] = df['month'].astype('int')
		df[['consideration']] = df[['consideration']].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseRvdDomesticStock(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_stock = pd.read_excel(self.input().path, sheet_name="Stock_總存量", skiprows=14, skip_footer=6, usecols=[2, 4, 6, 8, 10, 12], names=['year', 'a', 'b', 'c', 'd', 'e'])
		df_stock.to_csv(self.output().path, **STAGE_FORMAT)

class ParseRvdDomesticVacancy(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_vacancy = pd.read_excel(self.input().path, sheet_name="Vacancy_空置量", skiprows=14, skip_footer=6, usecols=[2, 5, 7, 9, 11, 13], names=['year', 'a', 'b', 'c', 'd', 'e'])
		df_vacancy.to_csv(self.output().path, **STAGE_FORMAT)
