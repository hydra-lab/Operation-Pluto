# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" CenstatD pipeline orchestrators. """

import csv
from time import strptime
import luigi
import numpy as np
import pandas as pd
from .utils import STAGE_FORMAT, Normalizer
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickCenstatd(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='censtatd')
	def requires(self):
		yield KickCenstatdCpi(**self.givedir)
		yield KickCenstatdEer(**self.givedir)
		yield KickCenstatdTrade(**self.givedir)
		yield KickCenstatdGdp(**self.givedir)
		yield KickCenstatdHouseholdIncome(**self.givedir)
		yield KickCenstatdHouseholdType(**self.givedir)
		yield KickCenstatdLabourParticipation(**self.givedir)
		yield KickCenstatdWageIndex(**self.givedir)
		yield KickCenstatdPopulation(**self.givedir)
		yield KickCenstatdRetailSales(**self.givedir)
		yield KickCenstatdRetailSalesType(**self.givedir)
		yield KickCenstatdRollingUnemployment(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickCenstatdCpi(MkDir):
	workdir = luigi.Parameter(default='composite-cpi')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=052')
		parse = ParseCenstatdCpi(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdEer(MkDir):
	workdir = luigi.Parameter(default='effective-exchange-rates')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=125')
		parse = ParseCenstatdEer(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdTrade(MkDir):
	workdir = luigi.Parameter(default='external-trades')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=055')
		parse = ParseCenstatdTrade(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdGdp(MkDir):
	workdir = luigi.Parameter(default='gdp')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=032')
		parse = ParseCenstatdGdp(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# FIXME : Get rid of date parameter and still escape luigi's instance caching
class KickCenstatdHouseholdIncome(MkDir):
	workdir = luigi.Parameter(default='household-sizes-and-incomes')
	def requires(self):
		periods = list(pd.bdate_range(end=self.date, periods=3, freq='QS').date)
		for year, quarter, month in [(d.year, (d.month + 2) // 3, d.month) for d in periods]:
			pastday = pd.to_datetime('{}-{}-1'.format(year, month)).date()
			extract = ExtractCenstatd(**self.givedir, urlpath='/fd.jsp?file=D5250036E{}QQ0{}E.xls&product_id=D5250036&lang=1'.format(year, quarter), date=pastday)
			parse = ParseCenstatdHouseholdIncome(**self.givedir, upstream=extract, date=pastday)
			yield Load(**self.givedir, upstream=parse, date=pastday)

class KickCenstatdHouseholdType(MkDir):
	workdir = luigi.Parameter(default='household-types')
	def requires(self):
		periods = list(pd.bdate_range(end=self.date, periods=3, freq='QS').date)
		for year, quarter, month in [(d.year, (d.month+2)//3, d.month) for d in periods]:
			pastday = pd.to_datetime('{}-{}-1'.format(year, month)).date()
			extract = ExtractCenstatd(**self.givedir, urlpath='/fd.jsp?file=D5250033E{}QQ0{}E.xls&product_id=D5250033&lang=1'.format(year, quarter), date=pastday)
			parse = ParseCenstatdHouseholdType(**self.givedir, upstream=extract, date=pastday)
			yield Load(**self.givedir, upstream=parse, date=pastday)

class KickCenstatdLabourParticipation(MkDir):
	workdir = luigi.Parameter(default='labour-participation')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=008')
		parse = ParseCenstatdLabourParticipation(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdWageIndex(MkDir):
	workdir = luigi.Parameter(default='non-managerial-wage-index')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=019')
		parse = ParseCenstatdWageIndex(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdPopulation(MkDir):
	workdir = luigi.Parameter(default='population')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=001')
		parse = ParseCenstatdPopulation(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdRetailSales(MkDir):
	workdir = luigi.Parameter(default='retail-sales')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=089')
		parse = ParseCenstatdRetailSales(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickCenstatdRetailSalesType(MkDir):
	workdir = luigi.Parameter(default='retail-sales-type')
	def requires(self):
		periods = list(pd.bdate_range(end=self.date, periods=3, freq='MS').date)
		for year, month in [(d.year, d.month) for d in periods]:
			pastday = pd.to_datetime('{}-{}-1'.format(year, month)).date()
			extract = ExtractCenstatd(**self.givedir, urlpath='/fd.jsp?file=D5600089B{}MM{}B.xls&product_id=D5600089&lang=1'.format(year, str(month).rjust(2, '0')), date=pastday)
			parse = ParseCenstatdRetailSalesType(**self.givedir, upstream=extract, date=pastday)
			yield Load(**self.givedir, upstream=parse, date=pastday)

class KickCenstatdRollingUnemployment(MkDir):
	workdir = luigi.Parameter(default='rolling-unemployment')
	def requires(self):
		extract = ExtractCenstatd(**self.givedir, urlpath='/showtablecsv.jsp?TableID=006')
		parse = ParseCenstatdRollingUnemployment(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractCenstatd(ExtractHttp):
	domain = luigi.Parameter(default="http://www.censtatd.gov.hk")

class ParseCenstatdCpi(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, skiprows=3, skipfooter=9, usecols=[0, 1, 2, 4, 6, 8], names=["year", "month", "composite_cpi", "cpi_a", "cpi_b", "cpi_c"], na_values=['N.A.'], converters={'year': int, 'month': str}, engine='python')
		df = df[df['month'].notnull()]
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdEer(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, skiprows=3, skipfooter=3, usecols=[0, 1, 2, 3, 4], names=["year", "month", "trade_weighted", "import_weighted", "export_weighted"], converters={'year': int, 'month': str}, engine='python')
		df = df[df['month'].notnull()]
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdTrade(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1M = Normalizer().times1000000
		df = pd.read_csv(self.input().path, skiprows=3, skipfooter=11, usecols=[0, 1, 2, 4, 6], names=["year", "month", "import", "domestic_export", "reexport"], na_values=['#'], converters={'year': int, 'month': str, 'import': x1M, 'domestic_export': x1M, 'reexport': x1M}, engine='python')
		df = df[df['month'].notnull()]
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdGdp(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1M = Normalizer().times1000000
		df = pd.read_csv(self.input().path, skiprows=2, skipfooter=11, usecols=[0, 1, 2, 4, 6, 8, 10], names=['year', 'quarter', 'gdp', 'private_consumption', 'government_consumption', 'gfcf', 'change_in_inventories'], converters={'gdp': x1M, 'private_consumption': x1M, 'government_consumption': x1M, 'gfcf': x1M, 'change_in_inventories': x1M}, engine='python')
		df = df[df['quarter'].notnull()]
		df['year'].fillna(method="ffill", inplace=True)
		df['quarter'] = df['quarter'].str.replace(r' [rp]', r'')

		df1 = []
		df1 = df.iloc[0:np.where(df['year'] == 'Year')[0][0]]
		df2 = []
		df2 = df.iloc[np.where(df['year'] == 'Year')[0][0] + 1:len(df.index)]

		df = df1.merge(df2, how='left', on=['year', 'quarter'])
		df.columns = ['year', 'quarter', 'gdp', 'private_consumption', 'government_consumption', 'gfcf', 'change_in_inventories', 'export_of_goods', 'export_of_services', 'import_of_goods', 'import_of_services', 'a']
		df = df[['year', 'quarter', 'gdp', 'private_consumption', 'government_consumption', 'gfcf', 'change_in_inventories', 'export_of_goods', 'export_of_services', 'import_of_goods', 'import_of_services']]
		df['quarter'] = df.quarter.str.replace('Q', '')
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdHouseholdIncome(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1K = Normalizer().times1000
		names = ['year', 'quarter']
		datafields = ['1p_4000', '1p_4000_5999', '1p_6000_7999', '1p_8000_9999', '1p_10000_14999', '1p_15000_19999', '1p_20000_24999', '1p_25000_29999', '1p_30000_34999', '1p_35000_39999', '1p_40000_44999', '1p_45000_49999', '1p_50000_59999', '1p_60000_79999', '1p_over_79999', '2p_4000', '2p_4000_5999', '2p_6000_7999', '2p_8000_9999', '2p_10000_14999', '2p_15000_19999', '2p_20000_24999', '2p_25000_29999', '2p_30000_34999', '2p_35000_39999', '2p_40000_44999', '2p_45000_49999', '2p_50000_59999', '2p_60000_79999', '2p_over_79999', '3p_4000', '3p_4000_5999', '3p_6000_7999', '3p_8000_9999', '3p_10000_14999', '3p_15000_19999', '3p_20000_24999', '3p_25000_29999', '3p_30000_34999', '3p_35000_39999', '3p_40000_44999', '3p_45000_49999', '3p_50000_59999', '3p_60000_79999', '3p_80000_99999', '3p_over_100000', '4p_4000', '4p_4000_5999', '4p_6000_7999', '4p_8000_9999', '4p_10000_14999', '4p_15000_19999', '4p_20000_24999', '4p_25000_29999', '4p_30000_34999', '4p_35000_39999', '4p_40000_44999', '4p_45000_49999', '4p_50000_59999', '4p_60000_79999', '4p_80000_99999', '4p_over_100000', '5p_8000', '5p_8000_9999', '5p_10000_14999', '5p_15000_19999', '5p_20000_24999', '5p_25000_29999', '5p_30000_34999', '5p_35000_39999', '5p_40000_44999', '5p_45000_49999', '5p_50000_59999', '5p_60000_79999', '5p_80000_99999', '5p_over_100000', 'over_5p_8000', 'over_5p_8000_9999', 'over_5p_10000_14999', 'over_5p_15000_19999', 'over_5p_20000_24999', 'over_5p_25000_29999', 'over_5p_30000_34999', 'over_5p_35000_39999', 'over_5p_40000_44999', 'over_5p_45000_49999', 'over_5p_50000_59999', 'over_5p_60000_79999', 'over_5p_80000_99999', 'over_5p_over_100000']
		names.extend(datafields)
		df = pd.read_excel(self.input().path, sheet_name="Households", skiprows=5, skip_footer=0, usecols=[col for col in range(97) if col not in [17, 33, 51, 68, 83]], names=names)
		df = df.replace({'‡': ''}, regex=True)
		df = pd.melt(df, id_vars=['year', 'quarter'], var_name='household', value_name='amount', col_level=0)
		df_split = df['household'].apply(lambda x: pd.Series(x.split('p_')))
		df_split.columns = ['household_size', 'household_income']
		df.drop('household', 1, inplace=True)
		df = df.join(df_split)
		df.replace('over_', 'over ', regex=True, inplace=True)
		df.replace('_', ' - ', regex=True, inplace=True)
		df = df[['year', 'quarter', 'household_size', 'household_income', 'amount']]
		df[['amount']] = df[['amount']].applymap(x1K)
		df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')

class ParseCenstatdHouseholdType(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1K = Normalizer().times1000
		df = pd.read_excel(self.input().path, skiprows=4, skipfooter=0, usecols=[0, 1, 2, 3, 4, 5], names=["year", "quarter", "prh", "hos", "private", "temporary"], converters={'year': int, 'month': str, 2: x1K, 3: x1K, 4: x1K, 5: x1K})
		df['year'] = df['year'].astype('int')
		df['quarter'] = df['quarter'].astype('int')
		df.to_csv(self.output().path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')

class ParseCenstatdLabourParticipation(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1K = Normalizer().times1000
		d100 = Normalizer().divide100
		df = pd.read_csv(self.input().path, skiprows=3, skipfooter=6, usecols=[0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], names=['period', '15_24_number', '15_24_rate', '25_44_number', '25_44_rate', '45_64_number', '45_64_rate', 'over_64_number', 'over_64_rate', 'over_14_number', 'over_14_rate'], converters={'15_24_number': x1K, '15_24_rate': d100, '25_44_number': x1K, '25_44_rate': d100, '45_64_number': x1K, '45_64_rate': d100, 'over_64_number': x1K, 'over_64_rate': d100, 'over_14_number': x1K, 'over_14_rate': d100}, engine='python')
		df = df.replace({r'[ ]*[#@\*]+': ''}, regex=True)
		df = df.replace({r'N.A[.]*': ''}, regex=True)
		df = df[df.period.str.len() > 4]
		df['year'] = df.period.str[-4:]
		df['month'] = df.period.str[-7:-5].str.strip()
		df = df[['year', 'month', '15_24_number', '15_24_rate', '25_44_number', '25_44_rate', '45_64_number', '45_64_rate', 'over_64_number', 'over_64_rate', 'over_14_number', 'over_14_rate']]
		df = df.replace({r'[ ]*': ''}, regex=True)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdWageIndex(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, skiprows=3, skipfooter=8, usecols=[0, 1, 10], names=['year', 'month', 'index_all'], engine='python')
		df = df[df['index_all'].notnull()]
		df = df.iloc[2:, :]
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdPopulation(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1K = Normalizer().times1000
		df = pd.read_csv(self.input().path, skiprows=4, skipfooter=6, usecols=[0, 1, 2, 4], names=["year", "month", "male_population", "female_population"], converters={'male_population': x1K, 'female_population': x1K}, na_values=['N.A.'], engine='python')
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df.month.str.replace(r'Mid-year[ #]*', '6')
		df['month'] = df.month.str.replace(r'Year-end[ #]*', '12')
		df = df[df['year'] != 'Year']
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdRetailSales(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1M = Normalizer().times1000000
		df = pd.read_csv(self.input().path, skiprows=4, skipfooter=8, usecols=[0, 1, 2, 5], names=["year", "month", "sales_value", "sales_volume"], na_values=['N.A.'], converters={'year': int, 'sales_value': x1M}, engine='python')
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df.month.str.replace(r'[ #]*', '')
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdRetailSalesType(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1M = Normalizer().times1000000
		names = ['year', 'month']
		datafields = ['all', 'food', 'supermarket', 'fuel', 'apparel', 'consumer_durables', 'car', 'electricals', 'furniture', 'department_store', 'valuables', 'other_consumer_goods', 'medicine_and_cosmetics']
		names.extend(datafields)
		df = pd.read_excel(self.input().path, sheet_name="E089-1", skiprows=3, skip_footer=16, converters={0: int, 1: str}, usecols=[0, 1, 2, 3, 9, 10, 11, 14, 15, 16, 17, 19, 20, 21, 25], names=names)
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df.month.str.strip('#')
		df[datafields] = df[datafields].applymap(x1M)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseCenstatdRollingUnemployment(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		x1K = Normalizer().times1000
		d100 = Normalizer().divide100
		df = pd.read_csv(self.input().path, skiprows=3, names=['period', 'labour_force', 'seasonally_adjusted_unemployment_rate', 'underemployed'], skipfooter=15, usecols=[0, 2, 5, 7], na_values=['-', 'N.A.'], converters={'labour_force': str, 'seasonally_adjusted_unemployment_rate': d100, 'underemployed': x1K}, engine='python')
		df = df.replace({r'[ ]*[#@\*]+': ''}, regex=True)
		df = df.replace({r'N.A[.]*': ''}, regex=True)
		df['labour_force'] = df.labour_force.str.strip(' ')
		df['labour_force'] = df['labour_force'].astype(float) * 1000
		df = df[df.period.str.len() > 4]
		df['year'] = df.period.str[-4:]
		df['month'] = df.period.str[-7:-5].str.strip()
		df = df[['year', 'month', 'labour_force', 'seasonally_adjusted_unemployment_rate', 'underemployed']]
		df.to_csv(self.output().path, **STAGE_FORMAT)
