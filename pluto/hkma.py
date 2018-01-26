# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" HKMA pipeline orchestrators. """

from time import strptime
import luigi
import pandas as pd
from .utils import STAGE_FORMAT, Normalizer
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickHkma(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='hkma')
	def requires(self):
		yield KickHkmaT0101(**self.givedir)
		yield KickHkmaT0101(**self.givedir)
		yield KickHkmaT0201(**self.givedir)
		yield KickHkmaT020201(**self.givedir)
		yield KickHkmaT020301(**self.givedir)
		yield KickHkmaT020302(**self.givedir)
		yield KickHkmaT0204(**self.givedir)
		yield KickHkmaT030301(**self.givedir)
		yield KickHkmaT030302(**self.givedir)
		yield KickHkmaT030502(**self.givedir)
		yield KickHkmaT0306(**self.givedir)
		yield KickHkmaT0307(**self.givedir)
		yield KickHkmaT0308(**self.givedir)
		yield KickHkmaT050301(**self.givedir)
		yield KickHkmaT060102(**self.givedir)
		yield KickHkmaT060103(**self.givedir)
		yield KickHkmaT060303(**self.givedir)
		yield KickHkmaT0605(**self.givedir)
		yield KickHkmaT070202(**self.givedir)
		yield KickHkmaT0803(**self.givedir)
		yield KickHkmaT090401(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickHkmaT0101(MkDir):
	workdir = luigi.Parameter(default='t0101-monetary-base')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0101.xls')
		yield extract

class KickHkmaT0201(MkDir):
	workdir = luigi.Parameter(default='t0201-currency-in-circulation')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0201.xls')
		parse = ParseHkmaT0201(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT020201(MkDir):
	workdir = luigi.Parameter(default='t020201-money-supply-fx-swap-adjusted')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T020201.xls')
		parse = ParseHkmaT020201(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT020301(MkDir):
	workdir = luigi.Parameter(default='t020301-hkd-money-supply-component')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T020301.xls')
		parse = ParseHkmaT020301(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT020302(MkDir):
	workdir = luigi.Parameter(default='t020302-fx-money-supply-component')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T020302.xls')
		parse = ParseHkmaT020302(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT0204(MkDir):
	workdir = luigi.Parameter(default='t0204-hkd-m1-component')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0204.xls')
		parse = ParseHkmaT0204(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT030301(MkDir):
	workdir = luigi.Parameter(default='t030301-hkd-deposit')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T030301.xls')
		parse = ParseHkmaT030301(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT030302(MkDir):
	workdir = luigi.Parameter(default='t030302-renmenbi-deposit')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T030302.xls')
		parse = ParseHkmaT030302(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT030502(MkDir):
	workdir = luigi.Parameter(default='t030502-econ-sector-loans-and-advances')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T030502.xls')
		yield extract

class KickHkmaT0306(MkDir):
	workdir = luigi.Parameter(default='t0306-retail-bank-asset-quality')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0306.xls')
		parse = ParseHkmaT0306(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT0307(MkDir):
	workdir = luigi.Parameter(default='t0307-residential-mortgage')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0307.xls')
		parse = ParseHkmaT0307(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT0308(MkDir):
	workdir = luigi.Parameter(default='t0308-credit-card-delinquency')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0308.xls')
		parse = ParseHkmaT0308(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT050301(MkDir):
	workdir = luigi.Parameter(default='t050301-efbn-yield')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T050301.xls')
		parse = ParseHkmaT050301(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT060102(MkDir):
	workdir = luigi.Parameter(default='t060102-reer')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T060102.xls')
		parse = ParseHkmaT060102(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT060103(MkDir):
	workdir = luigi.Parameter(default='t060103-daily-eer')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T060103.xls')
		parse = ParseHkmaT060103(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT060303(MkDir):
	workdir = luigi.Parameter(default='t060303-daily-hibor')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T060303.xls')
		parse = ParseHkmaT060303(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT0605(MkDir):
	workdir = luigi.Parameter(default='t0605-composite-interest-rate')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0605.xls')
		parse = ParseHkmaT0605(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT070202(MkDir):
	workdir = luigi.Parameter(default='t070202-daily-monetary-base-component')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T070202.xls')
		parse = ParseHkmaT070202(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT0803(MkDir):
	workdir = luigi.Parameter(default='t0803-currency-board-account')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T0803.xls')
		parse = ParseHkmaT0803(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

class KickHkmaT090401(MkDir):
	workdir = luigi.Parameter(default='t090401-government-bond-yield')
	def requires(self):
		extract = ExtractHkma(**self.givedir, urlpath='/T090401.xls')
		parse = ParseHkmaT090401(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractHkma(ExtractHttp):
	domain = luigi.Parameter(default="http://www.hkma.gov.hk/media/eng/doc/market-data-and-statistics/monthly-statistical-bulletin")

class ParseHkmaT0201(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=14, usecols=[0, 1, 4, 5], names=['year', 'month', 'commercial_bank_issues', 'government_issue'], na_values=[0], converters={0: int})
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[["commercial_bank_issues", "government_issue"]] = df[["commercial_bank_issues", "government_issue"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT020201(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_old = pd.read_excel(self.input().path, sheet_name="T2.2.1 (old series)", skiprows=12, skip_footer=5, usecols=[0, 1, 5, 7, 11, 13, 17, 19], names=["year", "month", "hkd_m1", "fx_m1", "hkd_m2", "fx_m2", "hkd_m3", "fx_m3"], na_values=['0'], converters={0: int})
		df_old['year'].fillna(method="ffill", inplace=True)
		df_old = df_old[df_old['month'].notnull()]
		df_old['month'] = df_old['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_old = df_old[(df_old['year'] < 1997) | ((df_old['year'] <= 1997) & (df_old['month'] <= 3))]
		df_new = pd.read_excel(self.input().path, sheet_name="T2.2.1 (new series)", skiprows=11, skip_footer=4, usecols=[0, 1, 5, 6, 8, 9, 11, 12], names=["year", "month", "hkd_m1", "fx_m1", "hkd_m2", "fx_m2", "hkd_m3", "fx_m3"], na_values=['0'], converters={0: int})
		df_new = df_new[df_new['month'].notnull()]
		df_new['year'].fillna(method="ffill", inplace=True)
		df_new['month'] = df_new['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_new = df_new[(df_new['year'] > 1997) | ((df_new['year'] >= 1997) & (df_new['month'] >= 4))]
		df = pd.concat([df_old, df_new])
		df[["hkd_m1", "fx_m1", "hkd_m2", "fx_m2", "hkd_m3", "fx_m3"]] = df[["hkd_m1", "fx_m1", "hkd_m2", "fx_m2", "hkd_m3", "fx_m3"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT020301(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_old = pd.read_excel(self.input().path, sheet_name="T2.3.1(old series)", skiprows=19, skip_footer=3, usecols=[0, 1, 5, 7, 11, 13, 15, 19, 21], names=["year", "month", "notes_and_coins_in_public", "demand_deposit", "saving_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"], na_values=['0'], converters={0: int})
		df_old['year'].fillna(method="ffill", inplace=True)
		df_old = df_old[df_old['month'].notnull()]
		df_old['month'] = df_old['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_old = df_old[(df_old['year'] < 1997) | ((df_old['year'] <= 1997) & (df_old['month'] <= 3))]
		df_new = pd.read_excel(self.input().path, sheet_name="T2.3.1 (new series)", skiprows=19, skip_footer=3, usecols=[0, 1, 5, 6, 8, 9, 10, 12, 13], names=["year", "month", "notes_and_coins_in_public", "demand_deposit", "saving_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"], na_values=['0'], converters={0: int})
		df_new = df_new[df_new['month'].notnull()]
		df_new['year'].fillna(method="ffill", inplace=True)
		df_new['month'] = df_new['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_new = df_new[(df_new['year'] > 1997) | ((df_new['year'] >= 1997) & (df_new['month'] >= 4))]
		df = pd.concat([df_old, df_new])
		df[["notes_and_coins_in_public", "demand_deposit", "saving_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"]] = df[["notes_and_coins_in_public", "demand_deposit", "saving_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT020302(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_old = pd.read_excel(self.input().path, sheet_name="T2.3.2 (old series)", skiprows=19, skip_footer=3, usecols=[0, 1, 5, 9, 11, 13, 17, 19], names=["year", "month", "demand_deposit", "saivng_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"], na_values=['0'], converters={0: int})
		df_old['year'].fillna(method="ffill", inplace=True)
		df_old = df_old[df_old['month'].notnull()]
		df_old['month'] = df_old['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_old = df_old[(df_old['year'] < 1997) | ((df_old['year'] <= 1997) & (df_old['month'] <= 3))]
		df_new = pd.read_excel(self.input().path, sheet_name="T2.3.2 (new series)", skiprows=19, skip_footer=3, usecols=[0, 1, 5, 7, 8, 9, 11, 12], names=["year", "month", "demand_deposit", "saivng_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"], na_values=['0'], converters={0: int})
		df_new = df_new[df_new['month'].notnull()]
		df_new['year'].fillna(method="ffill", inplace=True)
		df_new['month'] = df_new['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_new = df_new[(df_new['year'] > 1997) | ((df_new['year'] >= 1997) & (df_new['month'] >= 4))]
		df = pd.concat([df_old, df_new])
		df[["demand_deposit", "saivng_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"]] = df[["demand_deposit", "saivng_deposit", "time_deposit", "ncd_by_licensed_bank", "other_deposit", "other_ncd"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0204(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=13, usecols=[0, 1, 10, 11], names=['year', 'month', 'notes_and_coins_in_public', 'demand_deposit'], converters={0: int})
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[['notes_and_coins_in_public', 'demand_deposit']] = df[['notes_and_coins_in_public', 'demand_deposit']].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT030301(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=17, skip_footer=1, usecols=[0, 1, 4, 5, 7, 8, 10, 11], names=["year", "month", "hkd_demand", "fx_demand", "hkd_saving", "fx_saving", "hkd_time", "fx_time"], na_values=['0.0'], converters={0: int})
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[["hkd_demand", "fx_demand", "hkd_saving", "fx_saving", "hkd_time", "fx_time"]] = df[["hkd_demand", "fx_demand", "hkd_saving", "fx_saving", "hkd_time", "fx_time"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT030302(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=14, skip_footer=9, usecols=[0, 1, 4, 5], names=["year", "month", "saving", "time"], converters={0: int})
		df = df[df['month'].notnull()]
		df['month'] = df.month.str.replace('June', 'Jun')
		df['month'] = df.month.str.replace('July', 'Jul')
		df['year'].fillna(method="ffill", inplace=True)
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[["saving", "time"]] = df[["saving", "time"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0306(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_old = pd.read_excel(self.input().path, sheet_name="T3.6 (old series)", skiprows=13, skip_footer=9, usecols=[0, 1, 3, 4, 5, 6, 7], names=["year", "month", "pass", "special_mention", "substandard", "doubtful", "loss"], converters={0: int})
		df_old['year'].fillna(method="ffill", inplace=True)
		df_old['month'] = df_old['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_new = pd.read_excel(self.input().path, sheet_name="T3.6 (new series)", skiprows=11, skip_footer=9, usecols=[0, 1, 3, 4, 5, 6, 7], names=['year', 'month', 'pass', 'special_mention', 'substandard', 'doubtful', 'loss'], converters={0: int})
		df_new['year'].fillna(method='ffill', inplace=True)
		df_new['month'] = df_new['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_new = df_new[df_new['month'].notnull()]
		df = pd.concat([df_old, df_new])
		df[['pass', 'special_mention', 'substandard', 'doubtful', 'loss']] = df[['pass', 'special_mention', 'substandard', 'doubtful', 'loss']].applymap(Normalizer().divide100)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0307(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=15, skip_footer=3, usecols=[0, 1, 3, 4, 5, 9, 11, 13], names=["year", "month", "outstanding_loan", "delinquency_ratio", "rescheduled_loan_ratio", "gross_loan_made", "new_loan_approved", "undrawn_new_loan_approved"], na_values=['N.A.'])
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[["outstanding_loan", "gross_loan_made", "new_loan_approved", "undrawn_new_loan_approved"]] = df[["outstanding_loan", "gross_loan_made", "new_loan_approved", "undrawn_new_loan_approved"]].applymap(Normalizer().times1000000)
		df[["delinquency_ratio", "rescheduled_loan_ratio"]] = df[["delinquency_ratio", "rescheduled_loan_ratio"]].applymap(Normalizer().divide100)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0308(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="T3.8", skiprows=16, skip_footer=2, usecols=[0, 1, 3, 4, 9], names=['year', 'quarter', 'number_of_account', 'delinquent_amount', 'average_ar'])
		df['year'].fillna(method='ffill', inplace=True)
		df = df[df['quarter'].notnull()]
		df[['number_of_account']] = df[['number_of_account']].applymap(Normalizer().times1000)
		df[['delinquent_amount', 'average_ar']] = df[['delinquent_amount', 'average_ar']].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT050301(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="T5.3.1", skiprows=13, skip_footer=24, na_values=['N.A.', '-'], usecols=[0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], names=['year', 'month', '7_day', '30_day', '91_day', '182_day', '273_day', '364_day', '2_year', '3_year', '4_year', '5_year', '7_year', '10_year', '15_year'])
		df = df[df['month'].notnull()]
		df['year'].fillna(method='ffill', inplace=True)
		df['year'] = df['year'].astype('int')
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT060102(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df_base1983 = pd.read_excel(self.input().path, sheet_name="T6.1.2(old basket, 11.1983=100)", skiprows=15, skip_footer=6, usecols=[0, 1, 25], names=['year', 'month', 'real_effective_exchange_rate'], na_values=['N.A.'])
		df_base2000 = pd.read_excel(self.input().path, sheet_name="T6.1.2(old basket, 01.2000=100)", skiprows=18, skip_footer=5, usecols=[0, 1, 22], names=['year', 'month', 'real_effective_exchange_rate'], na_values=['N/A'])
		df_base2010 = pd.read_excel(self.input().path, sheet_name="T6.1.2(new basket, 01.2010=100)", skiprows=18, skip_footer=5, usecols=[0, 1, 23], names=['year', 'month', 'real_effective_exchange_rate'], na_values=['N/A'])
		df_base1983['year'].fillna(method='ffill', inplace=True)
		df_base1983['year'] = df_base1983['year'].astype('int')
		df_base1983['month'] = df_base1983['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_base2000['year'].fillna(method='ffill', inplace=True)
		df_base2000['year'] = df_base2000['year'].astype('int')
		df_base2000['month'] = df_base2000['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df_base2010['year'].fillna(method='ffill', inplace=True)
		df_base2010['year'] = df_base2010['year'].astype('int')
		df_base2010['month'] = df_base2010['month'].apply(lambda x: strptime(x, '%b').tm_mon)

		rate2000 = df_base1983[(df_base1983['year'] == 2000) & (df_base1983['month'] == 1)].iloc[0]['real_effective_exchange_rate']
		df_base1983['real_effective_exchange_rate'] = df_base1983['real_effective_exchange_rate'].apply(lambda x: x / rate2000 * 100)
		df_base1983 = df_base1983[(df_base1983['year'] < 2000)]
		rate2010 = df_base2000[(df_base2000['year'] == 2010) & (df_base2000['month'] == 1)].iloc[0]['real_effective_exchange_rate']
		df_base1983['real_effective_exchange_rate'] = df_base1983['real_effective_exchange_rate'].apply(lambda x: x / rate2010 * 100)
		df_base2000['real_effective_exchange_rate'] = df_base2000['real_effective_exchange_rate'].apply(lambda x: x / rate2010 * 100)
		df_base2000 = df_base2000[(df_base2000['year'] < 2010)]

		df = pd.concat([df_base1983, df_base2000, df_base2010])
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT060103(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=16, skip_footer=2, usecols=[0, 19], names=['date', 'effective_exchange_rate'], parse_dates=[0])
		df = df[df['date'].notnull()]
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT060303(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=8, skip_footer=13, usecols=[0, 3, 4, 5, 6, 7, 9], names=["date", "overnight", "1_week", "1_month", "3_month", "6_month", "12_month"], parse_dates=[0], na_values=['N.A.'])
		df = df[df['date'].notnull()]
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0605(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=8, skip_footer=27, usecols=[0, 1, 6], names=['year', 'month', 'composite_interest_rate'], converters={0: int})
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df['month'].apply(lambda x: strptime(x.strip(" "), '%b').tm_mon)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT070202(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=13, skip_footer=1, usecols=[0, 3, 4, 5, 6], names=["date", "certificate_of_indebtedness", "currency_in_circulation", "aggregate_balance", "exchange_fund_bills_and_notes"], parse_dates=[0])
		df = df[df['date'].notnull()]
		df[["certificate_of_indebtedness", "currency_in_circulation", "aggregate_balance", "exchange_fund_bills_and_notes"]] = df[["certificate_of_indebtedness", "currency_in_circulation", "aggregate_balance", "exchange_fund_bills_and_notes"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT0803(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, skiprows=17, skip_footer=20, usecols=[0, 1, 4, 5, 6, 8], names=["year", "month", "certificate_of_indebtedness", "government_issued_currency", "exchange_fund_bills_and_notes", "aggregate_balance"], converters={0: int})
		df['year'].fillna(method="ffill", inplace=True)
		df = df[df['month'].notnull()]
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[["certificate_of_indebtedness", "government_issued_currency", "exchange_fund_bills_and_notes", "aggregate_balance"]] = df[["certificate_of_indebtedness", "government_issued_currency", "exchange_fund_bills_and_notes", "aggregate_balance"]].applymap(Normalizer().times1000000)
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseHkmaT090401(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_excel(self.input().path, sheet_name="T9.4.1 (Benchmark yields)", skiprows=17, skip_footer=7, na_values=['N.A.', '-'], usecols=[0, 1, 9, 10, 11, 12, 13], names=['year', 'month', '2_year', '3_year', '5_year', '10_year', '15_year'])
		df = df[df['month'].notnull()]
		df['year'].fillna(method='ffill', inplace=True)
		df['year'] = df['year'].astype('int')
		df['month'] = df['month'].apply(lambda x: strptime(x, '%b').tm_mon)
		df[['2_year', '3_year', '5_year', '10_year', '15_year']] = df[['2_year', '3_year', '5_year', '10_year', '15_year']].astype('float')
		df.to_csv(self.output().path, **STAGE_FORMAT)
