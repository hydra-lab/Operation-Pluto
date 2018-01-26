# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" FRED pipeline orchestrators. """

import luigi
import pandas as pd
from .utils import STAGE_FORMAT, Normalizer
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickFred(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='fred')
	def requires(self):
		yield KickFredCpi(**self.givedir)
		yield KickFredCpi(**self.givedir)
		yield KickFredHknfc(**self.givedir)
		yield KickFredHkdReer(**self.givedir)
		yield KickFredPce(**self.givedir)
		yield KickFredLiborOvernight(**self.givedir)
		yield KickFredLibor1Week(**self.givedir)
		yield KickFredLibor1Month(**self.givedir)
		yield KickFredLibor2Month(**self.givedir)
		yield KickFredLibor3Month(**self.givedir)
		yield KickFredLibor6Month(**self.givedir)
		yield KickFredLibor12Month(**self.givedir)
		yield KickFredTips5Year(**self.givedir)
		yield KickFredTips7Year(**self.givedir)
		yield KickFredTips10Year(**self.givedir)
		yield KickFredTips20Year(**self.givedir)
		yield KickFredTips30Year(**self.givedir)
		yield KickFredHyOas(**self.givedir)
		yield KickFredBbbOas(**self.givedir)
		yield KickFredVxTyn10Year(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickFredCpi(MkDir):
	workdir = luigi.Parameter(default='cpi-for-all-urban-consumers-core')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=CPILFESL')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='core_cpi')
		yield Load(**self.givedir, upstream=parse)

class KickFredHknfc(MkDir):
	workdir = luigi.Parameter(default='credit-to-hknfc')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=CRDQHKANABIS')
		parse = ParseFredHknfc(**self.givedir, upstream=extract, seriesname='hknfc_debt')
		yield Load(**self.givedir, upstream=parse)

class KickFredHkdReer(MkDir):
	workdir = luigi.Parameter(default='hkd-narrow-reer')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=RNHKBIS')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='hkd_real_narrow_effective_exchange_rate')
		yield Load(**self.givedir, upstream=parse)

class KickFredPce(MkDir):
	workdir = luigi.Parameter(default='pce')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=PCEPI')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='pce')
		yield Load(**self.givedir, upstream=parse)

class KickFredLiborOvernight(MkDir):
	workdir = luigi.Parameter(default='libor-overnight')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USDONTD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='overnight_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor1Week(MkDir):
	workdir = luigi.Parameter(default='libor-1-week')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD1WKD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='1_week_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor1Month(MkDir):
	workdir = luigi.Parameter(default='libor-1-month')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD1MTD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='1_month_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor2Month(MkDir):
	workdir = luigi.Parameter(default='libor-2-month')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD2MTD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='2_month_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor3Month(MkDir):
	workdir = luigi.Parameter(default='libor-3-month')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD3MTD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='3_month_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor6Month(MkDir):
	workdir = luigi.Parameter(default='libor-6-month')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD6MTD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='6_month_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredLibor12Month(MkDir):
	workdir = luigi.Parameter(default='libor-12-month')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=USD12MD156N')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='12_month_libor')
		yield Load(**self.givedir, upstream=parse)

class KickFredTips5Year(MkDir):
	workdir = luigi.Parameter(default='tips-5-year')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=DFII5')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='5_year_tips')
		yield Load(**self.givedir, upstream=parse)

class KickFredTips7Year(MkDir):
	workdir = luigi.Parameter(default='tips-7-year')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=DFII7')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='7_year_tips')
		yield Load(**self.givedir, upstream=parse)

class KickFredTips10Year(MkDir):
	workdir = luigi.Parameter(default='tips-10-year')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=DFII10')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='10_year_tips')
		yield Load(**self.givedir, upstream=parse)

class KickFredTips20Year(MkDir):
	workdir = luigi.Parameter(default='tips-20-year')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=DFII20')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='20_year_tips')
		yield Load(**self.givedir, upstream=parse)

class KickFredTips30Year(MkDir):
	workdir = luigi.Parameter(default='tips-30-year')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=DFII30')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='30_year_tips')
		yield Load(**self.givedir, upstream=parse)

class KickFredHyOas(MkDir):
	workdir = luigi.Parameter(default='bofa-hy-oas')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=BAMLH0A0HYM2')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='bofa_hy_oas')
		yield Load(**self.givedir, upstream=parse)

class KickFredBbbOas(MkDir):
	workdir = luigi.Parameter(default='bofa-bbb-oas')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=BAMLC0A4CBBB')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='bofa_bbb_oas')
		yield Load(**self.givedir, upstream=parse)

class KickFredVxTyn10Year(MkDir):
	workdir = luigi.Parameter(default='t-note-10-year-vol-future')
	def requires(self):
		extract = ExtractFred(**self.givedir, urlpath='?id=VXTYN')
		parse = ParseFred(**self.givedir, upstream=extract, seriesname='10_year_tn_vx')
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractFred(ExtractHttp):
	domain = luigi.Parameter(default="https://fred.stlouisfed.org/graph/fredgraph.csv")

class ParseFred(Parse):
	workdir = luigi.Parameter(default='staging')
	seriesname = luigi.Parameter(default='')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, header=0, parse_dates=[0], na_values=['.'], names=['date', self.seriesname])
		df[self.seriesname] = df[self.seriesname].astype('float')
		df.to_csv(self.output().path, **STAGE_FORMAT)

class ParseFredHknfc(Parse):
	workdir = luigi.Parameter(default='staging')
	seriesname = luigi.Parameter(default='')
	def run(self):
		super().run()
		x1B = Normalizer().times1000000000
		df = pd.read_csv(self.input().path, header=0, parse_dates=[0], na_values=['.'], names=['date', self.seriesname], converters={1: x1B})
		df.to_csv(self.output().path, **STAGE_FORMAT)
