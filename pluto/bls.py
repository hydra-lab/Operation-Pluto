# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" BLS pipeline orchestrators. """

import luigi
import pandas as pd
from .utils import STAGE_FORMAT
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickBls(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='bls')
	def requires(self):
		yield KickBlsCpi(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickBlsCpi(MkDir):
	workdir = luigi.Parameter(default='consumer-price-index-for-all-urban-consumers')
	def requires(self):
		extract = ExtractBls(**self.givedir, urlpath='/pub/time.series/cu/cu.data.1.AllItems')
		parse = ParseBlsCpi(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractBls(ExtractHttp):
	domain = luigi.Parameter(default="http://download.bls.gov")

class ParseBlsCpi(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, sep='\t', usecols=[0, 1, 2, 3], header=0, names=['series_id', 'year', 'month', 'cpi'], converters={'cpi': float})
		df = df.loc[df['series_id'] == 'CUSR0000SA0      ', ['year', 'month', 'cpi']]
		df['month'] = df['month'].str.replace('M', '').astype('int')
		df.to_csv(self.output().path, **STAGE_FORMAT)
