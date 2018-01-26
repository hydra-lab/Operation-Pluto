# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Fed pipeline orchestrators. """

import luigi
import pandas as pd
from .utils import STAGE_FORMAT
from .task import MkDir, Parse, ExtractHttp, Load

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickFed(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='fed')
	def requires(self):
		yield KickFedH15(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickFedH15(MkDir):
	workdir = luigi.Parameter(default='h15-selected-interest-rates')
	def requires(self):
		extract = ExtractFed(**self.givedir, urlpath='?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package')
		parse = ParseFedH15(**self.givedir, upstream=extract)
		yield Load(**self.givedir, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractFed(ExtractHttp):
	domain = luigi.Parameter(default="https://www.federalreserve.gov/datadownload/Output.aspx")

class ParseFedH15(Parse):
	workdir = luigi.Parameter(default='staging')
	def run(self):
		super().run()
		df = pd.read_csv(self.input().path, na_values=['ND'], skiprows=6, names=["date", "1_month", "3_month", "6_month", "1_year", "2_year", "3_year", "5_year", "7_year", "10_year", "20_year", "30_year"])
		df.to_csv(self.output().path, **STAGE_FORMAT)

