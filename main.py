#!/usr/bin/env python

# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Top-most pipeline orchestrators. """

import logging
import luigi
import pluto

# Primordial orchestrator.

class RunAll(pluto.task.MkDir):
	""" Top-most level orchestrator. Kick entire pipeline. Traverse all data sources. """
	# Data directory structure is defined in code
	# Cascade working directory down to child tasks recursively
	# Child task output under parent task's directory
	workdir = luigi.Parameter(default='data')

	def __init__(self, *args, **kwargs):
		# Set-up logging
		super().__init__(*args, **kwargs)
		logging.basicConfig(filename='log/luigi.log', level=logging.DEBUG, filemode="w")
		pluto.task.ExtractHttp().__config_logger__(filename='log/http-request.log', level=logging.INFO, filemode="w")

	def requires(self):
		# Curate data sources.
		yield pluto.bls.KickBls(**self.givedir)
		yield pluto.censtatd.KickCenstatd(**self.givedir)
		yield pluto.fed.KickFed(**self.givedir)
		yield pluto.fred.KickFred(**self.givedir)
		yield pluto.hkab.KickHkab(**self.givedir)
		yield pluto.hkgb.KickHkgb(**self.givedir)
		yield pluto.hkma.KickHkma(**self.givedir)
		yield pluto.holiday.KickHolidayApi(**self.givedir)
		yield pluto.hsi.KickHsi(**self.givedir)
		yield pluto.rvd.KickRvd(**self.givedir)


class RunDaily(pluto.task.MkDir):
	""" Schedule for daily updating data sources only. """
	workdir = luigi.Parameter(default='data')

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		logging.basicConfig(filename='log/luigi.log', level=logging.DEBUG, filemode="w")
		pluto.task.ExtractHttp().__config_logger__(filename='log/http-request.log', level=logging.INFO, filemode="w")

	def requires(self):
		yield pluto.fed.KickFed(**self.givedir)
		yield pluto.fred.KickFred(**self.givedir)
		yield pluto.hkab.KickHkab(**self.givedir)
		yield pluto.hkgb.KickHkgb(**self.givedir)
		yield pluto.hkma.KickHkma(**self.givedir)
		yield pluto.holiday.KickHolidayApi(**self.givedir)
		yield pluto.hsi.KickHsi(**self.givedir)


class RunWeekly(pluto.task.MkDir):
	""" Schedule for sparsely updating data sources only. """
	workdir = luigi.Parameter(default='data')

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		logging.basicConfig(filename='log/luigi.log', level=logging.DEBUG, filemode="w")
		pluto.task.ExtractHttp().__config_logger__(filename='log/http-request.log', level=logging.INFO, filemode="w")

	def requires(self):
		yield pluto.bls.KickBls(**self.givedir)
		yield pluto.censtatd.KickCenstatd(**self.givedir)
		yield pluto.rvd.KickRvd(**self.givedir)


class RunMock(pluto.task.MkDir):
	""" Mock. """
	workdir = luigi.Parameter(default='test/data')

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		logging.basicConfig(filename='test/log/luigi.log', level=logging.DEBUG, filemode="w")
		pluto.task.ExtractHttp().__config_logger__(filename='test/log/http-request.log', level=logging.DEBUG, filemode="w")

	def requires(self):
		yield pluto.censtatd.KickCenstatd(**self.givedir)
		yield pluto.hkgb.KickHkgb(**self.givedir)


if __name__ == '__main__':
	luigi.run()
