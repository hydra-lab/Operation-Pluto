# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" HKAB pipeline orchestrators. """

import csv
import datetime
import bs4 as bs
import luigi
import pandas as pd
from .task import MkDir, Parse, ExtractHttp, MergeCsv, ListDirectory, Load
from .utils import Date

# Data-source level orchestrators.
# Delegate endpoints / tables.

class KickHkab(MkDir):
	dirname = luigi.Parameter(default='data')
	workdir = luigi.Parameter(default='hkab')
	def requires(self):
		yield KickHkabHiborDaily(**self.givedir)

# Endpoint / table level orchestrators.
# Arrange and kick off jobs.

class KickHkabHiborDaily(MkDir):
	workdir = luigi.Parameter(default='hkd-interest-settlement-rates')
	def requires(self):
		pastdays = Date().last_week_bdays
		for pastday in pastdays:
			extract = ExtractHkab(**self.givedir, date=pastday, urlpath='/hibor/listRates.do?year={}&month={}&day={}&Submit=Search'.format(pastday.strftime("%Y"), pastday.strftime("%#m"), pastday.strftime("%#d")))
			parse = ParseHkabHibor(**self.givedir, date=pastday, upstream=extract)
			yield parse

		merge = MergeCsv(**self.givedir, upstream=ListDirectory(**self.givedir))
		yield Load(**self.givedir, upstream=merge)

class KickHkabHiborHistorical(MkDir):
	workdir = luigi.Parameter(default='hkd-interest-settlement-rates')
	def requires(self):
		pastdays = pd.date_range(datetime.date(2002, 3, 2), datetime.date.today() - datetime.timedelta(days=1), freq='B').date
		for pastday in pastdays:
			extract = ExtractHkab(**self.givedir, date=pastday, urlpath='/hibor/listRates.do?year={}&month={}&day={}&Submit=Search'.format(pastday.strftime("%Y"), pastday.strftime("%#m"), pastday.strftime("%#d")))
			parse = ParseHkabHibor(**self.givedir, date=pastday, upstream=extract)
			yield Load(date=pastday, upstream=parse)

# Implement execution of job at leaf nodes.

class ExtractHkab(ExtractHttp):
	domain = luigi.Parameter(default="http://www.hkab.org.hk")
	def filename(self, custom=None, default='data{}.dat'.format('')):
		default = 'data{}.dat'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

class ParseHkabHibor(Parse):
	workdir = luigi.Parameter(default='map')
	keymap = {'Overnight': 'overnight_hibor',
           '1 Week': '1_week_hibor',
           '2 Weeks': '2_week_hibor',
           '1 Month': '1_month_hibor',
           '2 Months': '2_month_hibor',
           '3 Months': '3_month_hibor',
           '4 Months': '4_month_hibor',
           '5 Months': '5_month_hibor',
           '6 Months': '6_month_hibor',
           '7 Months': '7_month_hibor',
           '8 Months': '8_month_hibor',
           '9 Months': '9_month_hibor',
           '10 Months': '10_month_hibor',
           '11 Months': '11_month_hibor',
           '12 Months': '12_month_hibor'}
	
	def filename(self, custom=None, default='data{}.csv'.format('')):
		default = 'data{}.csv'.format(self.date.strftime("-%Y-%m-%d"))
		return super().filename(custom=custom, default=default)

	def run(self):
		super().run()
		with open(self.input().path, 'r', encoding='utf-8') as page, open(self.output().path, 'w', newline='', encoding='utf-8') as fout:
			soup = bs.BeautifulSoup(page, 'lxml')
			table = soup.find('table', {'class': 'etxtmed', 'bgcolor': "#ffffff"})
			rows = table.find_all('tr')[3:-2]
			data = {}
			data['date'] = self.date.strftime("%Y-%m-%d")
			writer = csv.DictWriter(fout, fieldnames=["date", "overnight_hibor", "1_week_hibor", "2_week_hibor", "1_month_hibor", "2_month_hibor", "3_month_hibor", "4_month_hibor", "5_month_hibor", "6_month_hibor", "7_month_hibor", "8_month_hibor", "9_month_hibor", "10_month_hibor", "11_month_hibor", "12_month_hibor"], quoting=csv.QUOTE_ALL)
			writer.writeheader()
			for row in rows:
				field = self.keymap.get(row.find('td', {'align': 'right'}).text, "")
				if row.find('td', {'align': 'middle'}).text == 'N/A':
					data[field] = ''
				else:
					data[field] = row.find('td', {'align': 'middle'}).text
			writer.writerow(data)
