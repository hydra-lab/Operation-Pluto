# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Tests for task module """

import os
from unittest import TestCase
import pluto.task


class TestMkDir(TestCase):
	""" Tests for `MkDir()` """
	
	def setUp(self):
		self.task = pluto.task.MkDir(dirname='test/data', workdir='workdir')

	def test_default_path(self):
		self.assertEqual(self.task.path, 'test\\data\\workdir')
		self.assertEqual(self.task.givedir, {'dirname': 'test\\data\\workdir'})

	def test_fullpath(self):
		self.fullpath = pluto.task.MkDir(dirname='test/data', workdir='workdir', fullpath='test/data/fullpath')
		self.assertEqual(self.fullpath.path, 'test\\data\\fullpath')

	def test_mkdir(self):
		self.task.run()
		self.assertTrue(os.path.exists('test/data/workdir'))


class TestGenericTask(TestCase):
	""" Tests for `GenericTask()` """

	def test_default_filename(self):
		open('test/data/workdir/staging/data.csv', 'w+').close()
		self.default = pluto.task.GenericTask(dirname='test/data/workdir', workdir='staging')
		self.assertEqual(self.default.filename(), 'data.csv')
		self.assertEqual(self.default.path, 'test\\data\\workdir\\staging\\data.csv')
		self.assertEqual(self.default.output().path, 'test\\data\\workdir\\staging\\data.csv')
		self.assertTrue(os.path.exists('test/data/workdir/staging/data.csv'))
		
	def test_change_filename(self):
		self.change = pluto.task.GenericTask(dirname='test/data/workdir-chg', workdir='staging')
		self.change.filename('data.gz')
		self.assertEqual(self.change.filename(), 'data.gz')
		self.assertEqual(self.change.path, 'test\\data\\workdir-chg\\staging\\data.gz')
		self.assertEqual(self.change.output().path, 'test\\data\\workdir-chg\\staging\\data.gz')

	def test_fullpath(self):
		self.fullpath = pluto.task.GenericTask(dirname='test', workdir='workdir', fullpath='test/data/fullpath/source/data.gz')
		self.assertEqual(self.fullpath.path, 'test\\data\\fullpath\\source\\data.gz')


class TestExtractHttp(TestCase):
	""" Tests for `ExtractHttp()` """
	
	def setUp(self):
		os.makedirs('test/data/workdir/source', exist_ok=True)
		open('test/data/workdir/source/nonexist.dat', 'w+').close()
		self.dl = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/source/test100k.db')
		self.dl.complete()
		if not self.dl.complete():
			self.dl.run()
		os.makedirs('test/data/workdir/overwrite', exist_ok=True)

	def test_fixurl(self):
		self.fix = pluto.task.ExtractHttp(domain='http://youtube.com/ ', urlpath=' /watch=')
		self.assertEqual(self.fix.endpoint(), 'http://youtube.com/watch=')

	def test_fullurl(self):
		self.fullurl = pluto.task.ExtractHttp(domain='youtube.com', url='google.com/ ')
		self.assertEqual(self.fullurl.endpoint(), 'https://google.com')

	def test_complete(self):
		self.complete_local = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/source/test100k.db')
		self.assertTrue(self.complete_local.complete())
		self.nonexist_local = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/source/nonexist.dat')
		self.assertFalse(self.nonexist_local.complete())
		self.obsolete_local = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/source/SpeedTest_16MB.dat')
		self.assertFalse(self.obsolete_local.complete())
		self.nonexist_remote = pluto.task.ExtractHttp(url='nonexistentdomain', fullpath='test/data/workdir/source/test100k.db')
		self.assertFalse(self.nonexist_remote.complete())

	def test_zero_remote_non_overwrite(self):
		self.zeroremote = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/overwrite/test100k.db')
		if not self.zeroremote.complete():
			self.zeroremote.run()
		self.assertTrue(os.stat(self.zeroremote.path).st_size > 0)
		self.zeroremote.url = 'http://speedtest.ftp.otenet.gr/files/test101k.db'
		if not self.zeroremote.complete():
			self.zeroremote.run()
		self.assertTrue(os.stat(self.zeroremote.path).st_size > 0)

	def test_overwrite_zero_local(self):
		open('test/data/workdir/overwrite/test100k.db', 'w+').close()
		self.zerolocal = pluto.task.ExtractHttp(url='http://speedtest.ftp.otenet.gr/files/test100k.db', fullpath='test/data/workdir/overwrite/test100k.db')
		if not self.zerolocal.complete():
			self.zerolocal.run()
		self.assertTrue(os.stat(self.zerolocal.path).st_size > 0)


class TestListDirectory(TestCase):
	""" Tests for `ListDirectory()` """

	def test_default_path(self):
		self.default = pluto.task.ListDirectory(dirname='test/data/workdir')
		self.assertEqual(self.default.path, 'test\\data\\workdir\\map')
		for target in self.default.output():
			self.assertEqual(os.path.dirname(target.path), 'test\\data\\workdir\\map')

	def test_fullpath(self):
		self.fullpath = pluto.task.ListDirectory(fullpath='test/data/workdir/source')
		self.assertEqual(self.fullpath.path, 'test\\data\\workdir\\source')


class TestListFile(TestCase):
	""" Tests for `ListFile()` """
	
	def test_default_filename(self):
		self.default = pluto.task.ListFile(dirname='test/data/workdir')
		self.assertEqual(self.default.filename(), 'data.csv')
		self.assertEqual(self.default.path, 'test\\data\\workdir\\map\\data.csv')
		self.assertEqual(self.default.output().path, 'test\\data\\workdir\\map\\data.csv')
	
	def test_fullpath(self):
		self.fullpath = pluto.task.ListFile(fullpath='test/data/workdir/map/data.dat')
		self.assertEqual(self.fullpath.path, 'test\\data\\workdir\\map\\data.dat')
		self.fullpath.filename('data.gz')
		self.assertEqual(self.fullpath.path, 'test\\data\\workdir\\map\\data.dat')

	def test_change_filename(self):
		self.change = pluto.task.ListFile(dirname='test/data/workdir-chg', workdir='staging')
		self.change.filename('data.gz')
		self.assertEqual(self.change.filename(), 'data.gz')
		self.assertEqual(self.change.path, 'test\\data\\workdir-chg\\staging\\data.gz')
		self.assertEqual(self.change.output().path, 'test\\data\\workdir-chg\\staging\\data.gz')


class MergeCsv(TestCase):
	""" Tests for `MergeCsv()` """

	def setUp(self):
		os.makedirs('test/data/workdir/map-merge', exist_ok=True)
		with open('test/data/workdir/map-merge/data1.csv', 'w+') as f1:
			f1.write('item,message\n1,hello-world1\n')
		with open('test/data/workdir/map-merge/data2.csv', 'w+') as f2:
			f2.write('item,message\n2,hello-world2\n')
		ls = pluto.task.ListDirectory(dirname="test/data/workdir", workdir="map-merge")
		self.task = pluto.task.MergeCsv(dirname="test/data/workdir", workdir="staging", upstream=ls)

	def test_merge(self):
		self.task.filename('merged.csv')
		self.task.run()
		with open('test/data/workdir/staging/merged.csv', 'r') as fin:
			data = fin.read()
		self.assertEqual(data, 'item,message\n1,hello-world1\n2,hello-world2\n')


if __name__ == '__main__':
	unittest.main()
