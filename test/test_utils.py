# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Tests for utils module """

import hashlib
import datetime
from unittest import TestCase
import requests as req
import pluto.utils
from pluto.task import proxies


class TestDate(TestCase):
	""" Tests for `Date()` """

	def setUp(self):
		self.task = pluto.utils.Date(datetime.date(1900, 1, 4))

	def test_init(self):
		self.assertTrue(self.task.last_bday, datetime.date(1900, 1, 4))


class TestNormalizer(TestCase):
	""" Tests for `Normalizer()` """

	def setUp(self):
		self.converter = pluto.utils.Normalizer()

	def test_set_prec(self):
		self.converter_init = pluto.utils.Normalizer(3)
		self.assertEqual(self.converter_init.prec, 3)
		self.converter_init.prec = -13
		self.assertEqual(self.converter_init.prec, 15)

	def test_constructor_out_of_bound(self):
		self.converter_init = pluto.utils.Normalizer(16)
		self.assertEqual(self.converter_init.prec, 15)

	def test_times_divide(self):
		self.assertEqual(self.converter.times1000(""), "")
		self.assertEqual(self.converter.times1000000(), None)
		self.assertEqual(self.converter.times1000000000('-0.003141592653589793'), -3141592.65358979)
		self.assertEqual(self.converter.divide100('3.14159265358979323846'), 0.0314159265358979)

	def test_sqmeter2sqfeet(self):
		self.assertEqual(self.converter.sqmeter2sqfeet('0.0'), 0)


class TestHash(TestCase):
	""" Tests for hashing tools """

	def setUp(self):
		self.hash_function = hashlib.md5()
		self.dl = pluto.task.ExtractHttp(url='http://ipv4.download.thinkbroadband.com/5MB.zip', fullpath='test/data/workdir/source/hash.dat')
		self.dl.complete()
		if not self.dl.complete():
			self.dl.run()

	def test_hash_local(self):
		with open('test/data/workdir/source/hash.dat', 'rb') as fin:
			digest = pluto.utils.chunk_hash(fin.read(), self.hash_function)
		self.assertEqual(digest, 'b3215c06647bc550406a9c8ccc378756')

	def test_hash_remote(self):
		response = req.get('http://ipv4.download.thinkbroadband.com/5MB.zip', proxies={'https': proxies().https, 'http': proxies().http})
		digest = pluto.utils.chunk_hash(response.content, self.hash_function)
		self.assertEqual(digest, 'b3215c06647bc550406a9c8ccc378756')

	def test_hash_null(self):
		null_byte_digest = pluto.utils.chunk_hash(None, self.hash_function)
		self.assertEqual(null_byte_digest, 'd41d8cd98f00b204e9800998ecf8427e')
		open('test/data/workdir/source/null.dat', 'w+').close()
		with open('test/data/workdir/source/null.dat', 'rb') as fin:
			null_file_digest = pluto.utils.chunk_hash(fin.read(), self.hash_function)
		self.assertEqual(null_file_digest, 'd41d8cd98f00b204e9800998ecf8427e')


if __name__ == '__main__':
    unittest.main()
