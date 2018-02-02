# Copyright (c) 2018, Operation Pluto contributors. #
# All rights reserved. #

""" Pipeline Task Templates """

import os
import time
from random import uniform
import warnings
import logging
import hashlib
import datetime
import luigi
import requests as req
import urllib3.util.url
from .utils import chunk_hash

TASK_PRIORITY = {'extract-http': 999, 'parse': 500, 'generic-task' : 500, \
'list-file': 200, 'list-directory': 100, 'merge-csv': 50, 'load': 20 \
, 'make-directory': 0}

# Luigi Configurations #

class proxies(luigi.Config):
	https = luigi.Parameter(default='')
	http = luigi.Parameter(default='')

class mysql(luigi.Config):
	username = luigi.Parameter(default='')
	password = luigi.Parameter(default='')
	hostname = luigi.Parameter(default='')
	port = luigi.IntParameter(default=3306)

class force(luigi.Config):
	forcable = luigi.BoolParameter(default=True)


# Luigi Task Implementations #

class GenericWrapperTask(luigi.WrapperTask):
	"""
	Store pathname segment in `path`, pass it on to upstream tasks. \n
	Chain other tasks and run nothing else.

	:param upstream: Upstream task. `luigi.Task()`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	upstream = luigi.TaskParameter(default=luigi.WrapperTask())
	dirname = luigi.Parameter(default='')
	workdir = luigi.Parameter(default='.')
	fullpath = luigi.Parameter(default='')
	date = luigi.DateParameter(default=datetime.date.today())

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		# Prepare to give Path data as parent path of upstream task
		self.givedir = {'dirname': self.path}

	def filename(self, custom=None, default=''):
		"""
		Assign filename to `name` and override this method.

		Call `Task().filename()` to initialize to default and reset `path`.
		Call `Task().filename(filename)` to reset filename and `path`.
		"""
		if not hasattr(self, '_filename'):
			self._filename = default

		if custom is None:
			return self._filename
		else:
			self._filename = custom
			self.path = self._resolve_path()
			return self._filename

	def _resolve_path(self):
		""" `fullpath` takes precedence and will be locked in.
		Otherwise join path parameters to form full path. """
		if os.path.normpath(self.fullpath) != '.':
			return self.fullpath
		else:
			return os.path.join(self.dirname, self.workdir, self.filename())

	@property
	def path(self):
		""" Return full path of working directory being created. """
		self.path = self._resolve_path()
		return self._path

	@path.setter
	def path(self, value):
		self._path = os.path.normpath(value)

	def requires(self):
		""" Override `requires()` to set upstream task. """
		return self.upstream

	def run(self):
		""" Create working directory if it does not exist. """
		os.makedirs(self.path, exist_ok=True)


class GenericExternalTask(luigi.ExternalTask):
	"""
	Base `ExternalTask()`.

	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Default 'map'. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	dirname = luigi.Parameter(default='')
	workdir = luigi.Parameter(default='.')
	fullpath = luigi.Parameter(default='')
	date = luigi.DateParameter(default=datetime.date.today())

	def filename(self, custom=None, default=''):
		"""
		Assign filename to `name` and override this method.

		Call `Task().filename()` to initialize to default and reset `path`.
		Call `Task().filename(filename)` to reset filename and `path`.
		"""
		if not hasattr(self, '_filename'):
			self._filename = default

		if custom is None:
			return self._filename
		else:
			self._filename = custom
			self.path = self._resolve_path()
			return self._filename

	def _resolve_path(self):
		""" `fullpath` takes precedence and will be locked in.
		Otherwise join path parameters to form full path. """
		if os.path.normpath(self.fullpath) != '.':
			return self.fullpath
		else:
			return os.path.join(self.dirname, self.workdir, self.filename())

	@property
	def path(self):
		""" Return full path of working directory being listed. """
		self.path = self._resolve_path()
		return self._path

	@path.setter
	def path(self, value):
		self._path = os.path.normpath(value)

	def run(self):
		run = None


class GenericTask(luigi.Task):
	"""
	Base `Task`. \n
	Create a working directory under passed-in parent path if it does not exist. \n
	Implement plumbing routine in `run()`. \n
	Output file under working directory.
	Recommend single output per `Task` Instance. \n

	:param upstream: Upstream task. `luigi.Task()`.
	:param force: Delete targets first. `bool`. Default `False`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	# Upstream task
	upstream = luigi.TaskParameter(default=luigi.WrapperTask())
	# Force overwrite once
	force = luigi.BoolParameter(default=False)
	# Parent directory
	dirname = luigi.Parameter(default='')
	# Working directory
	workdir = luigi.Parameter(default='.')
	# Full filepath
	fullpath = luigi.Parameter(default='')
	# Date with no time component. Default is today.
	date = luigi.DateParameter(default=datetime.date.today())
	# Luigi Task Priority
	priority = TASK_PRIORITY['generic-task']

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		# https://groups.google.com/forum/#!topic/luigi-user/vO0Ubtb3TBY
		self._run = False
		self.force = self.force if force().forcable else False

	def filename(self, custom=None, default='data{}.csv'.format('')):
		"""
		Assign filename to `name` and override this method.
		Or call `Task().filename(filename)` to reset filename and `path`.

		Define file name of output target. \n
		Default to CSV for universal inter-working.
		"""
		if not hasattr(self, '_filename'): self._filename = default

		if custom is None:
			return self._filename
		else:
			self._filename = custom
			self.path = self._resolve_path()['fullpath']
			return self._filename

	def _resolve_path(self):
		""" `fullpath` takes precedence and will be locked in.
		Otherwise join path parameters to form full path. """
		params = {}
		if os.path.normpath(self.fullpath) != '.':
			params['dirname'] = os.path.dirname(os.path.dirname(self.fullpath))
			params['workdir'] = os.path.basename(os.path.dirname(self.fullpath))
			params['filename'] = os.path.basename(self.fullpath)
			params['fullpath'] = self.fullpath
			return params
		else:
			params['dirname'] = self.dirname
			params['workdir'] = self.workdir
			params['filename'] = self.filename()
			params['fullpath'] = os.path.join(self.dirname, self.workdir, self.filename())
			return params

	@property
	def path(self):
		""" Return full path of output file. This path is called by `output()`. """
		self.path = self._resolve_path()['fullpath']
		return self._path

	@path.setter
	def path(self, value):
		self._path = os.path.normpath(value)


	def _force_delete_target(self):
		# https://github.com/spotify/luigi/issues/595
		"""
		DEPRECATED. If Luigi config enables delete, force delete output targets.
		 """
		if self.force:
			outputs = luigi.task.flatten(self.output())
			for out in outputs:
				if out.exists():
					os.remove(self.output().path)

	# http://luigi.readthedocs.io/en/stable/luigi_patterns.html#atomic-writes-problem
	# http://luigi.readthedocs.io/en/stable/api/luigi.target.html#luigi.target.FileSystemTarget.temporary_path
	def output(self):
		return luigi.LocalTarget(self.path)

	def requires(self):
		""" Override `requires()` to set upstream task. """
		return self.upstream

	def complete(self):
		"""
		Return `True` if all outputs exist and have non-zero size,
		or Task has simply been run once.
		"""
		outputs = luigi.task.flatten(self.output())
		target_implemented = bool(outputs)
		if not target_implemented:
			warnings.warn("Task %r without outputs has no custom complete() method" % self, stacklevel=2)
			return False
		
		targets_nonzero = all(map(lambda output: output.exists() and (os.stat(output.path).st_size > 0), outputs))
		if not targets_nonzero:
			return False

		if self._run:
			return True

		PRESERVED_FILE = not self.force
		PRESERVED_DIR = self._resolve_path()['workdir'] not in ['staging']

		return target_implemented and targets_nonzero and PRESERVED_DIR and PRESERVED_FILE

	def run(self):
		""" Record task run state. """
		self.output().makedirs()
		self._run = True


class MkDir(GenericWrapperTask):
	"""
	Making data directories from parameters.

	:param upstream: Upstream task. `luigi.Task()`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""

	priority = TASK_PRIORITY['make-directory']


class Parse(GenericTask):
	"""
	Parse data into readable forms.

	:param upstream: Upstream task. `luigi.Task()`.
	:param force: Delete targets first. `bool`. Default `False`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""

	priority = TASK_PRIORITY['parse']

class ExtractHttp(GenericTask):
	"""
	Consume fixed URL.
	`GET` request HTTP resource and save to local path.

	:param upstream: Upstream task. `luigi.Task()`.
	:param force: Delete targets first. `bool`. Default `False`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param date: Date. `datetime.date`.
	:param domain: Working URL domain. URL `str`.
	:param urlpath: URL path. Path `str`.
	:param url: Full URL. Precedes `urlpath`. URL `str`.
	"""
	workdir = luigi.Parameter(default='source')
	# Working URL domain
	domain = luigi.Parameter(default='')
	# URL path
	urlpath = luigi.Parameter(default='')
	# Full URL pointing to resource
	url = luigi.Parameter(default='')

	priority = TASK_PRIORITY['extract-http']

	def filename(self, custom=None, default='data{}.dat'.format('')):
		""" Default to DAT for generic processing. """
		return super().filename(custom=custom, default=default)

	@classmethod
	def _fix_url(self, domain, urlpath=None):
		""" Repair URL from trivial typos. """
		if urlpath is None:
			parsed = urllib3.util.url.parse_url(domain.strip().strip('/'))
		else:
			parsed = urllib3.util.url.parse_url(domain.strip().strip('/') + urlpath.strip().rstrip('/'))
		
		fix_scheme = 'https' if parsed[0] is None else parsed[0]
		fix_host = None if parsed[2] is None else parsed[2].strip('/')
		fix_path = None if parsed[4] is None else '/' + parsed[4].strip('/')
		return urllib3.util.url.Url(fix_scheme, parsed[1], fix_host, parsed[3], fix_path, parsed[5], parsed[6]).url

	def endpoint(self):
		""" Construct URL from passed-in parameters. """
		if self.url.strip():
			self.url = self._fix_url(self.url)
			return self.url
		else:
			self.url = self._fix_url(self.domain, self.urlpath)
			return self.url

	@classmethod
	def __config_logger__(self, filename='log/http-request.log', level=logging.INFO, filemode='w'):
		""" Configure HTTP extraction task log. """
		httplogger = logging.getLogger("HttpLogger")
		httplogger.addHandler(logging.FileHandler(filename, mode=filemode))
		httplogger.setLevel(level)

	def _get_response(self, endpoint):
		"""
		Given endpoint address, make direct `GET` request, log, and save to buffer.
		
		:param endpoint: Full URL. Path `str`.
		"""
		httplogger = logging.getLogger("HttpLogger")
		session = req.session()
		time.sleep(uniform(0.164, 0.289))
		session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"})
		
		try:
			response = session.get(endpoint, proxies={'https': proxies().https, 'http': proxies().http})
		except req.exceptions.RequestException as e:
			message = "{}:\n  endpoint: {}\n  status: {}".format(self.output().path, endpoint, "RequestException")
			httplogger.debug(message)
			print('{}: {}'.format(endpoint, e))
			return None
		else:
			message = "{}:\n  endpoint: {}\n  status: {}".format(self.output().path, endpoint, response.status_code)
			if response.status_code == 200:
				# https://github.com/spotify/luigi/issues/1647
				httplogger.info(message)
				return response.content
			else:
				httplogger.debug(message)
				return None

	def requires(self):
		""" Extraction task is on uppermost stream. """
		return None

	def complete(self):
		"""
		If the task has any output,
		return `True` if all outputs exist, have non-zero size, and local and remote content eqauls. \n
		Otherwise, return `False`.
		"""
		generic_conditions = super().complete()
		
		# BUG : HTTP requests get logged twice. Exceptions aren't logged.

		if self._run:
			__remote = self._buffer
		else:
			__remote = self._get_response(self.endpoint())
		
		remote_nonzero = __remote is not None
		self._buffer = __remote if remote_nonzero else None

		if remote_nonzero:
			remote_digest = chunk_hash(__remote, hashlib.md5())
		else:
			return False

		targets_exist = True
		identical_to_remote = True
		outputs = luigi.task.flatten(self.output())
		for output in outputs:
			if output.exists():
				with open(output.path, 'rb') as fout:
					output_digest = chunk_hash(fout.read(), hashlib.md5())
				identical_to_remote = (output_digest == remote_digest) and identical_to_remote
			else:
				targets_exist = False

		return self._run or generic_conditions and remote_nonzero and targets_exist and identical_to_remote


	def run(self):
		""" Commit buffer to local path. """
		super().run()
		if isinstance(self._buffer, bytes):
			with open(self.output().path, 'wb') as fout:
				fout.write(self._buffer)


class ListDirectory(GenericExternalTask):
	"""
	Return a list of `LocalTarget()` under working directory.

	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	dirname = luigi.Parameter(default='')
	workdir = luigi.Parameter(default='map')
	fullpath = luigi.Parameter(default='')
	date = luigi.DateParameter(default=datetime.date.today())
	priority = TASK_PRIORITY['list-directory']

	def output(self):
		""" Return list of `LocalTarget()` under working directory. """
		return [luigi.LocalTarget(x) for x in luigi.local_target.LocalFileSystem().listdir(self.path)]


class ListFile(GenericExternalTask):
	"""
	`Task()` to wrap a single `LocalTarget()`. Inherit `filename()` to specify file name.

	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Default 'map'. Path `str`.
	:param fullpath: Full directory. Precedes all other paths. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	# Working directory
	workdir = luigi.Parameter(default='map')
	# Luigi Task Priority
	priority = TASK_PRIORITY['list-file']

	def filename(self, custom=None, default='data{}.csv'.format('')):
		return super().filename(custom=custom, default=default)

	def output(self):
		""" Return a single `LocalTarget()` wrapping file path. """
		return luigi.LocalTarget(self.path)


class MergeCsv(GenericTask):
	"""
	Merge a list of CSV `LocalTargets()` into one. Set upstream task to `ListDirectory()`.

	:param upstream: Upstream task. `luigi.Task()`.
	:param force: Delete targets first. `bool`. Default `False`.
	:param dirname: Parent directory. Path `str`.
	:param workdir: Working directory. Path `str`.
	:param date: Date. `datetime.date`.
	"""
	workdir = luigi.Parameter(default='staging')
	priority = TASK_PRIORITY['merge-csv']

	# FIXME : Optional field names specification
	def fieldnames(self):
		""" Implement `fieldnames()` to set fixed CSV field names. """
		return []

	def run(self):
		super().run()

		with open(self.input()[0].path, 'r', encoding='utf-8') as first_fin:
			header = first_fin.readline()

		with open(self.output().path, 'w', encoding='utf-8') as fout:
			fout.write(header)
			for file in self.input():
				with open(file.path, 'r', encoding='utf-8') as fin:
					fin.readline()
					for line in fin:
						fout.write(line)


# TODO : Implement (incremental) Load-to-DB by SA or external script.
# TODO : Data monitoring, ID range / max logging, schema hash, DB log.
class Load(GenericWrapperTask):

	priority = TASK_PRIORITY['load']

	def run(self):
		super().run()
