#!/usr/bin/env python3
import argparse
import sys
import os
import hashlib
import csv
import internetarchive as ia
import random
import dbm
import pickle
import time
from contextlib import ExitStack
from datetime import datetime
from typing import NamedTuple, List, Tuple, Union, Optional, Callable, TypeVar, Iterable
from multiprocessing import Pool
from itertools import repeat
from requests.exceptions import ConnectionError


class DownloadError(RuntimeError):
	pass


class File(NamedTuple):
	name: str
	url: str
	md5: str


class Download(NamedTuple):
	path: str
	size: int
	md5: str
	time: int


In = TypeVar('In')
Out = TypeVar('Out')

class FakePool:
	"""multiprocessing.Pool but running in this thread. Useful for getting
	stack traces from exception occurring in the callback."""
	def imap_unordered(self, fn:Callable[[In],Out], iterable:Iterable[In]) -> Iterable[Out]:
		for item in iterable:
			yield fn(item)


def download_file(session, file:File, file_path:str, *, timeout=60) -> Tuple[Download,int]:
	# Construct temporary filename in same directory
	dest_dir, file_name = os.path.split(file_path)
	temp_path = os.path.join(dest_dir, f'.{file_name}~{os.getpid()}')

	start_time = datetime.now()

	# Fetch header
	response = session.get(file.url, stream=True, timeout=timeout, auth=ia.auth.S3Auth(session.access_key, session.secret_key))
	if not response.ok:
		print(f"ERROR: non-ok server response for {file.url}: {response.reason}", file=sys.stderr)
		raise DownloadError(response.reason)

	# Fetch body, calculate checksum as we read through its chunks
	digest = hashlib.md5()
	size = 0
	try:
		with open(temp_path, 'wb') as fout:
			for chunk in response.iter_content(chunk_size=1048576):
				size += fout.write(chunk)
				digest.update(chunk)

		if digest.hexdigest() != file.md5:
			print(f"ERROR: md5 mismatch when downloading {file.url}", file=sys.stderr)
			raise DownloadError('md5 mismatch')

		# Download finished and no errors! Move file to its permanent destination.
		os.rename(temp_path, file_path)
	finally:
		# Clean up tempfile in case of error
		# TODO: Resume download if we implement Partial or chunked download.
		if os.path.exists(temp_path):
			os.unlink(temp_path)

	return Download(file_path, size, digest.hexdigest(), (datetime.now() - start_time).seconds)


def compute_md5(path:str, *, buffering=2**16) -> str:
	with open(path, 'rb', buffering=buffering) as fh:
		digest = hashlib.md5()
		while True:
			chunk = fh.read(buffering)
			if len(chunk) == 0:
				break
			digest.update(chunk)
		return digest.hexdigest()


def worker_setup():
	global session
	session = ia.api.get_session()
	session.mount_http_adapter(max_retries=2)


def worker_download_file(entry: Tuple[Tuple[str, File],str,bool]) -> Tuple[str,File,Union[Tuple[Download,int],Exception]]:
	global session
	(item, file), dest_dir, check_md5 = entry
	item_path = os.path.join(dest_dir, item)
	file_path = os.path.join(item_path, file.name)
	retval = None
	
	# If we find the wrong md5, delete the file.
	if check_md5 and os.path.exists(file_path):
		file_md5 = compute_md5(file_path)
		if file_md5 != file.md5:
			print(f"md5 mismatch: {file_path}\t{file_md5}\t{file.md5}", file=sys.stderr)
			os.unlink(file_path)

	if not os.path.exists(file_path):
		try:
			os.makedirs(item_path, exist_ok=True)
			retval = download_file(session, file, file_path)
		except Exception as err:
			retval = err
	
	return item, file, retval


def ia_get_files(cache, session, item:str, *, glob_pattern:Optional[str]=None) -> List[File]:
	key = f"{item}${glob_pattern!s}"
	if cache is not None and key in cache:
		return pickle.loads(cache[key])
	
	response = None
	for retry in range(1, 6):
		try:
			response = session.get_item(item).get_files(glob_pattern=args.filter)
			break
		except ConnectionError as err:
			if retry < 5:
				print(f"Waiting for {4**retry}s because: {err}", file=sys.stderr)
				time.sleep(4 ** retry) # back-off
			else:
				raise
	
	files = [File(file.name, file.url, file.md5) for file in response]
	
	if cache is not None:
		cache[key] = pickle.dumps(files)

	return files


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--jobs', '-j', type=int, default=os.cpu_count(), help='parallel downloads')
	parser.add_argument('--dest', '-d', type=str, default='.', help='destination directory')
	parser.add_argument('--shuffle', action='store_true', help='download items in random order')
	parser.add_argument('--filter', default='*.warc.gz', help='filename filter')
	parser.add_argument('--cache', type=str, help='IA api call cache')
	parser.add_argument('--check-md5', action='store_true', help='check md5 is file already exists')
	parser.add_argument('identifiers', type=str, nargs='*', help='IA identifiers to download warcs from. If none specified read from stdin')

	args = parser.parse_args()

	session = ia.api.get_session()

	if not args.identifiers:
		args.identifiers = (line.rstrip('\n') for line in sys.stdin)

	if args.shuffle:
		args.identifiers = list(args.identifiers)
		random.shuffle(args.identifiers)

	with ExitStack() as ctx:
		cache = ctx.enter_context(dbm.open(args.cache, 'c', mode=0o600)) if args.cache else None

		files = (
			(item, file)
			for item in args.identifiers
			for file in ia_get_files(cache, session, item, glob_pattern=args.filter)
		)

		total_errors = 0

		# Keep a counter of how often downloads fail. If it keeps happening, stop
		# because we might just making things worse.
		consecutive_errors = 0

		if args.jobs > 1:
			pool = ctx.enter_context(Pool(args.jobs, initializer=worker_setup))
		else:
			pool = FakePool()

		out = csv.DictWriter(sys.stdout, ['timestamp', 'item', 'name', 'path', 'size', 'time', 'md5', 'error'], delimiter='\t')

		for item, file, retval in pool.imap_unordered(worker_download_file, zip(files, repeat(args.dest), repeat(args.check_md5))):
			if retval is None:
				continue
			elif isinstance(retval, Download):
				out.writerow({
					'timestamp': datetime.now().isoformat(),
					'item': item,
					'name': file.name,
					'path': retval.path,
					'size': retval.size,
					'time': retval.time,
					'md5': retval.md5,
					
				})
				consecutive_errors = 0
			else:
				out.writerow({
					'timestamp': datetime.now().isoformat(),
					'item': item,
					'name': file.name,
					'error': str(retval)
				})
				consecutive_errors += 1
				total_errors += 1

			if consecutive_errors > 100:
				raise RuntimeError('More than a 100 consecutive errors')

	sys.exit(1 if total_errors > 0 else 0)
