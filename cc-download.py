#!/usr/bin/env python3
import argparse
import csv
import gzip
import hashlib
import os
import shutil
import sys
import time
	
from datetime import datetime
from multiprocessing.pool import Pool
from typing import Optional, NamedTuple
from urllib.request import urlopen, Request
from http.client import HTTPResponse
from typing import cast, Optional, Union, List


BUFSIZE=2**16

CC_HOST='http://data.commoncrawl.org'

MAX_ATTEMPTS = 10


class FileDownloaded(NamedTuple):
	path: str
	size: int
	time: int
	md5: str


class FileExists(NamedTuple):
	path: str


class DownloadError(NamedTuple):
	path: str
	size: Optional[int]
	time: int
	error: str


DownloadResult = Union[FileDownloaded, FileExists, DownloadError]


def get_warc_list(crawl:str) -> List[str]:
	with urlopen(f'{CC_HOST}/crawl-data/{crawl}/warc.paths.gz') as fz, gzip.open(fz, 'rt') as fh:
		return list(line.rstrip() for line in fh)


def get_content_length(response: HTTPResponse) -> int:
	"""Get whole content length from either a normal or a Range request."""
	content_range = response.getheader('Content-Range', '').split('/')
	if len(content_range) == 2 and content_range[1] != '*':
		return int(content_range[1])

	size = response.getheader('Content-Length')
	if size is not None:
		return int(size)

	raise ValueError('No content size')


def download_warc(path:str) -> DownloadResult:
	file_name = os.path.basename(path)
	temp_name = f'.{file_name}'
	size: Optional[int] = None
	start_time = datetime.now()

	if os.path.exists(file_name):
		return FileExists(path)

	try:
		# Open the temp file (it may already exist from a previously interrupted session)
		with open(temp_name, 'a+b') as ftemp:
			# Restart the md5 hash and read ftemp to end, after which we'll append
			ftemp.seek(0)
			digest = hashlib.md5()
			while True:
				chunk = ftemp.read(BUFSIZE)
				if len(chunk) == 0:
					break
				digest.update(chunk)

			# Attempt downloading, resuming based on how much is already on disk
			attempt = 0
			while attempt < MAX_ATTEMPTS:
				attempt += 1
				
				request = Request(f'{CC_HOST}/{path}', headers={
					'Range': f'bytes={ftemp.tell()}-'
				})

				with urlopen(request) as fin:
					# Get the expected full content length (throws if not available)
					size = get_content_length(cast(HTTPResponse, fin))

					# Read downloaded bytes, writing them to the digest & temp file
					# until there's nothing left to read.
					while True:
						chunk = fin.read(BUFSIZE)
						if len(chunk) == 0:
							break
						ftemp.write(chunk)
						digest.update(chunk)

				# If we haven't finished the whole promised file yet, re-attempt
				if ftemp.tell() < size:
					continue

				# If we're somehow past our expected size, something went wrong
				elif ftemp.tell() > size:
					raise Exception(f'Downloaded too much: {ftemp.tell()} > {size}')

				# Otherwise, make temp file permanent
				os.rename(temp_name, file_name)
				return FileDownloaded(
					path=path,
					size=size,
					md5=digest.hexdigest(),
					time=(datetime.now() - start_time).seconds)
			
			# If we ran through all attempts of the loop without ever returning
			# FileDownloaded or throwing an exception: sad.
			raise Exception(f'Downloaded not enough: {ftemp.tell()} < {size}')
	except Exception as err:
		return DownloadError(
					path=path,
					size=size,
					time=(datetime.now() - start_time).seconds,
					error=str(err))


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--jobs', '-j', type=int, default=os.cpu_count(), help='parallel downloads')
	parser.add_argument('crawl', type=str)
	args = parser.parse_args()

	out = csv.DictWriter(sys.stdout, ['timestamp', 'item', 'name', 'path', 'size', 'time', 'md5', 'error'], delimiter='\t')

	with Pool(args.jobs) as pool:
		for result in pool.imap_unordered(download_warc, get_warc_list(args.crawl)):
			if isinstance(result, FileDownloaded):
				out.writerow({
					'timestamp': datetime.now().isoformat(),
					'item': os.path.basename(result.path),
					'name': os.path.basename(result.path),
					'path': result.path,
					'size': result.size,
					'time': result.time,
					'md5':  result.md5,
				})
			elif isinstance(result, DownloadError):
				out.writerow({	
					'timestamp': datetime.now().isoformat(),
					'item': os.path.basename(result.path),
					'name': os.path.basename(result.path),
					'error': result.error,
				})
