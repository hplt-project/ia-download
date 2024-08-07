# Internet Archive Downloader
Downloads collections from the [Internet Archive](https://archive.org/).

## Installation

```bash
pip install internetarchive
ia configure
```

## Fully automated downloading with restarts on failures
The easiest way to download a crawl from Internet Archive is running [ia-download.sh](ia-download.sh):
```bash
bash ia-download.sh <CRAWL> <DOWNLOAD_DIR> <LOG_DIR> <NTHREADS> <CREDENTIALS_FILE>
```
downloads the given CRAWL to DOWNLOAD_DIR/CRAWL in NTHREADS parallel threads, making as many attempts as required to download all files successfully. Logs are written to LOG_DIR/CRAWL. User credentials are taken from CREDENTIALS_FILE.

E.g.:

```bash
bash ia-download.sh survey_00003 ../../../two/warc/ia/ ../../../two/warc/log/ia/ 1000 ./ia-env/ia-oe\@ifi.uio.no.ini
```

## Downloading step-by-step
For more control over the downloading process, e.g. when the previous method does not work, run the following steps manually: get the list of items for the given crawl (ia search), then get file URLs and download files for those items (ia-download.py). Check stderr, if downloading some of the files failed re-run ia-download.py.

```bash
ia search -i collection:wide00016 > wide00016.txt

./ia-download.py -j 64 < ./wide00016.txt | tee ia-download.log
```

Options:

```
> ./ia-download.py --help
usage: ia-download.py [-h] [--jobs JOBS] [--dest DEST] [--shuffle] [--filter FILTER] [identifiers ...]

positional arguments:
  identifiers           IA identifiers to download warcs from. If none specified read from stdin

optional arguments:
  -h, --help            show this help message and exit
  --jobs JOBS, -j JOBS  parallel downloads
  --dest DEST, -d DEST  destination directory
  --shuffle             download items in random order
  --filter FILTER       filename filter
```



## Acknowledgements

This project has received funding from the 🇪🇺 European Union’s Horizon Europe research and innovation programme under grant agreement No 101070350 and from UK Research and Innovation (UKRI) under the UK government’s Horizon Europe funding guarantee [grant number 100525A46].
