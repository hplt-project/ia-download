# Internet Archive Downloader
Downloads collections from the Internet Archive.

Usage:

```bash
pip install internetarchive
ia configure
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
