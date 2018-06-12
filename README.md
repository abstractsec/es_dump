# Elasticsearch dump util

### Install
```
git clone git@github.com:abstractsec/es_dump.git
cd es_dump
pip install -r requirements.txt
```

## es_dump.py
Used to dump elastic search data to compressed json files (list of docs) with one file per type per index
```
usage: es_dump.py [-h] [-u ES_URL] [-p PROXY_URL] [-t NUM_THREADS] [-d FOLDER]
                  [-x EXCLUDE] [--all] [-v] [-vv]
                  [index [index ...]]

A script that dumps elasticsearch indicies to disk

positional arguments:
  index                 indicies_to_dump

optional arguments:
  -h, --help            show this help message and exit
  -u ES_URL, --url ES_URL
                        URL of an elasticsearch node (default: http://localhost:9200/)
  -p PROXY_URL, --proxy_url PROXY_URL
                        http proxy to use
  -t NUM_THREADS, --theads NUM_THREADS
                        number of execution thread to use (default: 1)
  -d FOLDER, --dst FOLDER
                        destination folder (default: ./output/)
  -x EXCLUDE, --exclude EXCLUDE
                        comma-seperated list of indicies to exclude
  --all                 dump all discovered indicies
  -v                    verbose logging
  -vv                   very verbose logging
```

## Tips
get completed indicies 
```
#!/bin/bash
completed_indicies="$(echo $(ls ./output | cut -d_ -f1,2 | sort | uniq) | tr '[:blank:]' ',')"
```
