"""The script will dump you ES index to local disk"""
import argparse
import bz2
import json
import logging
import os
import Queue
import threading
import urlparse

import requests as r


class IndiciesToDump(object):
    def __init__(self, indicies=None, excluded_indicies=None, all_indicies=False):
        self.__indicies = indicies if indicies else []
        self.__excluded_indicies = excluded_indicies
        self.__all_indicies = all_indicies

    def get_indicies(self, discovered_indicies):
        indicies = discovered_indicies
        if self.__excluded_indicies:
            indicies = [i for i in indicies if i not in self.__excluded_indicies]

        if self.__all_indicies:
            return indicies
        else:
            indicies = [i for i in indicies if i in self.__indicies]

        return indicies

    def all_indicies(self):
        return self.__all_indicies



class RequestsClient(object):
    """Simple proxy-enables request client"""

    __slots__ = ["__url", "__proxies"]

    def __init__(self, url, proxy=None):
        self.__url = urlparse.urlparse(url).geturl()
        self.__proxies = {"http": proxy, "https": proxy} if proxy else None

    def __get_uri(self, path):
        return urlparse.urljoin(self.__url, path)

    def get(self, path, params=None, body=None):
        uri = self.__get_uri(path)
        resp = r.get(uri, json=body, params=params, proxies=self.__proxies)
        logging.info("GET %s -> [%d]", resp.url, resp.status_code)
        if not resp.ok:
            logging.error("ERROR: GET %s -> [%d]\n---\n%s", resp.url, resp.status_code, resp.text)
            raise EsError("Failed to execute GET %s" % resp.url)
        return resp.json()

    def post(self, path, params=None, body=None):
        uri = self.__get_uri(path)
        resp = r.post(uri, params=params, json=body, proxies=self.__proxies)
        logging.info("POST %s -> [%d]", resp.url, resp.status_code)
        if not resp.ok:
            logging.error("ERROR: POST %s -> [%d]\n---\n%s",
                          resp.url, resp.status_code, resp.text)
            raise EsError("Failed to execute POST %s" % resp.url)
        return resp.json()

    def delete(self, path, params=None, body=None):
        uri = self.__get_uri(path)
        resp = r.delete(uri, params=params, json=body, proxies=self.__proxies)
        logging.info("DELETE %s -> [%d]", resp.url, resp.status_code)
        if not resp.ok:
            logging.error("ERROR: DELETE %s -> [%d]\n---\n%s",
                          resp.url, resp.status_code, resp.text)
            raise EsError("Failed to execute DELETE %s" % resp.url)
        return resp.json()


class EsError(Exception):
    pass


class DumpError(Exception):
    pass


def discover_indicies(client):
    result = client.get("_aliases")
    return [str(i) for i in result.keys()]


def discover_types(client, index):
    result = client.get(index)
    return [str(i) for i in result[index]["mappings"].keys()]


def get_docs(client, index, dtype):
    page_size = 5000
    scroll_ttl = "5m"
    search_path = "%s/%s/_search" % (index, dtype)

    page_req = {"from": 0, "size": page_size}
    page = 0
    resp = client.post(search_path, {"scroll": scroll_ttl}, page_req)
    logging.info("%s / %s / %d docs", index, dtype, resp["hits"]["total"])
    records = []
    new_records = resp["hits"]["hits"]
    scroll_id = resp["_scroll_id"]

    while new_records:
        logging.info("page %d / docs %d", page, len(new_records))
        page += 1
        records += new_records
        scroll_req = {"scroll": scroll_ttl, "scroll_id": scroll_id}
        resp = client.post("_search/scroll", body=scroll_req)
        new_records = resp["hits"]["hits"]

    resp = client.delete("_search/scroll", body={"scroll_id": [scroll_id]})

    return records


def dump_index(client, index, dst):
    # get types in this index
    types = discover_types(client, index)
    logging.info("%s :: %s", index, str(types))
    for dtype in types:
        out_file = "%s/%s_%s.json.bz2" % (dst, index, dtype)

        attempts = 0
        records = []
        while True:
            attempts += 1
            try:
                logging.info("Dumping %s / %s...", index, dtype)
                records = get_docs(client, index, dtype)
                break
            except EsError:
                if attempts >= 3:
                    raise DumpError("ERROR Failed to dump %s/%s" % (index, dtype))
                else:
                    logging.error("ERROR Failed to dump %s/%s (attempt %d/3), retrying...",
                                  index, dtype, attempts)

        logging.info("%s / docs %d", out_file, len(records))
        data = bz2.compress(json.dumps(records))
        with open(out_file, "w+") as out:
            out.write(data)


def worker(index_queue, dst, client):
    while True:
        if index_queue.empty():
            break
        index = index_queue.get()
        try:
            dump_index(client, index, dst)
        except DumpError as err:
            logging.error(err.message)

        index_queue.task_done()
        logging.info("%s complete / %d indicies remain", index, index_queue.qsize())


def main(folder, client, indicies_to_dump, threads=1):
    output_dir = os.path.abspath(folder)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    logging.info("output dir = %s", output_dir)

    indicies = indicies_to_dump.get_indicies(discover_indicies(client))

    logging.info(indicies)

    # queue indicies to dump
    print "Dumping %d indicies..." % len(indicies)
    queue = Queue.Queue()
    for i in indicies:
        queue.put(i)

    for i in range(threads):
        thread = threading.Thread(target=worker, args=(queue, folder, client))
        thread.start()


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="A script that dumps elasticsearch indicies to disk")
    PARSER.add_argument("-u", "--url", dest="es_url", default="http://localhost:9200",
                        help="URL of an elasticsearch node")
    PARSER.add_argument("-p", "--proxy_url", dest="proxy_url", help="http proxy to use")
    PARSER.add_argument("-t", "--theads", dest="num_threads", default=1, type=int,
                        help="number of execution thread to use")
    PARSER.add_argument("-d", "--dst", dest="folder", default="./output", help="destination folder")
    PARSER.add_argument("-x", "--exclude", dest="exclude", default=None,
                        help="comma-seperated list of indicies to exclude")
    PARSER.add_argument("--all", dest="all_indicies", action='store_const', const=True,
                        default=False, help="dump all discovered indicies")
    PARSER.add_argument("-v", dest="verbose", action='store_const', const=True,
                        default=False, help="verbose logging")
    PARSER.add_argument("-vv", dest="very_verbose", action='store_const', const=True,
                        default=False, help="very verbose logging")
    PARSER.add_argument('index', nargs='*', default=[], help='indicies_to_dump')
    ARGS = PARSER.parse_args()

    if ARGS.verbose:
        logging.basicConfig(level=logging.INFO, format='(%(threadName)-10s) %(message)s')
    if ARGS.very_verbose:
        logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s')
    logging.debug(ARGS)

    EXCLUSIONS = str.split(ARGS.exclude, ",") if ARGS.exclude else []

    CLIENT = RequestsClient(ARGS.es_url, ARGS.proxy_url)
    INDICIES = IndiciesToDump(ARGS.index, EXCLUSIONS, ARGS.all_indicies)

    main(ARGS.folder, CLIENT, INDICIES, ARGS.num_threads)
