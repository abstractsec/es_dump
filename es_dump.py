"""The script will dump you ES index to local disk"""
import argparse
import bz2
import json
import logging
import os
import queue
import threading

from utils import DumpError, EsError, RequestsClient

def discover_indices(client):
    result = client.get("_aliases")
    return [str(i) for i in result.keys()]


def discover_types(client, index):
    result = client.get(index)
    return [str(i) for i in result[index]["mappings"].keys()]


def dump_docs(client, index, dtype, file_handle):
    page_size = 5000
    scroll_ttl = "5m"
    search_path = "%s/%s/_search" % (index, dtype)

    page_req = {"from": 0, "size": page_size}
    page = 0
    resp = client.post(search_path, {"scroll": scroll_ttl}, page_req)
    logging.info("%s / %s / %d docs", index, dtype, resp["hits"]["total"])
    new_records = resp["hits"]["hits"]
    scroll_id = resp["_scroll_id"]

    record_cnt = 0
    while new_records:
        record_cnt += len(new_records)
        file_handle.writelines([json.dumps(doc) for doc in new_records])
        logging.info("page %d / docs %d", page, len(new_records))
        page += 1

        scroll_req = {"scroll": scroll_ttl, "scroll_id": scroll_id}
        resp = client.post("_search/scroll", body=scroll_req)
        new_records = resp["hits"]["hits"]

    resp = client.delete("_search/scroll", body={"scroll_id": [scroll_id]})

    return record_cnt


def dump_index(client, index, dst):
    # get types in this index
    types = discover_types(client, index)
    logging.info("%s :: %s", index, types)
    for dtype in types:
        out_file_path = "%s/%s_%s.jsonl.bz2" % (dst, index, dtype)

        attempts = 0
        records = 0
        while True:
            attempts += 1
            try:
                out_file = bz2.BZ2File(out_file_path, "w")
                logging.info("Dumping %s / %s...", index, dtype)
                records = dump_docs(client, index, dtype, out_file)
                break
            except EsError:
                if attempts >= 3:
                    raise DumpError("ERROR Failed to dump %s/%s" % (index, dtype))
                else:
                    logging.error("ERROR Failed to dump %s/%s (attempt %d/3), retrying...",
                                  index, dtype, attempts)
            finally:
                out_file.close()

        logging.info("%s / docs %d", out_file, records)


def worker(index_queue, dst, client):
    while True:
        if index_queue.empty():
            break
        index = index_queue.get()
        try:
            dump_index(client, index, dst)
        except DumpError as err:
            logging.error(err)

        index_queue.task_done()
        logging.info("%s complete / %d indices remain", index, index_queue.qsize())


def get_indices(discovered_indices, requested_indices=None,
                excluded_indicies=None, all_indices=False):
    indices = discovered_indices
    if not all_indices:
        indices = [idx for idx in indices if idx in requested_indices]

    if excluded_indicies:
        for idx in excluded_indicies:
            indices.remove(idx)

    return indices


def main():
    parser = argparse.ArgumentParser(
        description="A script that dumps elasticsearch indices to disk")
    parser.add_argument("-u", "--url", dest="es_url", default="http://localhost:9200",
                        help="URL of an elasticsearch node")
    parser.add_argument("-p", "--proxy_url", dest="proxy_url", help="http proxy to use")
    parser.add_argument("-t", "--theads", dest="num_threads", default=1, type=int,
                        help="number of execution thread to use")
    parser.add_argument("-d", "--dst", dest="folder", default="./output", help="destination folder")
    parser.add_argument("-x", "--exclude", dest="exclude", default=None,
                        help="comma-seperated list of indices to exclude")
    parser.add_argument("--all", dest="all_indices", action='store_const', const=True,
                        default=False, help="dump all discovered indices")
    parser.add_argument("-v", dest="verbose", action='store_const', const=True,
                        default=False, help="verbose logging")
    parser.add_argument("-vv", dest="very_verbose", action='store_const', const=True,
                        default=False, help="very verbose logging")
    parser.add_argument('index', nargs='*', default=[], help='indices_to_dump')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='(%(threadName)-10s) %(message)s')
    if args.very_verbose:
        logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s')
    logging.debug(args)

    # get indicies to dump
    exclusions = str.split(args.exclude, ",") if args.exclude else []
    client = RequestsClient(args.es_url, args.proxy_url)
    indices = get_indices(discover_indices(client), args.index, exclusions, args.all_indices)
    logging.info(indices)

    # setup output dir
    output_dir = os.path.abspath(args.folder)
    os.makedirs(output_dir, exist_ok=True)
    logging.info("output dir = %s", output_dir)

    # queue indices to dump
    print("Dumping %d indices..." % len(indices))
    idx_queue = queue.Queue()
    for i in indices:
        idx_queue.put(i)

    for i in range(args.num_threads):
        thread = threading.Thread(target=worker, args=(idx_queue, output_dir, client))
        thread.start()

if __name__ == "__main__":
    main()
