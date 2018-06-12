import logging
from urllib import parse as urlparse

import requests as r

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
