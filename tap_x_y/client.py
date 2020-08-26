import urllib
from pprint import pprint

import backoff
import requests
import singer

BACKOFF_MAX_TRIES = 2
BACKOFF_FACTOR = 2
BASE_URL = "https://developer.xyretail.com"
LOGGER = singer.get_logger()  # noqa
PAGE_SIZE = 100


class Server5xxError(Exception):
    pass


class Server42xRateLimitError(Exception):
    pass


def lookup_backoff_max_tries():
    return BACKOFF_MAX_TRIES


def lookup_backoff_factor():
    return BACKOFF_FACTOR


class XYClient:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.access_token = config.get('token')

    def build_url(self, baseurl, path, args_dict):
        # Returns a list in the structure of urlparse.ParseResult
        url_parts = list(urllib.parse.urlparse(baseurl))
        url_parts[2] = '_g' + '/' + path
        url_parts[4] = urllib.parse.urlencode(args_dict)
        return urllib.parse.urlunparse(url_parts)

    def get_resources(self, path, filter_param=None):
        page_from = 0
        total = 1

        args = {'size': PAGE_SIZE, 'from': page_from}
        if filter_param:
            args = {**args, **filter_param}

        next = self.build_url(BASE_URL, path, args)

        rows_in_response = 1
        while rows_in_response > 0:
            response = self.make_request(method='GET', url=next)
            total = response.get('total')
            data = (response.get('rows'))
            rows_in_response = len(data)
            page_from += PAGE_SIZE
            args['from'] = page_from
            next = self.build_url(BASE_URL, path, args)
            yield data

    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, ConnectionError, Server42xRateLimitError),
        max_tries=lookup_backoff_max_tries,
        factor=lookup_backoff_factor)
    def make_request(self,
                     method,
                     url=None,
                     params=None,
                     data=None,
                     stream=False):

        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}

        if self.config.get('user_agent'):
            headers['User-Agent'] = self.config['user_agent']

        try:
            if method == "GET":
                LOGGER.info(
                    f"Making {method} request to {url} with params: {params}")
                response = self.session.get(url, headers=headers)
            else:
                raise Exception("Unsupported HTTP method")
        except ConnectionError as ex:
            LOGGER.info("Retrying on connection error {}".format(ex))
            raise ConnectionError

        LOGGER.info("Received code: {}".format(response.status_code))

        if response.status_code >= 500:
            LOGGER.info(f"")
            raise Server5xxError()

        result = []
        try:
            result = response.json()
        except ConnectionError:
            pprint("Response json parse failed: {}".format(response))

        return result
