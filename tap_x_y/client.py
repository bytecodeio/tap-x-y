import urllib
from pprint import pprint

import backoff
import requests
import singer
from urllib3.exceptions import ProtocolError

BACKOFF_MAX_TRIES = 10
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

    def build_url(self, baseurl, path, space_uri, api_user, args_dict):
        # Returns a list in the structure of urlparse.ParseResult
        # https://developer.xyretail.com/_g/spaces-identity.toddsnyder/worksheet/sheetTemplate/commerce.salesorderline-9518699780000999/
        # customV4/aaron.pugliese@bytecode.io
        url_parts = list(urllib.parse.urlparse(baseurl))
        url_parts[2] = '_g/' + space_uri +'/worksheet/sheetTemplate/' + path + '/customV4/' + api_user
        url_parts[4] = urllib.parse.urlencode(args_dict)
        return urllib.parse.urlunparse(url_parts)

    def get_resources(self, path, space_uri, api_user, filter_param=None):
        page_from = 0

        args = {'size': PAGE_SIZE, 'from': page_from}
        if filter_param:
            args = {**args, **filter_param}

        next = self.build_url(BASE_URL, path, space_uri, api_user, args)

        rows_in_response = 1
        while rows_in_response > 0:
            response = self.make_request(method='GET', url=next)
            data = (response.get('rows'))
            rows_in_response = len(data)
            page_from += PAGE_SIZE
            args['from'] = page_from
            next = self.build_url(BASE_URL, path, space_uri, api_user, args)
            yield data
        yield []

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
        except (ConnectionError, ProtocolError) as ex:
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
