import os
from datetime import datetime, timedelta

import humps
import singer
import singer.metrics
from singer import Transformer, metadata, metrics, utils
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()


class Base:
    def __init__(self, client=None, config=None, catalog=None, state=None):
        self.client = client
        self.config = config
        self.catalog = catalog
        self.state = state
        self.top = 50
        self.date_window_size = 1
        self.size = 100

    @staticmethod
    def get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        # pylint: disable=no-member
        return singer.utils.load_json('{}/{}.json'.format(
            schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        # pylint: disable=no-member
        return singer.write_schema(stream_name=self.name,
                                   schema=schema,
                                   key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def update_bookmark(self, stream, value):
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        self.state['bookmarks'][stream] = value
        LOGGER.info('Stream: {} - Write state, bookmark value: {}'.format(
            stream, value))
        self.write_state()

    def get_bookmark(self, stream, default):
        # default only populated on initial sync
        if (self.state is None) or ('bookmarks' not in self.state):
            return default
        return self.state.get('bookmarks', {}).get(stream, default)

    # Returns max key and date time for all replication key data in record
    def max_from_replication_dates(self, record):
        date_times = {
            dt: strptime_to_utc(record[dt])
            for dt in self.valid_replication_keys if record[dt] is not None
        }
        max_key = max(date_times)
        return date_times[max_key]

    def get_resources_by_date(self, date):
        if self.replication_key:
            filter_param = {
                self.replication_key + '.filter': int(date.timestamp()) * 1000
            }
        return self.client.get_resources(self.get_endpoint(), filter_param)

    def get_resources(self):
        return self.client.get_resources(self.get_endpoint())

    def sync(self, mdata):
        schema = self.load_schema()

        with singer.metrics.job_timer(job_type=self.name) as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                if self.replication_key:

                    bookmark_date = self.get_bookmark(
                        self.name, self.config.get('start_date'))
                    today = utils.now()

                    date_window_start = strptime_to_utc(bookmark_date)

                    data = []
                    while date_window_start <= today:
                        result = self.get_resources_by_date(date_window_start)
                        date_window_start = date_window_start + timedelta(
                            days=self.date_window_size)
                        data.extend(result)
                    yield data
                
                else:
                    yield self.get_resources()


class CommerceSalesOrderline(Base):
    name = 'commerce_salesorderline'
    key_properties = ['order']
    replication_method = 'INCREMENTAL'
    replication_key = 'orderDate'
    endpoint = 'commerce.salesorderline-{salesorderline}'
    valid_replication_keys = ['orderDate']

    def get_endpoint(self):
        return self.endpoint.format(
            salesorderline=self.config.get('salesorderline'))


class Customer(Base):
    name = 'customer'
    key_properties = ['email']
    replication_method = 'INCREMENTAL'
    replication_key = 'lastTxnDate'
    endpoint = '{customer}'
    valid_replication_keys = ['lastTxnDate']

    def get_endpoint(self):
        return self.endpoint.format(customer=self.config.get('customer'))


class Inventory(Base):
    name = 'inventory'
    key_properties = ['item']
    replication_method = 'FULL_TABLE'
    endpoint = 'commerce.inventory-{inventory}'
    replication_key = None
    valid_replication_keys = ['']

    def get_endpoint(self):
        return self.endpoint.format(inventory=self.config.get('inventory'))


class Invoice(Base):
    name = 'invoice'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'orderDate'
    endpoint = '{invoice}'
    valid_replication_keys = ['orderDate']

    def get_endpoint(self):
        return self.endpoint.format(invoice=self.config.get('invoice'))


class InventoryMovement(Base):
    name = 'inventory_movement'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    endpoint = '{inventory_movement}'
    valid_replication_keys = ['date']

    def get_endpoint(self):
        return self.endpoint.format(
            inventory_movement=self.config.get('inventory_movement'))

class Item(Base):
    name = 'item'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    endpoint = 'commerce.item-{item}'
    replication_key = None
    valid_replication_keys = ['']

    def get_endpoint(self):
        return self.endpoint.format(item=self.config.get('item'))


class StockTransfer(Base):
    name = 'stock_transfer'
    key_properties = ['item']
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    endpoint = 'commerce.stocktransferline-{stock_transfer}'
    valid_replication_keys = ['date']

    def get_endpoint(self):
        return self.endpoint.format(
            stock_transfer=self.config.get('stock_transfer'))


AVAILABLE_STREAMS = {
    "commerce_salesorderline": CommerceSalesOrderline,
    "customer": Customer,
    "inventory": Inventory,
    "invoice": Invoice,
    "inventory_movement": InventoryMovement,
    "item": Item,
    "stock_transfer": StockTransfer
}
