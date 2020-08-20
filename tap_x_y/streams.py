import os
from datetime import datetime, timedelta

import humps
import singer
import singer.metrics
from singer import Transformer, metadata, metrics, utils
from singer.utils import strptime_to_utc

LOGGER = singer.get_logger()
DEFAULT_ATTRIBUTION_WINDOW = 90

class Base:
    # Todo: add lookback as attribution window
    def __init__(self, client=None, config=None, catalog=None, state=None):
        self.client = client
        self.config = config
        self.catalog = catalog
        self.state = state
        self.top = 50
        self.date_window_size = 1
        self.size = 1000

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
                self.bookmark_field + '.filter': int(date.timestamp()) * 1000
            }
        return self.client.get_resources(self.get_endpoint(), filter_param)

    def get_resources(self):
        return self.client.get_resources(self.get_endpoint())



    def remove_hours_local(self, dttm):
        new_dttm = dttm.replace(hour=0, minute=0, second=0, microsecond=0)
        return new_dttm


    # Round time based to day
    def round_times(self, start=None, end=None):
        start_rounded = None
        end_rounded = None
        # Round min_start, max_end to hours or dates
        start_rounded = self.remove_hours_local(start) - timedelta(days=1)
        end_rounded = self.remove_hours_local(end) + timedelta(days=1)
        return start_rounded, end_rounded


    # Determine absolute start and end times w/ attribution_window constraint
    # abs_start/end and window_start/end must be rounded to nearest hour or day (granularity)
    def get_absolute_start_end_time(self, last_dttm, attribution_window):
        now_dttm = utils.now()
        delta_days = (now_dttm - last_dttm).days
        if delta_days < attribution_window:
            start = now_dttm - timedelta(days=attribution_window)
        elif delta_days > 89:
            start = now_dttm - timedelta(88)
            LOGGER.info(
                (f'Start date with attribution window exceeds max API history.'
                f'Setting start date to {start}')
            )
        else:
            start = last_dttm

        abs_start, abs_end = self.round_times(start, now_dttm)
        return abs_start, abs_end

    def sync(self, mdata):
        schema = self.load_schema()

        with singer.metrics.job_timer(job_type=self.name) as timer:
            with singer.metrics.record_counter(endpoint=self.name) as counter:

                if self.replication_key:

                    # Bookmark datetimes
                    last_datetime = str(self.get_bookmark(
                        self.name, self.config.get('start_date')))
                    last_dttm = strptime_to_utc(last_datetime)

                    # Get absolute start and end times
                    attribution_window = int(
                        self.config.get("attribution_window", DEFAULT_ATTRIBUTION_WINDOW))
                    abs_start, abs_end = self.get_absolute_start_end_time(last_dttm,
                                                                    attribution_window)

                    window_start = abs_start

                    while window_start <= abs_end:
                        result = self.get_resources_by_date(window_start)
                        window_start = window_start + timedelta(
                            days=self.date_window_size)
                        yield result
                
                else:
                    yield self.get_resources()


class CommerceSalesOrderline(Base):
    name = 'sales_order_line'
    key_properties = ['order', 'item']
    replication_method = 'INCREMENTAL'
    replication_key = 'order_date'
    bookmark_field = 'orderDate'
    endpoint = 'commerce.salesorderline-{sales_order_line}'
    valid_replication_keys = ['order_date']
    uri_root = 'commerce'
    uri_root_path = 'store'

    def get_endpoint(self):
        return self.endpoint.format(
            sales_order_line=self.config.get('sales_order_line'))


class Customer(Base):
    name = 'customer'
    key_properties = ['email']
    replication_method = 'INCREMENTAL'
    replication_key = 'last_txn_date'
    bookmark_field = 'lastTxnDate'
    endpoint = '{customer}'
    valid_replication_keys = ['last_txn_date']

    def get_endpoint(self):
        return self.endpoint.format(customer=self.config.get('customer'))


class Inventory(Base):
    name = 'inventory'
    key_properties = ['item']
    replication_method = 'FULL_TABLE'
    endpoint = 'commerce.inventory-{inventory}'
    replication_key = None
    valid_replication_keys = ['']
    # Todo: add extract date field
    def get_endpoint(self):
        return self.endpoint.format(inventory=self.config.get('inventory'))


class Invoice(Base):
    name = 'invoice'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    replication_key = 'order_date'
    bookmark_field = 'orderDate'
    endpoint = '{invoice}'
    valid_replication_keys = ['order_date']

    def get_endpoint(self):
        return self.endpoint.format(invoice=self.config.get('invoice'))


class InventoryMovement(Base):
    name = 'inventory_movement'
    key_properties = ['item', 'record_type', 'date']
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    bookmark_field = 'date'
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
    key_properties = ['item', 'stocktransfer']
    replication_method = 'INCREMENTAL'
    replication_key = 'date'
    bookmark_field = 'date'
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
