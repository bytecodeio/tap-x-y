import json

import singer
from singer import Transformer, metadata
from singer.utils import strftime, strptime_to_utc
import sys
from tap_x_y.streams import AVAILABLE_STREAMS
from tap_x_y.client import XYClient
from tap_x_y.catalog import generate_catalog


LOGGER = singer.get_logger()


def discover(client):
    LOGGER.info('Starting Discovery..')
    streams = [
        stream_class(client) for _, stream_class in AVAILABLE_STREAMS.items()
    ]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)


def sync(client, config, catalog, state):
    LOGGER.info('Starting Sync..')
    selected_streams = catalog.get_selected_streams(state)

    streams = []
    stream_keys = []
    with Transformer() as transformer:
        for catalog_entry in selected_streams:
            streams.append(catalog_entry)
            stream_keys.append(catalog_entry.stream)

        for catalog_entry in streams:
            stream = AVAILABLE_STREAMS[catalog_entry.stream](client=client,
                                                             config=config,
                                                             catalog=catalog,
                                                             state=state)
            LOGGER.info('Syncing stream: %s', catalog_entry.stream)

            stream.write_state()

            bookmark_date = stream.get_bookmark(stream.name,
                                                config['start_date'])
            bookmark_dttm = strptime_to_utc(bookmark_date)

            stream_schema = catalog_entry.schema.to_dict()
            stream.write_schema()
            stream_metadata = metadata.to_map(catalog_entry.metadata)
            max_bookmark_value = None
            with singer.metrics.job_timer(job_type=stream.name) as timer:
                with singer.metrics.record_counter(
                        endpoint=stream.name) as counter:
                    for page in stream.sync(catalog_entry.metadata):
                        for record in page:
                            singer.write_record(
                                catalog_entry.stream,
                                transformer.transform(
                                    record,
                                    stream_schema,
                                    stream_metadata,
                                ))
                            counter.increment()
                        stream.update_bookmark(stream.name,
                                                max_bookmark_value)
                        stream.write_state()

        stream.write_state()
        LOGGER.info('Finished Sync..')


def main():
    parsed_args = singer.utils.parse_args(required_config_keys=[
        'token'
    ])
    config = parsed_args.config

    client = XYClient(config)

    if parsed_args.discover:
        discover(client=client)
    elif parsed_args.catalog:
        sync(client, config, parsed_args.catalog, parsed_args.state)


if __name__ == '__main__':
    main()
