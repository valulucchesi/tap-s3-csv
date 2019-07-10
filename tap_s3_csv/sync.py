import io
import sys
import csv
import zipfile
from datetime import timedelta

from singer import metadata
from singer import Transformer
from singer import utils

import singer
from singer_encodings import csv as singer_encodings_csv
from tap_s3_csv import s3
import json

LOGGER = singer.get_logger()

def sync_stream(config, state, table_spec, stream):
    table_name = table_spec['table_name']
    modified_since = utils.strptime_with_tz(singer.get_bookmark(state, table_name, 'modified_since') or
                                            config['start_date'])
    modified_since += timedelta(days=1)
    LOGGER.info('Syncing table "%s".', table_name)
    LOGGER.info('Getting files modified since %s.', modified_since)

    s3_files = s3.get_input_files_for_table(
        config, table_spec, modified_since)

    records_streamed = 0

    # We sort here so that tracking the modified_since bookmark makes
    # sense. This means that we can't sync s3 buckets that are larger than
    # we can sort in memory which is suboptimal. If we could bookmark
    # based on anything else then we could just sync files as we see them.
    for s3_file in sorted(s3_files, key=lambda item: item['last_modified']):
        records_streamed += sync_table_file(
            config, s3_file['key'], table_spec, stream, s3_file['last_modified'])

        state = singer.write_bookmark(state, table_name, 'modified_since', s3_file['last_modified'].isoformat())
        singer.write_state(state)

    LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    return records_streamed

def sync_table_file(config, s3_path, table_spec, stream, modified):
    LOGGER.info('Syncing file "%s".', s3_path)

    bucket = config['bucket']
    table_name = table_spec['table_name']

    s3_file_handle = s3.get_file_handle(config, s3_path)
    # We observed data who's field size exceeded the default maximum of
    # 131072. We believe the primary consequence of the following setting
    # is that a malformed, wide CSV would potentially parse into a single
    # large field rather than giving this error, but we also think the
    # chances of that are very small and at any rate the source data would
    # need to be fixed. The other consequence of this could be larger
    # memory consumption but that's acceptable as well.
    csv.field_size_limit(sys.maxsize)
    longitud = 0
    if s3_path.endswith('zip'):
        with io.BytesIO(s3_file_handle.read()) as tf:
                if tf is not None:
                    tf.seek(0)

                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode='r') as zipf:
                    for subfile in zipf.namelist():
                            with zipf.open(subfile) as myfile:
                                iterator = singer_encodings_csv.get_row_iterator(myfile, table_spec)
                                rows = list(iterator)
                                longitud = len(rows)

    else:
        iterator = singer_encodings_csv.get_row_iterator(
            s3_file_handle._raw_stream, table_spec) #pylint:disable=protected-access
        rows = list(iterator)
        longitud = len(rows)



    records_synced = 0
    current_row = 0
    i=0
    for row in rows:

        custom_columns = {
            s3.SDC_SOURCE_BUCKET_COLUMN: bucket,
            s3.SDC_SOURCE_FILE_COLUMN: s3_path,

            # index zero, +1 for header row
            s3.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))
            if "preprocess" in config and config['preprocess'] != '':
                preprocess_items = json.loads(config['preprocess'])
                for i in preprocess_items:
                    preprocess = i
                    if(table_name == preprocess['table_name']):
                        for value in preprocess['values']:
                            to_get = value.split("|")[0]
                            to_del = value.split("|")[1]
                            if to_get in rec:
                                if to_del in rec:
                                    if rec[to_get] == rec[to_del]:
                                        if to_del in to_write:
                                            del to_write[to_del]
                                    else:
                                        LOGGER.warning('removing record: ' + json.dumps(rec) + ' ' + to_get + ' and ' + to_del + ' are not equals')

                            elif to_del in rec:
                                to_write[to_get] = rec[to_del]
                                if to_del in to_write:
                                    del to_write[to_del]
                            else:
                                to_write[to_get] = ""

        to_write['last_modified'] = modified.__str__()
        singer.write_record(table_name, to_write)
        records_synced += 1
        current_row += 1
        if (i == longitud):
            continue

    return records_synced
