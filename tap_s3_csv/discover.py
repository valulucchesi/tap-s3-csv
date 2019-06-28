from singer import metadata
from tap_s3_csv import s3
import json

def discover_streams(config):
    streams = []

    for table_spec in config['tables']:
        schema = discover_schema(config, table_spec)
        if "preprocess" in config:
            preprocess = json.loads(config['preprocess'])
            if (table_spec['table_name'] == preprocess['table_name']):
                for value in preprocess['values']:
                    to_get = value.split("|")[0]
                    to_del = value.split("|")[1]
                    if to_del in schema['properties']:
                        del schema['properties'][to_del]
                    if to_get not in schema['properties']:
                        schema['properties'][to_get] = {'type':['null', 'string']}
        streams.append({'stream': table_spec['table_name'], 'tap_stream_id': table_spec['table_name'], 'schema': schema, 'metadata': load_metadata(table_spec, schema)})

    return streams

def discover_schema(config, table_spec):
    sampled_schema = s3.get_sampled_schema_for_table(config, table_spec)
    return sampled_schema

def load_metadata(table_spec, schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', table_spec['key_properties'])

    for field_name in schema.get('properties', {}).keys():
        if table_spec.get('key_properties', []) and field_name in table_spec.get('key_properties', []):
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)
