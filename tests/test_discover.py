import os
import unittest

# Our test case class
import mock
import simplejson
from tap_s3_csv.s3 import get_input_files_for_table, list_files_in_bucket, get_sampled_schema_for_table, merge_dicts

from tap_s3_csv.discover import discover_schema, load_metadata

from tap_s3_csv import discover_streams, validate_table_config


class DiscoverTestCase(unittest.TestCase):

    def load_file(self, filename, path):
        myDir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(myDir, path, filename)
        with open(path) as file:
            return simplejson.load(file)


    def test_discover_streams(self):
        config = self.load_file("config-file.json", "data_test")
        config['tables'] = validate_table_config(config)
        schema = self.load_file("schema_input.json", "data_test")
        streams = self.load_file("stream_expected.json", "data_test")
        with mock.patch("tap_s3_csv.discover.discover_schema", return_value=schema):
            resp = discover_streams(config)
            self.assertEqual(simplejson.dumps(resp), simplejson.dumps(streams))
            config_preprocess = self.load_file("config-file-preprocess.json", "data_test")
            config_preprocess['tables'] = validate_table_config(config_preprocess)
            streams_preprocess = self.load_file("stream_expected_preprocess.json", "data_test")
            resp_preprocess = discover_streams(config_preprocess)
            self.assertEqual(simplejson.dumps(resp_preprocess), simplejson.dumps(streams_preprocess))
    '''
    def test_merge_dicts(self):
        first = self.load_file("first.json", "data_test")
        second = self.load_file("second.json", "data_test")
        output = self.load_file("dict_merged.json", "data_test")
        resp = merge_dicts(first, second)
        self.assertEqual(resp, output)
    '''

    def test_load_metadata(self):
        table = self.load_file("table_spec_without_key.json", "data_test")
        schema = self.load_file("schema_input.json", "data_test")
        output = load_metadata(table, schema)
        value = self.load_file("load_metadata_output.json", "data_test")
        self.assertEqual(simplejson.dumps(value), simplejson.dumps(output))
        table_key = self.load_file("table_spec_with_key.json", "data_test")
        output_key = load_metadata(table_key, schema)
        value_key = self.load_file("load_metadata_output_with_key.json", "data_test")
        self.assertEqual(simplejson.dumps(value_key), simplejson.dumps(output_key))

if __name__ == '__main__':
    unittest.main()
