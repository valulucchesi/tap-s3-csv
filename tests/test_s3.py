import os
import unittest

import requests_mock
import simplejson

import mock
import tap_s3_csv

from unittest.mock import Mock
from collections import OrderedDict

from tap_s3_csv.s3 import sample_files, get_sampled_schema_for_table, list_files_in_bucket


class TestS3(unittest.TestCase):

    def load_file(self, filename, path):
        myDir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(myDir, path, filename)
        with open(path) as file:
            return simplejson.load(file)

    def load_orderedDict(self, filename):
        myDir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(myDir, "data_test", filename)
        result = []
        with open(path) as file:
            for line in file:
                result.append(eval(line))
            return result


    def test_sample_schema_for_table(self):
        value = self.load_file("dict_merged.json", "data_test")
        table_spec = self.load_file("table_spec_without_key.json", "data_test")
        config = self.load_file("config-file.json", "data_test")
        samples = self.load_orderedDict("samples.json")
        tap_s3_csv.s3.sample_files = Mock(return_value=samples)
        tap_s3_csv.s3.merge_dicts = Mock(return_value=[])
        resp = get_sampled_schema_for_table(config, table_spec)
        self.assertEqual(value, resp)

    def test_list_files_in_bucket(self):
        bucket = 'bucket-name'
        page = {'Contents':[{'key':'value'}]}
        with mock.patch('boto3.client') as m:
            m.get_paginator = Mock()
            with mock.patch('singer.get_logger') as patching:
                for list in list_files_in_bucket(bucket, "regex"):
                    patching.assert_called_with('Found no files for bucket "%s" that match prefix "%s"', bucket, "regex")

    def test_sample_file(self):
        config =  self.load_file("config-file.json", "data_test")
        s3_files = [{'key':'value'}]
        table_input = self.load_file("table_spec_without_key.json", "data_test")
        tap_s3_csv.s3.sample_file = Mock(return_value=[])
        with mock.patch('singer.get_logger') as patching:
            for sample in sample_files(config, table_input, s3_files):
                patching.assert_called_with("Sampling files (max files: %s)", 2)
                patching.assert_called_with("Sampling %s (max records: %s, sample rate: %s)", "value", 1000, 2)








