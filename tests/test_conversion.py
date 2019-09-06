import os
import unittest

import simplejson

from tap_s3_csv.conversion import infer, count_sample, pick_datatype, generate_schema


class ConversionTestCase(unittest.TestCase):

    def load_file(self, filename, path):
        myDir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(myDir, path, filename)
        with open(path) as file:
            return simplejson.load(file)

    def test_infer(self):
        input_integer = '233692469'
        output_i = infer(input_integer)
        self.assertEqual(output_i, "integer")
        self.assertIsNot(output_i, "string")
        self.assertIsNot(output_i, "number")
        self.assertIsNot(output_i, None)
        input_string = 'test'
        output_s = infer(input_string)
        self.assertEqual(output_s, "string")
        self.assertIsNot(output_s, "integer")
        self.assertIsNot(output_s, "number")
        self.assertIsNot(output_s, None)
        input_empty = ''
        output_e = infer(input_empty)
        self.assertEqual(output_e, None)
        self.assertIsNot(output_e, "integer")
        self.assertIsNot(output_e, "number")
        self.assertIsNot(output_e, "string")

    def test_count_sample(self):
        count = {"InvoiceID": {"integer": 1}, "PayerAccountId": {"integer": 1}, "LinkedAccountId": {}, "RecordType": {"string": 1}}
        table_input = self.load_file("table_spec_without_key.json", "data_test")
        sample = self.load_file("sample.json", "data_test")
        output = '{"InvoiceID": {"integer": 2}, "PayerAccountId": {"integer": 2}, "LinkedAccountId": {}, "RecordType": {"string": 2}, "RecordID": {"integer": 1}, "BillingPeriodStartDate": {"string": 1}, "BillingPeriodEndDate": {"string": 1}, "InvoiceDate": {"string": 1}, "PayerAccountName": {"string": 1}, "LinkedAccountName": {}, "TaxationAddress": {}, "PayerPONumber": {}, "ProductCode": {"string": 1}, "ProductName": {"string": 1}, "SellerOfRecord": {"string": 1}, "UsageType": {"string": 1}, "Operation": {"string": 1}, "RateId": {"integer": 1}, "ItemDescription": {"string": 1}, "UsageStartDate": {"string": 1}, "UsageEndDate": {"string": 1}, "UsageQuantity": {"number": 1}, "BlendedRate": {}, "CurrencyCode": {"string": 1}, "CostBeforeTax": {"number": 1}, "Credits": {"number": 1}, "TaxAmount": {"number": 1}, "TaxType": {"string": 1}, "TotalCost": {"number": 1}}'
        output_wrong = '{"InvoiceID": {"integer": 3}, "PayerAccountId": {"integer": 3}, "LinkedAccountId": {}, "RecordType": {"string": 3}, "RecordID": {"integer": 2}, "BillingPeriodStartDate": {"string": 2}, "BillingPeriodEndDate": {"string": 2}, "InvoiceDate": {"string": 2}, "PayerAccountName": {"string": 2}, "LinkedAccountName": {}, "TaxationAddress": {}, "PayerPONumber": {}, "ProductCode": {"string": 2}, "ProductName": {"string": 2}, "SellerOfRecord": {"string": 2}, "UsageType": {"string": 2}, "Operation": {"string": 2}, "RateId": {"integer": 2}, "ItemDescription": {"string": 2}, "UsageStartDate": {"string": 2}, "UsageEndDate": {"string": 2}, "UsageQuantity": {"number": 2}, "BlendedRate": {}, "CurrencyCode": {"string": 2}, "CostBeforeTax": {"number": 2}, "Credits": {"number": 1}, "TaxAmount": {"number": 2}, "TaxType": {"string": 2}, "TotalCost": {"number": 2}}'
        result = count_sample(sample, count, table_input)
        self.assertEqual(output, simplejson.dumps(result))
        self.assertIsNot(None, simplejson.dumps(result))
        self.assertIsNot(output_wrong, simplejson.dumps(result))

    def test_pick_datatype(self):
        counts = {'integer': 445, 'string': 8}
        result = pick_datatype(counts)
        self.assertEqual(result, 'string')
        self.assertIsNot(result, 'date-time')
        self.assertIsNot(result, 'integer')
        self.assertIsNot(result, None)
        counts_date = {'date-time': 1}
        result_date = pick_datatype(counts_date)
        self.assertEqual(result_date, 'date-time')
        self.assertIsNot(result_date, 'string')
        self.assertIsNot(result_date, 'integer')
        self.assertIsNot(result_date, None)

    def test_generate_schema(self):
        samples = self.load_file("sample.json", "data_test")
        table_input = self.load_file("table_spec_without_key.json", "data_test")
        output = self.load_file("data_schema.json", "data_test")
        result = generate_schema([samples], table_input)
        self.assertEqual(simplejson.dumps(output), simplejson.dumps(result))