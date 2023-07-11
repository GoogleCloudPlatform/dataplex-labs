import unittest
from unittest import mock

import api_call_utils
import requests
from tests.test_utils import mocks


class ApiCallUtilsTest(unittest.TestCase):

  def test_error_message_if_no_token(self):
    with self.assertRaises(TypeError):
      api_call_utils.fetch_api_response(requests.get, '123')

  def test_return_no_error_msg_and_response(self):
    with mock.patch(
        'api_call_utils.requests.get',
        side_effect=mocks.mocked_get_api_response):
      response = api_call_utils.fetch_api_response(
          requests.get,
          'https://datacatalog.googleapis.com/v2/get_call/success',
          '12345'
      )
      self.assertIsNone(response['error_msg'])
      self.assertIsNotNone(response['json'])

  def test_return_error_msg_and_response(self):
    with mock.patch(
        'api_call_utils.requests.get',
        side_effect=mocks.mocked_get_api_response):
      response = api_call_utils.fetch_api_response(
          requests.get,
          'https://datacatalog.googleapis.com/v2/get_call/error',
          '12345'
      )
      self.assertIsNone(response['json'])
      self.assertIsNotNone(response['error_msg'])

  def test_return_error_msg_when_exception(self):
    with mock.patch(
        'api_call_utils.requests.get',
        side_effect=mocks.mocked_get_api_response):
      response = api_call_utils.fetch_api_response(
          requests.get,
          'https://datacatalog.googleapis.com/v2/get_call/error_with_exception',
          '12345')
      self.assertEqual(
        response['error_msg'],
        (
            'GET call to https://datacatalog.googleapis.com/v2'
            '/get_call/error_with_exception returned non valid'
            ' JSON.'
        )
      )
      self.assertIsNone(response['json'])


if __name__ == '__main__':
  unittest.main()
