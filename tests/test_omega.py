import unittest
from unittest import mock

from omega.cli import update_config, config_sync


class MyTestCase(unittest.TestCase):
    def test_update_config(self):
        conf = {
            "host": '127.0.0.1',
            'port': 6379
        }

        update_config("redis.dsn", "redis://127.0.0.1:6379")

        config = [{
            'name':       'jqdatasdk',
            'module':     'jqadaptor',
            'parameters': {
                'account':  'account',
                'password': 'password'
            }
        }]
        update_config('quotes_fetchers', config)

    def test_config_sync(self):
        inputs = [
            '1000', '900', '800', '700', '600', '500', '400', '300', '200', '15:06'
        ]

        expected_frames = {
            '1d':  1000,
            '1w':  900,
            '1M':  800,
            '1y':  700,
            '1m':  600,
            '5m':  500,
            '15m': 400,
            '30m': 300,
            '60m': 200
        }
        with mock.patch('builtins.input', side_effect=inputs):
            frames, sync_time = config_sync()
            self.assertDictEqual(expected_frames, frames)
            self.assertEqual('15:06', sync_time)

        inputs = [
            '1000', 'C', '', '700', '600', '500', '400', '300', '200', '25:01', '15:00'
        ]

        expected_frames = {
            '1d':  1000,
            '1M':  1000,
            '1y':  700,
            '1m':  600,
            '5m':  500,
            '15m': 400,
            '30m': 300,
            '60m': 200
        }

        with mock.patch('builtins.input', side_effect=inputs):
            frames, sync_time = config_sync()
            self.assertDictEqual(expected_frames, frames)
            self.assertEqual('15:00', sync_time)

if __name__ == '__main__':
    unittest.main()
