from unittest import TestCase

from app import models


class MockNamedConfig(models.NamedConfig):
    @staticmethod
    def from_json(data):
        pass


class NamedConfigTest(TestCase):
    def test_with_dev(self):
        instance = MockNamedConfig('test')

        instance.with_dev('test')
        self.assertEqual(instance.name, 'test_test')

    def test_with_version(self):
        instance = MockNamedConfig('test')

        instance.with_version('0.0.1')
        self.assertEqual(instance.name, 'test_0_0_1')

    def test_to_json(self):
        instance = MockNamedConfig('test')
        res = instance.to_json()
        self.assertEqual(res['name'], 'test')
