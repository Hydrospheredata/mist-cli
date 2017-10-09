from unittest import TestCase

from app import models


class EndpointTest(TestCase):
    def test_endpoint_create(self):
        endpoint = models.Endpoint('test', 'Test', 'test-context', 'test-path')

        self.assertEqual(endpoint.name, 'test')
        self.assertEqual(endpoint.class_name, 'Test')
        self.assertIsInstance(endpoint.default_context, models.Context)
        self.assertEqual(endpoint.default_context.name, 'test-context')
        self.assertEqual(endpoint.path, 'test-path')

        endpoint = models.Endpoint('test')
        self.assertIsNotNone(endpoint.default_context)
        self.assertEqual(endpoint.default_context.name, 'default')

    def test_to_json(self):
        endpoint = models.Endpoint('test', 'Test', 'test-context', 'test-path')
        res = endpoint.to_json()
        self.assertTrue('name' in res)
        self.assertTrue('defaultContext' in res)
        self.assertTrue('path' in res)
        self.assertTrue('className' in res)

    def test_from_json(self):
        endpoint = models.Endpoint.from_json(dict(
            name='test',
            path='test-path',
            className='Test',
            defaultContext='foo'
        ))

        self.assertEqual(endpoint.name, 'test')
        self.assertIsNotNone(endpoint.default_context)
        self.assertEqual(endpoint.path, 'test-path')
        self.assertEqual(endpoint.class_name, 'Test')
        self.assertEqual(endpoint.default_context.name, 'foo')

        defaults_check = models.Endpoint.from_json(dict(
            name='test',
            path='test-path',
            className='Test',
        ))
        self.assertIsNotNone(defaults_check.default_context)
        self.assertEqual(defaults_check.default_context.name, 'default')

    def test_to_row(self):
        row = models.Endpoint.to_row(models.Endpoint('test', 'Test', path='test-path'))
        self.assertListEqual(row, ['test', 'default', 'test-path', 'Test'])

    def test_header(self):
        self.assertListEqual(models.Endpoint.header, ['ROUTE', 'DEFAULT CONTEXT', 'PATH', 'CLASS NAME'])
