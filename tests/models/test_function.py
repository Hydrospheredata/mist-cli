from unittest import TestCase

from mist import models


class FunctionTest(TestCase):
    def test_function_create(self):
        fn = models.Function('test', 'Test', 'test-context', 'test-path')

        self.assertEqual(fn.name, 'test')
        self.assertEqual(fn.class_name, 'Test')
        self.assertIsInstance(fn.default_context, models.Context)
        self.assertEqual(fn.default_context.name, 'test-context')
        self.assertEqual(fn.path, 'test-path')

        fn = models.Function('test')
        self.assertIsNotNone(fn.default_context)
        self.assertEqual(fn.default_context.name, 'default')

    def test_to_json(self):
        fn = models.Function('test', 'Test', 'test-context', 'test-path')
        res = fn.to_json()
        self.assertTrue('name' in res)
        self.assertTrue('defaultContext' in res)
        self.assertTrue('path' in res)
        self.assertTrue('className' in res)

    def test_from_json(self):
        fn = models.Function.from_json(dict(
            name='test',
            path='test-path',
            className='Test',
            defaultContext='foo'
        ))

        self.assertEqual(fn.name, 'test')
        self.assertIsNotNone(fn.default_context)
        self.assertEqual(fn.path, 'test-path')
        self.assertEqual(fn.class_name, 'Test')
        self.assertEqual(fn.default_context.name, 'foo')

        defaults_check = models.Function.from_json(dict(
            name='test',
            path='test-path',
            className='Test',
        ))
        self.assertIsNotNone(defaults_check.default_context)
        self.assertEqual(defaults_check.default_context.name, 'default')

    def test_to_row(self):
        row = models.Function.to_row(models.Function('test', 'Test', path='test-path'))
        self.assertListEqual(row, ['test', 'default', 'test-path', 'Test'])

    def test_header(self):
        self.assertListEqual(models.Function.header, ['FUNCTION', 'DEFAULT CONTEXT', 'PATH', 'CLASS NAME'])
