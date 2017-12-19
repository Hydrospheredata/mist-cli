from unittest import TestCase

from pyhocon import ConfigTree

from mist.models import Deployment

class DeploymentTest(TestCase):

    def test_create_deployment(self):
        Deployment('test', 'Artifact', ConfigTree(), '0.0.1')

    def test_get_name(self):
        d = Deployment('test', 'Artifact', ConfigTree({
            'file-path': 'test-name.py'
        }), '0.0.1')
        self.assertEqual(d.get_name(), 'test_0_0_1.py')

    def test_with_user_name(self):
        d = Deployment('test', 'Function', ConfigTree({
            'context': 'foo',
            'path': 'test-name.jar'
        }), '0.0.1')
        d.with_user('test_name')
        self.assertEqual(d.name, 'test_name_test')
        self.assertEqual(d.data['path'], 'test_name_test-name.jar')
        self.assertEqual(d.data['context'], 'test_name_foo')

