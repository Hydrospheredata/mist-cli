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
