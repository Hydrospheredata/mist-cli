import json
import os
from unittest import TestCase
import unittest
import requests_mock
from mock import MagicMock
from pyhocon.exceptions import ConfigException
from requests.exceptions import HTTPError

from mist import models
from mist.app import MistApp, BadConfigException, DeployFailedException


@requests_mock.Mocker()
class MistAppTest(TestCase):
    MIST_APP_URL = 'http://localhost:2004/v2/api/'

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, 'test_config.conf')
        with open(file_path, 'w+') as f:
            f.write("""
            mist {
                contexts {
                    test-context {
                        worker-mode = shared
                        max-parallel-jobs = 20
                        downtime = Inf
                        precreated = false
                        streaming-duration = 1s
                        spark-conf {
                            spark.default.parallelism = 128
                            spark.driver.memory = "512m"
                            spark.executor.memory = "256m"
                            spark.scheduler.mode = "FAIR"
                        }
                        run-options = ""
                    }

                }
                endpoints {
                    test-class {
                        class-name = "SimpleContext"
                        context = "test-context" 
                    }
                }
            }            
            """)
        self.test_config_path = file_path

        job_path = os.path.join(dir_path, 'test-job.py')
        with open(job_path, 'w+') as f:
            f.write('print "Python job!"')

        self.test_job_path = job_path

    def tearDown(self):
        os.remove(self.test_config_path)
        os.remove(self.test_job_path)

    def test_mist_app_creation(self, m):
        mist = MistApp()
        self.assertEqual(mist.host, 'localhost')
        self.assertEqual(mist.port, 2004)
        self.assertEqual(mist.accept_all, False)
        self.assertEqual(mist.format_table, False)

    def test_endpoints(self, m):
        mist = MistApp()
        m.register_uri('GET', self.MIST_APP_URL + 'endpoints',
                       text='[{"name": "test", "path": "test-path", "defaultContext": "foo", "className": "Test$"}]')
        res = mist.endpoints()
        self.assertEqual(len(res), 1)
        item = res[0]
        self.assertEqual(item.name, 'test')
        self.assertEqual(item.path, 'test-path')

    def test_workers(self, m):
        mist = MistApp()
        m.register_uri('GET', self.MIST_APP_URL + 'workers',
                       text='[{"name": "test", "address": "localhost:0", "sparkUi": "foo"}]')
        res = mist.workers()
        self.assertEqual(len(res), 1)
        item = res[0]
        self.assertEqual(item.name, 'test')
        self.assertEqual(item.address, 'localhost:0')
        self.assertEqual(item.spark_ui, 'foo')

    def test_jobs(self, m):
        mist = MistApp()
        m.register_uri('GET', self.MIST_APP_URL + 'jobs?status=started&status=finished',
                       text="""[
                            {
                                "jobId": "test", 
                                "endpoint": "foo", 
                                "context": "bar", 
                                "source": "http", 
                                "status": "started"
                            },
                            {
                                "jobId": "test2", 
                                "endpoint": "foo2", 
                                "context": "bar2", 
                                "source": "http", 
                                "status": "finished"
                            }
                        ]""".strip()
                       )
        jobs = mist.jobs('started,finished')
        self.assertEqual(len(jobs), 2)
        item = jobs[0]
        self.assertEqual(item.job_id, 'test')
        self.assertEqual(item.status, 'started')
        self.assertEqual(jobs[1].job_id, 'test2')
        self.assertEqual(jobs[1].status, 'finished')

    def test_contexts(self, m):
        mist = MistApp()
        m.register_uri('GET', self.MIST_APP_URL + 'contexts',
                       text='[{"name": "foo", "workerMode": "shared"}]')
        res = mist.contexts()
        print(res)
        self.assertEqual(len(res), 1)
        item = res[0]
        self.assertEqual(item.name, 'foo')
        self.assertEqual(item.worker_mode, 'shared')

    def test_cancel_job(self, m):
        mist = MistApp()
        m.register_uri('DELETE', self.MIST_APP_URL + 'jobs/some-id-with-dash',
                       text='Cancelled')
        mist.cancel_job('some-id-with-dash')

    def test_kill_worker(self, m):
        mist = MistApp()
        m.register_uri('DELETE', self.MIST_APP_URL + 'workers/some-id-with-dash',
                       text='Cancelled')
        mist.kill_worker('some-id-with-dash')

    def test_dev_deploy(self, m):
        test_context = models.Context('foo')
        test_endpoint = models.Endpoint('test', 'Test', test_context)
        inst = MistApp()
        inst.job_path = 'test-path.py'
        inst.deploy_endpoint = MagicMock(return_value=test_endpoint)
        inst.deploy_context = MagicMock(return_value=test_context)
        inst.upload_job = MagicMock(return_value='test-path_0_0_1.py')
        inst.dev_deploy([test_endpoint], [test_context], 'baz', '0.0.1')

        endpoint_args = inst.deploy_endpoint.call_args
        context_args = inst.deploy_context.call_args
        endpoint = endpoint_args[0][0]
        context = context_args[0][0]
        self.assertEqual(endpoint.name, 'baz_test_0_0_1')
        self.assertEqual(endpoint.class_name, 'Test')
        self.assertEqual(endpoint.default_context.name, 'baz_foo_0_0_1')
        self.assertEqual(endpoint.path, 'test-path_0_0_1.py')
        self.assertEqual(context.name, 'baz_foo_0_0_1')
        inst.upload_job.assert_called_with('test-path_baz_0_0_1.py')

    def test_deploy(self, m):
        test_context = models.Context('foo')
        test_endpoint = models.Endpoint('test', 'Test', test_context, 'test-path.py')
        inst = MistApp()
        inst.job_path = 'test-path.py'
        inst.deploy_endpoint = MagicMock(return_value=test_endpoint)
        inst.deploy_context = MagicMock(return_value=test_context)
        inst.upload_job = MagicMock(return_value='test-path_0_0_1.py')
        inst.deploy([test_endpoint], [test_context], '0.0.1')

        endpoint_args = inst.deploy_endpoint.call_args
        context_args = inst.deploy_context.call_args
        endpoint = endpoint_args[0][0]
        context = context_args[0][0]
        self.assertEqual(endpoint.name, 'test')
        self.assertEqual(endpoint.class_name, 'Test')
        self.assertEqual(endpoint.default_context.name, 'foo')
        self.assertEqual(endpoint.path, 'test-path_0_0_1.py')
        self.assertEqual(context.name, 'foo')
        inst.upload_job.assert_called_with('test-path_0_0_1.py')

    def test_deploy_with_undeployed_contexts(self, m):
        test_context = models.Context('foo')
        test_endpoint = models.Endpoint('test', 'Test', test_context, 'test-path.py')
        inst = MistApp()
        inst.job_path = 'test-path.py'
        inst.deploy_endpoint = MagicMock(return_value=test_endpoint)
        inst.deploy_context = MagicMock(side_effect=HTTPError('failed to deploy context'))
        inst.upload_job = MagicMock(return_value='test-path_0_0_1.py')
        self.assertRaises(DeployFailedException, lambda: inst.deploy([test_endpoint], [test_context], '0.0.1'))
        self.assertTrue(inst.deploy_context.called)
        self.assertFalse(inst.deploy_endpoint.called)
        inst.upload_job.assert_called_with('test-path_0_0_1.py')

    def test_parse_config(self, m):
        mist = MistApp()
        mist.endpoint_parser.parse = MagicMock(return_value=models.Endpoint('test'))
        mist.context_parser.parse = MagicMock(return_value=models.Context('context'))
        endpoints, contexts = mist.parse_config(self.test_config_path)
        self.assertEqual(len(endpoints), 1)
        self.assertEqual(len(contexts), 1)

    def test_parse_with_errors(self, m):
        mist = MistApp()
        mist.endpoint_parser.parse = MagicMock(side_effect=ConfigException('failed'))
        mist.context_parser.parse = MagicMock(side_effect=ConfigException('failed'))
        try:
            mist.parse_config(self.test_config_path)
        except BadConfigException as e:
            self.assertEqual(len(e.errors), 2)

    def test_upload_job(self, m):
        m.register_uri('POST', self.MIST_APP_URL + 'artifacts', text='my-custom-name.py')
        mist = MistApp(job_path=self.test_job_path)
        result = mist.upload_job('my-custom-name.py')
        self.assertEqual(result, 'my-custom-name.py')

    def test_deploy_endpoint(self, m):
        m.register_uri('POST', self.MIST_APP_URL + 'endpoints', text="""
        {
            "name": "test-endpoint",
            "className": "Test",
            "path": "test-path.py",
            "defaultContext": "foo" 
        }
        """)
        endpoint = models.Endpoint('test-endpoint', 'Test', 'foo', 'test-path.py')
        mist = MistApp()
        res = mist.deploy_endpoint(endpoint)
        self.assertIsInstance(res, models.Endpoint)
        self.assertEqual(res.name, 'test-endpoint')
        self.assertEqual(res.class_name, 'Test')
        self.assertEqual(res.path, 'test-path.py')
        self.assertEqual(res.default_context.name, 'foo')

    def test_deploy_context(self, m):
        m.register_uri('POST', self.MIST_APP_URL + 'contexts', text="""
        {
            "name": "test-ctx",
            "workerMode": "shared"
        }
        """)
        ctx = models.Context('foo', worker_mode='shared')
        mist = MistApp()
        res = mist.deploy_context(ctx)
        self.assertIsInstance(res, models.Context)
        self.assertEqual(res.name, 'test-ctx')
        self.assertEqual(res.worker_mode, 'shared')

    def test_start_job(self, m):
        json_str = '{"errors": [], "payload": {"result": [1, 2, 3]}, "success": true}'
        m.register_uri('POST', self.MIST_APP_URL + 'endpoints/simple/jobs',
                       text=json_str)
        mist = MistApp()
        res = mist.start_job('simple', '{"numbers": [1,2,3], "multiplier": 4}')
        self.assertEqual(res, json.loads(json_str))
