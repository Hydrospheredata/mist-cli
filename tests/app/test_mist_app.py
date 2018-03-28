import json
import os
from unittest import TestCase

import requests_mock
from mock import MagicMock
from pyhocon import ConfigTree

from mist import models
from mist.app import MistApp


@requests_mock.Mocker()
class MistAppTest(TestCase):
    MIST_APP_URL = 'http://localhost:2004/v2/api/'

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        apply_config = os.path.join(dir_path, '00test_apply.conf')
        with open(apply_config, 'w+') as f:
            f.write("""
            model = Artifact
            name = test-artifact
            version = 0.0.1
            data.file-path = "test-path.py"
            """)
        self.apply_artifact_config = apply_config

        apply_config2 = os.path.join(dir_path, '00test_apply2.conf')
        with open(apply_config2, 'w+') as f:
            f.write("""
            model = Artifact
            version = 0.0.1
            data.file-path = "test-path.py"
            """)
        self.apply_artifact_config2 = apply_config2
        job_path = os.path.join(dir_path, 'test-job.py')
        with open(job_path, 'w+') as f:
            f.write('print "Python job!"')

        self.test_job_path = job_path

    def tearDown(self):
        os.remove(self.test_job_path)
        os.remove(self.apply_artifact_config)
        os.remove(self.apply_artifact_config2)

    def test_mist_app_creation(self, m):
        mist = MistApp()
        self.assertEqual(mist.host, 'localhost')
        self.assertEqual(mist.port, 2004)
        self.assertEqual(mist.accept_all, False)
        self.assertEqual(mist.format_table, False)

    def test_functions(self, m):
        mist = MistApp()
        m.register_uri('GET', self.MIST_APP_URL + 'functions',
                       text='[{"name": "test", "path": "test-path", "defaultContext": "foo", "className": "Test$"}]')
        res = mist.functions()
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
                                "function": "foo", 
                                "context": "bar", 
                                "source": "http", 
                                "status": "started"
                            },
                            {
                                "jobId": "test2", 
                                "function": "foo2", 
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

    def test_deploy_function_exists(self, m):
        m.register_uri('PUT', self.MIST_APP_URL + 'functions', text="""
        {
            "name": "test-fn",
            "className": "Test",
            "path": "test-path.py",
            "defaultContext": "foo" 
        }
        """)
        # returns something by with addr
        m.register_uri('GET', self.MIST_APP_URL + 'functions/test-fn', text="""
        {
            "name": "test-fn",
            "className": "Test",
            "path": "test-path.py",
            "defaultContext": "foo" 
        }
        """)

        fn = models.Function('test-fn', 'Test', 'foo', 'test-path.py')
        mist = MistApp()
        res = mist.update_function(fn)
        self.assertIsInstance(res, models.Function)
        self.assertEqual(res.name, 'test-fn')
        self.assertEqual(res.class_name, 'Test')
        self.assertEqual(res.path, 'test-path.py')
        self.assertEqual(res.default_context.name, 'foo')

    def test_deploy_function_non_existent(self, m):
        m.register_uri('POST', self.MIST_APP_URL + 'functions', text="""
        {
            "name": "test-fn",
            "className": "Test",
            "path": "test-path.py",
            "defaultContext": "foo" 
        }
        """)
        # returns something by with addr
        m.register_uri('GET', self.MIST_APP_URL + 'functions/test-fn', status_code=404)
        fn = models.Function('test-fn', 'Test', 'foo', 'test-path.py')
        mist = MistApp()
        res = mist.update_function(fn)
        self.assertIsInstance(res, models.Function)
        self.assertEqual(res.name, 'test-fn')
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
        res = mist.update_context(ctx)
        self.assertIsInstance(res, models.Context)
        self.assertEqual(res.name, 'test-ctx')
        self.assertEqual(res.worker_mode, 'shared')

    def test_start_job(self, m):
        json_str = '{"errors": [], "payload": {"result": [1, 2, 3]}, "success": true}'
        m.register_uri('POST', self.MIST_APP_URL + 'functions/simple/jobs',
                       text=json_str)
        mist = MistApp()
        res = mist.start_job('simple', '{"numbers": [1,2,3], "multiplier": 4}')
        self.assertEqual(res, json.loads(json_str))

    def test_get_fn(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'functions/simple', text="""
        {
            "name": "simple",
            "className": "Test",
            "path": "test-path.py",
            "defaultContext": "foo"
        }
        """)

        mist = MistApp()
        res = mist.get_function('simple')
        self.assertIsInstance(res, models.Function)
        self.assertEqual(res.name, 'simple')
        self.assertEqual(res.class_name, 'Test')
        self.assertEqual(res.path, 'test-path.py')
        self.assertEqual(res.default_context.name, 'foo')

    def test_get_full_fn(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'functions/simple', text="""
            {
              "name": "simple",
              "execute": {
                "numbers": {
                  "type": "MList",
                  "args": [
                    {
                      "type": "MInt",
                      "args": []
                    }
                  ]
                },
                "multiplier": {
                  "type": "MOption",
                  "args": [
                    {
                      "type": "MInt",
                      "args": []
                    }
                  ]
                }
              },
              "path": "my-awesome-job_0_0_1.jar",
              "tags": [],
              "className": "SimpleContext$",
              "defaultContext": "simple",
              "lang": "scala"
            }
        """)
        mist = MistApp()
        res = mist.get_function_json('simple')
        self.assertIsInstance(res, dict)
        self.assertIn('execute', res)

    def test_get_context(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'contexts/simple', text="""
        {
          "name": "simple",
          "maxJobs": 20,
          "workerMode": "shared",
          "precreated": false,
          "sparkConf": {
            "spark.executor.memory": "256m",
            "spark.driver.memory": "512m"
          },
          "runOptions": "",
          "downtime": "Inf",
          "streamingDuration": "1s"
        }
        """)

        mist = MistApp()
        res = mist.get_context('simple')
        self.assertIsInstance(res, models.Context)
        self.assertEqual(res.name, 'simple')

    def test_get_sha1(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'artifacts/my-artifact.py/sha', text="SOME_CONTENT")
        m.register_uri('GET', self.MIST_APP_URL + 'artifacts/unknown/sha', status_code=404)
        mist = MistApp()
        sha1 = mist.get_sha1('my-artifact.py')
        self.assertIsNotNone(sha1)
        res = mist.get_sha1('unknown')
        self.assertIsNone(res)

    def test_parse_deployment_w_name_defined(self, m):
        mist = MistApp()
        priority, res = mist.parse_deployment(self.apply_artifact_config)
        self.assertIsInstance(res, models.Deployment)
        self.assertEqual(res.name, 'test-artifact')
        self.assertEqual(res.version, '0.0.1')
        self.assertEqual(res.model_type, 'Artifact')

    def test_parse_deployment_wo_name(self, m):
        mist = MistApp()
        priority, res = mist.parse_deployment(self.apply_artifact_config2)
        self.assertIsInstance(res, models.Deployment)
        self.assertEqual(res.name, 'app')
        self.assertEqual(res.version, '0.0.1')
        self.assertEqual(res.model_type, 'Artifact')

    def test_update_deployment(self, m):
        mist = MistApp()
        artifact = models.Artifact('test-artifact.py', 'test-artifact.py')
        context = models.Context('test-context')
        fn = models.Function('test-fn', 'Test', 'test-context', 'test-path.py')
        mist.artifact_parser.parse = MagicMock(return_value=artifact)
        mist.context_parser.parse = MagicMock(return_value=context)
        mist.function_parser.parse = MagicMock(return_value=fn)
        mist._MistApp__upload_artifact = MagicMock(return_value=artifact)
        mist.update_context = MagicMock(return_value=context)
        mist.update_function = MagicMock(return_value=fn)
        mist._validate_artifact = MagicMock(return_value=None)
        mist._validate_function = MagicMock(return_value=None)
        mist._validate_context = MagicMock(return_value=None)

        mist.update(models.Deployment('test-artifact.py', 'Artifact', ConfigTree(**{'file-path': 'test-path.py'})))
        mist.update(models.Deployment('test-context', 'Context', ConfigTree()))
        mist.update(models.Deployment('test-fn', 'Function', ConfigTree()))
        call_artifact = mist._MistApp__upload_artifact.call_args[0][0]
        call_fn = mist.update_function.call_args[0][0]
        call_context = mist.update_context.call_args[0][0]
        self.assertEqual(call_artifact.name, 'test-artifact.py')
        self.assertEqual(call_context.name, 'test-context')
        self.assertEqual(call_fn.name, 'test-fn')

    def test_update_deployments_should_catch_exceptions(self, m):
        mist = MistApp(validate=False)

        context = models.Context('test-context')
        fn = models.Function('test-fn', 'Test', 'test-context', 'test-path.py')
        mist.update_function = MagicMock(return_value=fn)
        mist.update_context = MagicMock(return_value=context)
        mist.context_parser.parse = MagicMock(return_value=context)
        mist.function_parser.parse = MagicMock(return_value=fn)

        depls = [
            models.Deployment('simple', 'Function', ConfigTree()),
            models.Deployment('simple-ctx', 'Context', ConfigTree())
        ]

        mist.update_deployments(depls)

    def test_validate_methods(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'artifacts/test-name.jar/sha', text="SOME_CONTENT")
        m.register_uri('GET', self.MIST_APP_URL + 'artifacts/unknown.jar/sha', status_code=404)
        m.register_uri('GET', self.MIST_APP_URL + 'contexts/test-ctx', text="""
        {
          "name": "simple",
          "maxJobs": 20,
          "workerMode": "shared",
          "precreated": false,
          "sparkConf": {
            "spark.executor.memory": "256m",
            "spark.driver.memory": "512m"
          },
          "runOptions": "",
          "downtime": "Inf",
          "streamingDuration": "1s"
        }
        """)
        m.register_uri('GET', self.MIST_APP_URL + 'contexts/unknown-ctx', status_code=404)
        mist = MistApp()
        with self.assertRaises(ValueError):
            mist._validate_artifact(models.Artifact('test-name', 'test-name.jar'))
        mist._validate_artifact(models.Artifact('unknown', 'test-name.jar'))
        mist._validate_context(models.Context('test'))
        fn1 = models.Function('test', 'Test', 'test-ctx', path='test-name.jar')
        mist._validate_function(fn1)
        fn2 = models.Function('test', 'Test', 'unknown-ctx', path='unknown.jar')
        with self.assertRaises(ValueError):
            mist._validate_function(fn2)
        fn3 = models.Function('test', 'Test', 'test-ctx', path='unknown.jar')
        with self.assertRaises(ValueError):
            mist._validate_function(fn3)

    def test_get_status_return_smth(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'status', text="""
        {
          "mistVersion": "1.2.3"
          "sparkVersion": "1.2.3"
        }
        """)
        mist = MistApp()
        status = mist.get_status()
        self.assertIsNotNone(status, 'It should return json object when status code is 200')

    def test_get_status_return_empty_dict(self, m):
        m.register_uri('GET', self.MIST_APP_URL + 'status', status=404)
        mist = MistApp()
        status = mist.get_status()
        self.assertEqual(status, dict(), 'It should return empty dict')
