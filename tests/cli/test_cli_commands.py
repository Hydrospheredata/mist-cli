import json
import os
import shutil
import unittest
from unittest import TestCase

from click import testing
from mock import MagicMock
from pyhocon import ConfigFactory, ConfigTree

from mist import cli, app, models


class CliTest(TestCase):
    def setUp(self):
        self.runner = testing.CliRunner()

        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, 'test_job.py')
        with open(file_path, 'w+') as f:
            f.write('print "Hello!"')
        self.job_path = file_path

        conf_path = os.path.join(dir_path, 'test_conf.conf')
        with open(conf_path, 'w+') as f:
            f.write("""
            mist {
                endpoints {
                }
                contexts {
                }
            }
            """)

        self.test_apply_folder = os.path.abspath('../../example/simple-context')
        self.test_apply_invalid_folder1 = dir_path
        os.makedirs(os.path.join(dir_path, 'test'), exist_ok=True)
        self.apply_job_path = self.setup_job(dir_path)
        self.config_path = conf_path

    def setup_job(self, dir_path):
        job_path = os.path.join(dir_path, 'simple-context')
        os.mkdir(job_path)
        test_job_path = os.path.abspath(os.path.join(job_path, 'test-job.py'))
        with open(test_job_path, 'w+') as f:
            f.write("""
            from mist.mist_job import MistJob

            class SimpleContext(MistJob):

                def execute(self, numbers, multiplier = 2):
                    rdd = self.context.parallelize(numbers)
                    result = rdd.map(lambda s: s * multiplier).collect()
                    return {"result": result}
            """)
        with open(os.path.join(job_path, '00artifact.conf'), 'w+') as f:
            f.write("""
            name=test-job
            model=Artifact
            version=0.0.1
            data.file-path={path}
            """.format(path=test_job_path))
        with open(os.path.join(job_path, '10context.conf'), 'w+') as f:
            f.write("""
            name=foo
            model=Context
            version=0.0.1
            data {
                worker-mode = shared
                max-parallel-jobs = 20
                downtime = Inf
                precreated = false
                streaming-duration = 1s
                spark-conf { }
                run-options = ""
            }
            """)
        with open(os.path.join(job_path, '20endpoint.conf'), 'w+') as f:
            f.write("""
            model = Endpoint
            name = test-name 
            version = 0.0.1
            data {
                path = test-job
                class-name = SimpleContext
                context = foo
            }
            """)
        return job_path

    def tearDown(self):
        os.remove(self.job_path)
        os.remove(self.config_path)
        shutil.rmtree(os.path.join(self.test_apply_invalid_folder1, 'test'))
        shutil.rmtree(self.apply_job_path)

    def test_mist_cli_commands(self):
        self.assertEqual(len(cli.mist_cli.get_commands_to_format(cli.mist_cli)), 11)

    def test_mist_cli_list_subcommands(self):
        mist_app = app.MistApp()
        mist_app.workers = MagicMock(return_value=[models.Worker('test-worker-id', 'localhost:0', 'spark-ui')])
        mist_app.contexts = MagicMock(return_value=[models.Context('foo')])
        mist_app.jobs = MagicMock(return_value=[models.Job('test-job-id', 'test', 'foo', 'cli', 'started')])
        mist_app.endpoints = MagicMock(return_value=[models.Endpoint('test', 'Test', 'foo')])

        def invoke_cmd(cmd):
            return self.runner.invoke(cmd, catch_exceptions=True, obj=mist_app)

        w_res = invoke_cmd(cli.list_workers)
        e_res = invoke_cmd(cli.list_endpoints)
        j_res = invoke_cmd(cli.list_jobs)
        c_res = invoke_cmd(cli.list_contexts)

        self.assertEqual(w_res.exit_code, 0)
        self.assertEqual(j_res.exit_code, 0)
        self.assertEqual(c_res.exit_code, 0)
        self.assertEqual(e_res.exit_code, 0)

        self.assertTrue('localhost:0' in w_res.output)
        self.assertTrue('test-job-id' in j_res.output)
        self.assertTrue('Test' in e_res.output)
        self.assertTrue('foo' in c_res.output)

    def test_mist_cli_deploy(self):
        mist_app = app.MistApp()
        mist_app.deploy = MagicMock(return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        args = [
            '--job-path', self.job_path,
            '--config-path', self.config_path,
            '--job-version', '0.0.1'
        ]
        res = self.runner.invoke(cli.deploy, args=args, obj=mist_app)
        # requires some input for accepting deploy or arg
        self.assertEqual(res.exit_code, 1)

        res = self.runner.invoke(cli.deploy, args=args, obj=mist_app, input='yes')
        self.assertEqual(res.exit_code, 0)
        mist_app.accept_all = True

        res = self.runner.invoke(cli.deploy, args=args, obj=mist_app)
        self.assertEqual(res.exit_code, 0)

    def test_mist_cli_dev_deploy(self):
        mist_app = app.MistApp()
        mist_app.parse_config = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        mist_app.dev_deploy = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        args = [
            '--job-path', self.job_path,
            '--config-path', self.config_path,
            '--job-version', '0.0.1',
            '--user', 'foo'
        ]
        res = self.runner.invoke(cli.dev_deploy, args=args, obj=mist_app)
        self.assertEqual(res.exit_code, 0)
        mist_app.dev_deploy.assert_called_once()
        request = mist_app.dev_deploy.call_args
        eps, ctxs, user, version = request[0]
        self.assertEqual(len(eps), 1)
        self.assertEqual(len(ctxs), 1)
        self.assertEqual(user, 'foo')
        self.assertEqual(version, '0.0.1')

    def test_mist_cli_dev_deploy_wrong_args(self):
        mist_app = app.MistApp()
        mist_app.parse_config = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        mist_app.dev_deploy = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        wrong_args = [
            '--job-path', '/tmp/not_existent_file.py',
            '--job-version', '0.0.1',
            '--user', 'foo'
        ]
        res = self.runner.invoke(cli.dev_deploy, args=wrong_args, obj=mist_app)
        self.assertEqual(res.exit_code, 2)

        wrong_args = [
            '--job-path', self.job_path,
            '--config-path', '/tmp/not_existent_file.conf',
            '--job-version', '0.0.1',
            '--user', 'foo'
        ]
        res = self.runner.invoke(cli.dev_deploy, args=wrong_args, obj=mist_app)
        self.assertEqual(res.exit_code, 2)

        wrong_args = [
            '--job-path', self.job_path,
            '--config-path', self.config_path,
            '--job-version', 'asd.0.1',
            '--user', 'foo'
        ]
        res = self.runner.invoke(cli.dev_deploy, args=wrong_args, obj=mist_app)
        self.assertEqual(res.exit_code, 2)

    def test_mist_cli_dev_deploy_prompt_for_empty_options(self):
        mist_app = app.MistApp()
        mist_app.parse_config = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        mist_app.dev_deploy = MagicMock(
            return_value=([models.Endpoint('test', 'Test', 'foo')], [models.Context('foo')]))
        res = self.runner.invoke(cli.dev_deploy, args=[
            '--job-path', self.job_path,
            '--config-path', self.config_path
        ], obj=mist_app, input='foo\n0.0.1')
        self.assertEqual(res.exit_code, 0)
        mist_app.dev_deploy.assert_called_once()
        request = mist_app.dev_deploy.call_args
        _, _, dev, version = request[0]
        self.assertEqual(dev, 'foo')
        self.assertEqual(version, '0.0.1')

    def test_mist_cli_start_job(self):
        mist_app = app.MistApp()
        mist_app.start_job = MagicMock(return_value=dict(errors=[], payload={'result': [1, 2, 3]}, success=True))
        res = self.runner.invoke(cli.start_job, args=('simple',), obj=mist_app)
        self.assertEqual(res.exit_code, 0)
        returned_json = json.loads(res.output)
        expected_json = json.loads('{"errors": [], "payload": {"result": [1, 2, 3]}, "success": true}\n')
        self.assertEqual(returned_json, expected_json)
        endpoint = mist_app.start_job.call_args[0][0]
        self.assertEqual(endpoint, 'simple')

    def test_mist_cli_kill_job_w_manual_accepting(self):
        mist_app = app.MistApp()
        mist_app.cancel_job = MagicMock(return_value=None)
        res = self.runner.invoke(cli.kill_job, args=('test-job-id',), obj=mist_app, input='yes')
        self.assertEqual(res.exit_code, 0)
        args = mist_app.cancel_job.call_args
        job_id = args[0][0]
        self.assertEqual(job_id, 'test-job-id')
        self.assertIn('Killed job', res.output)
        self.assertIn('test-job-id', res.output)

    def test_mist_cli_kill_job_wo_manual_accepting(self):
        mist_app = app.MistApp(accept_all=True)
        mist_app.cancel_job = MagicMock(return_value=None)
        res = self.runner.invoke(cli.kill_job, args=('test-job-id',), obj=mist_app)
        self.assertEqual(res.exit_code, 0)
        args = mist_app.cancel_job.call_args
        job_id = args[0][0]
        self.assertEqual(job_id, 'test-job-id')
        self.assertIn('Killed job', res.output)
        self.assertIn('test-job-id', res.output)

    def test_mist_cli_kill_worker_w_manual_accepting(self):
        mist_app = app.MistApp()
        mist_app.kill_worker = MagicMock(return_value=None)
        res = self.runner.invoke(cli.kill_worker, args=('test-worker-id',), obj=mist_app, input='yes')
        self.assertEqual(res.exit_code, 0)
        args = mist_app.kill_worker.call_args
        worker_id = args[0][0]
        self.assertEqual(worker_id, 'test-worker-id')
        self.assertIn('Killing worker', res.output)
        self.assertIn('test-worker-id', res.output)

    def test_mist_cli_kill_worker_wo_manual_accepting(self):
        mist_app = app.MistApp(accept_all=True)
        mist_app.kill_worker = MagicMock(return_value=None)
        res = self.runner.invoke(cli.kill_worker, args=('test-worker-id',), obj=mist_app)
        self.assertEqual(res.exit_code, 0)
        args = mist_app.kill_worker.call_args
        worker_id = args[0][0]
        self.assertEqual(worker_id, 'test-worker-id')
        self.assertIn('Killing worker', res.output)
        self.assertIn('test-worker-id', res.output)

    def test_mist_cli_apply_not_existing_folder(self):
        mist_app = app.MistApp()
        res = self.runner.invoke(cli.apply, ('--folder', './test-folder'), obj=mist_app)
        self.assertEqual(res.exit_code, 2)

    def test_mist_cli_apply_folder_with_folders_and_files(self):
        mist_app = app.MistApp()
        res = self.runner.invoke(cli.apply, ('--folder', self.test_apply_invalid_folder1), obj=mist_app)
        self.assertEqual(res.exit_code, 2)

    def test_mist_cli_apply_new_configs_with_validation(self):
        mist_app = app.MistApp()
        mist_app.get_sha1 = MagicMock(return_value=None)
        # app.calculate_sha1 = MagicMock(return_value='TEST_CONTENT')
        mist_app.get_context = MagicMock(return_value=None)
        mist_app.get_endpoint = MagicMock(return_value=None)
        mist_app.update = MagicMock()
        mist_app.get_full_endpoint = MagicMock(return_value={
            'execute': {
                'test-arg': {
                    'args': [

                    ],
                    'type': 'MInt'
                }
            }
        })
        res = self.runner.invoke(cli.apply, ('--folder', self.apply_job_path), obj=mist_app)
        self.assertEqual(0, res.exit_code)

    def test_mist_cli_apply_new_configs_without_validation(self):
        mist_app = app.MistApp()
        mist_app.get_sha1 = MagicMock(return_value=None)
        mist_app.get_context = MagicMock(return_value=None)
        # app.calculate_sha1 = MagicMock(return_value='TEST_CONTENT')
        mist_app.update = MagicMock()
        mist_app.get_full_endpoint = MagicMock(return_value={
            'execute': {
                'test-arg': {
                    'args': [

                    ],
                    'type': 'MInt'
                }
            }
        })
        res = self.runner.invoke(cli.apply, ('--folder', self.apply_job_path, '--validate', 'false'), obj=mist_app)
        self.assertEqual(0, res.exit_code)


    # def test_mist_cli_apply_existing_configs_without_validation(self):
    #     pass
    #
    # def test_mist_cli_apply_existing_configs_with_validation(self):
    #     pass
