import os
import unittest
from unittest import TestCase

from click import testing
from mock import MagicMock
from pyhocon import ConfigFactory

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

        self.config_path = conf_path

    def tearDown(self):
        os.remove(self.job_path)
        os.remove(self.config_path)

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
        self.assertEqual(res.output, '{"errors": [], "payload": {"result": [1, 2, 3]}, "success": true}\n')
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
