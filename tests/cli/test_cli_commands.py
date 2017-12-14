import json
import os
import shutil
import sys
from unittest import TestCase

from click import testing
from mock import MagicMock, call
from pyhocon import ConfigTree

from mist import cli, app, models


def make_dirs(directory, exist_ok=False):
    major_ver = sys.version_info[0]
    if major_ver == 2:
        if exist_ok and not os.path.exists(directory):
            os.makedirs(directory)
        elif not exist_ok:
            raise IOError('directory {} exists'.format(directory))
    else:
        os.makedirs(directory, exist_ok=exist_ok)


class CliTest(TestCase):
    def setUp(self):
        self.runner = testing.CliRunner()

        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, 'test_job.py')
        with open(file_path, 'w+') as f:
            f.write('print "Hello!"')
        self.job_path = file_path
        self.test_apply_folder = os.path.abspath('../../example/simple-context')
        self.test_apply_invalid_folder1 = dir_path
        make_dirs(os.path.join(dir_path, 'test'), exist_ok=True)
        self.apply_job_path = self.setup_job(dir_path)
        self.test_function_obj = {
            'name': 'simple-context',
            'execute': {
                'numbers': {'type': 'MList', 'args': [{'type': 'MInt', 'args': []}]},
                'multiplier': {'type': 'MOption', 'args': [{'type': 'MInt', 'args': []}]}
            },
            'path': '/Users/blvp/scala/global/mist/target/mist-0.13.3-1.5.2/mist-examples-spark.jar',
            'tags': [],
            'className': 'SimpleContext$',
            'defaultContext': 'foo',
            'lang': 'scala'
        }

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
        self.test_apply_file_artifact = os.path.abspath(os.path.join(job_path, '00artifact.conf'))
        with open(self.test_apply_file_artifact, 'w+') as f:
            f.write("""
            name=test-job
            model=Artifact
            version=0.0.1
            data.file-path={path}
            """.format(path=test_job_path))
        self.ctx_file_apply = os.path.abspath(os.path.join(job_path, '10context.conf'))
        with open(self.ctx_file_apply, 'w+') as f:
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
        self.fn_apply_file = os.path.abspath(os.path.join(job_path, '20function.conf'))
        with open(self.fn_apply_file, 'w+') as f:
            f.write("""
            model = Function
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
        shutil.rmtree(os.path.join(self.test_apply_invalid_folder1, 'test'))
        shutil.rmtree(self.apply_job_path)

    def test_mist_cli_commands(self):
        self.assertEqual(len(cli.mist_cli.get_commands_to_format(cli.mist_cli)), 8)

    def test_mist_cli_list_subcommands(self):
        mist_app = app.MistApp()
        mist_app.workers = MagicMock(return_value=[models.Worker('test-worker-id', 'localhost:0', 'spark-ui')])
        mist_app.contexts = MagicMock(return_value=[models.Context('foo')])
        mist_app.jobs = MagicMock(return_value=[models.Job('test-job-id', 'test', 'foo', 'cli', 'started')])
        mist_app.functions = MagicMock(return_value=[models.Function('test', 'Test', 'foo')])

        def invoke_cmd(cmd):
            return self.runner.invoke(cmd, catch_exceptions=True, obj=mist_app)

        w_res = invoke_cmd(cli.list_workers)
        e_res = invoke_cmd(cli.list_functions)
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

    def test_mist_cli_start_job(self):
        mist_app = app.MistApp()
        mist_app.start_job = MagicMock(return_value=dict(errors=[], payload={'result': [1, 2, 3]}, success=True))
        res = self.runner.invoke(cli.start_job, args=('simple',), obj=mist_app)
        self.assertEqual(res.exit_code, 0)
        returned_json = json.loads(res.output)
        expected_json = json.loads('{"errors": [], "payload": {"result": [1, 2, 3]}, "success": true}\n')
        self.assertEqual(returned_json, expected_json)
        fn = mist_app.start_job.call_args[0][0]
        self.assertEqual(fn, 'simple')

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
        res = self.runner.invoke(cli.apply, ('--file', './test-folder'), obj=mist_app)
        self.assertEqual(res.exit_code, 2)

    def test_mist_cli_apply_file(self):
        mist_app = app.MistApp()
        mist_app.get_function_json = MagicMock(return_value=self.test_function_obj)
        mist_app.parse_deployment = MagicMock(return_value=(
            0, models.Deployment('test', 'Artifact', ConfigTree({'file-path': 'test-path.jar'}), '0.0.1')))
        mist_app.update_deployments = MagicMock(return_value=None)

        res = self.runner.invoke(cli.apply, ('--file', self.test_apply_file_artifact), obj=mist_app)
        self.assertEqual(res.exit_code, 0)

    def test_mist_cli_apply_ordered(self):
        mist_app = app.MistApp()

        mist_app.get_function_json = MagicMock(return_value=self.test_function_obj)
        fn_depl = models.Deployment('test-1', 'Function', ConfigTree())
        ctx_depl = models.Deployment('test-2', 'Context', ConfigTree())
        artifact_depl = models.Deployment('test-3', 'Artifact', ConfigTree({'file-path': 'test-path.jar'}), '0.0.1')
        mist_app.parse_deployment = MagicMock(side_effect=[
            (20, fn_depl),
            (0, ctx_depl),
            (10, artifact_depl),
        ])

        mist_app.update_deployments = MagicMock(return_value=None)
        res = self.runner.invoke(cli.apply, ('--file', self.apply_job_path), obj=mist_app)
        mist_app.update_deployments.assert_called_once_with([ctx_depl, artifact_depl, fn_depl])
        calls = [call(self.fn_apply_file), call(self.ctx_file_apply), call(self.test_apply_file_artifact)]
        mist_app.parse_deployment.assert_has_calls(calls, any_order=True)

        self.assertEqual(res.exit_code, 0)
