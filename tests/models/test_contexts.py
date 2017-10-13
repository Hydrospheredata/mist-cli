from unittest import TestCase

from mist import models


class ContextTest(TestCase):
    def test_context_create(self):
        context = models.Context('test', 20, '1s', dict(), 'shared', '', False, '1s')

        self.assertEqual(context.name, 'test')
        self.assertEqual(context.max_jobs, 20)
        self.assertEqual(context.downtime, '1s')
        self.assertEqual(context.spark_conf, dict())
        self.assertEqual(context.worker_mode, 'shared')
        self.assertEqual(context.run_options, '')
        self.assertEqual(context.precreated, False)
        self.assertEqual(context.streaming_duration, '1s')

        default_values = models.Context('test')
        self.assertEqual(default_values.spark_conf, dict())
        self.assertEqual(default_values.precreated, False)
        self.assertEqual(default_values.worker_mode, 'shared')
        self.assertEqual(default_values.run_options, '')

    def test_context_to_json(self):
        context = models.Context('test', 20, '1s', dict(), 'shared', '', False, '1s')
        res = context.to_json()
        self.assertTrue('name' in res)
        self.assertTrue('maxJobs' in res)
        self.assertTrue('sparkConf' in res)
        self.assertTrue('workerMode' in res)
        self.assertTrue('downtime' in res)
        self.assertTrue('runOptions' in res)
        self.assertTrue('precreated' in res)
        self.assertTrue('streamingDuration' in res)

    def test_context_from_json(self):
        ctx = models.Context.from_json(dict(
            name='test',
            maxJobs=20,
            streamingDuration='1s',
            sparkConf=dict(),
            downtime='1s',
            runOptions='',
            workerMode='shared'
        ))
        self.assertEqual(ctx.name, 'test')
        self.assertEqual(ctx.max_jobs, 20)
        self.assertEqual(ctx.streaming_duration, '1s')
        self.assertEqual(ctx.run_options, '')
        self.assertEqual(ctx.worker_mode, 'shared')

    def test_context_to_row(self):
        context = models.Context('test', 20, '1s', dict(), 'shared', '', False, '1s')
        row = models.Context.to_row(context)
        self.assertListEqual(row, ['test', 'shared'])

    def test_context_header(self):
        self.assertListEqual(models.Context.header, ['ID', 'WORKER MODE'])
