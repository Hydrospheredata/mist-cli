from unittest import TestCase

from mist import models


class ContextTest(TestCase):
    def test_context_create(self):
        context = models.Context('test', {
            'max-jobs': 20,
            'downtime': '1s',
            'spark-conf': dict(),
            'worker-mode': 'shared',
            'run-options': '',
            'precreated': False,
            'streaming-duration': '1s'
        })
        context_config = context.context_config
        self.assertEqual(context.name, 'test')
        self.assertEqual(context_config['max-jobs'], 20)
        self.assertEqual(context_config['downtime'], '1s')
        self.assertEqual(context_config['spark-conf'], dict())
        self.assertEqual(context_config['worker-mode'], 'shared')
        self.assertEqual(context_config['run-options'], '')
        self.assertEqual(context_config['precreated'], False)
        self.assertEqual(context_config['streaming-duration'], '1s')

        default_values = models.Context('test')
        context_config = default_values.context_config
        self.assertEqual(context_config, dict())

    def test_context_to_json(self):
        context = models.Context('test', {
            'max-jobs': 20,
            'downtime': '1s',
            'spark-conf': dict(),
            'worker-mode': 'shared',
            'run-options': '',
            'precreated': False,
            'streaming-duration': '1s'
        })

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
        context_config = ctx.context_config
        self.assertEqual(ctx.name, 'test')
        self.assertEqual(context_config['max-jobs'], 20)
        self.assertEqual(context_config['streaming-duration'], '1s')
        self.assertEqual(context_config['run-options'], '')
        self.assertEqual(context_config['worker-mode'], 'shared')

    def test_context_to_row(self):
        context = models.Context('test', {
            'max-jobs': 20,
            'streaming-duration': '1s',
            'spark-conf': dict(),
            'downtime': '1s',
            'run-options': '',
            'worker-mode': 'shared'
        })

        row = models.Context.to_row(context)
        self.assertListEqual(row, ['test', 'shared'])

    def test_context_header(self):
        self.assertListEqual(models.Context.header, ['ID', 'WORKER MODE'])
