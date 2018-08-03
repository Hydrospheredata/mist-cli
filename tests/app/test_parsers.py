from unittest import TestCase

from pyhocon import ConfigFactory

from mist import app, models


class NamedConfigParsers(TestCase):
    def test_context_parser(self):
        parser = app.ContextParser()
        cfg = ConfigFactory.parse_string("""
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
        """)
        ctx = parser.parse('foo', cfg)
        self.assertIsInstance(ctx, models.Context)
        context_config = ctx.context_config
        self.assertEqual(ctx.name, 'foo')
        self.assertEqual(context_config['worker-mode'], 'shared')
        self.assertEqual(len(context_config['spark-conf']), 4)
        self.assertEqual(context_config['streaming-duration'], '1s')
        self.assertEqual(context_config['downtime'], 'Inf')
        self.assertEqual(context_config['max-parallel-jobs'], 20)
        self.assertEqual(context_config['run-options'], '')

        with_defaults = ConfigFactory.parse_string("""
            worker-mode = shared
        """)

        w_default_ctx = parser.parse('foo', with_defaults)
        context_config = w_default_ctx.context_config
        self.assertEqual(context_config['spark-conf'], dict())
        self.assertIn('worker-mode', context_config)

        data = ConfigFactory.parse_string("""
            workerMode = shared
            maxParallelJobs = 20
            streaming-duration = 1s
            sparkConf {
                "kblk" = kek
            }
        """)
        ctx = parser.parse('foo', data)
        context_config = ctx.context_config
        self.assertEqual(context_config['workerMode'], 'shared')
        self.assertEqual(context_config['maxParallelJobs'], 20)
        self.assertEqual(context_config['spark-conf'], {
            'kblk': 'kek'
        })
        self.assertEqual(context_config['streaming-duration'], '1s')


    def test_function_parser(self):
        parser = app.FunctionParser()
        fn = parser.parse('foo', ConfigFactory.parse_string("""
            class-name = "SimpleContext"
            context = "test-context-2"
        """))
        self.assertIsInstance(fn, models.Function)
        self.assertEqual(fn.name, 'foo')
        self.assertEqual(fn.default_context.name, 'test-context-2')
        self.assertEqual(fn.class_name, 'SimpleContext')
        default_fn = parser.parse('foo', ConfigFactory.parse_string("""
            class-name = "SimpleContext"
        """))
        self.assertEqual(default_fn.default_context.name, 'default')
