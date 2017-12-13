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
        self.assertEqual(ctx.name, 'foo')
        self.assertEqual(ctx.worker_mode, 'shared')
        self.assertEqual(len(ctx.spark_conf), 4)
        self.assertEqual(ctx.streaming_duration, '1s')
        self.assertEqual(ctx.downtime, 'Inf')
        self.assertEqual(ctx.max_jobs, 20)
        self.assertEqual(ctx.run_options, '')

        with_defaults = ConfigFactory.parse_string("""
            worker-mode = shared
        """)

        w_default_ctx = parser.parse('foo', with_defaults)
        self.assertEqual(w_default_ctx.spark_conf, dict())
        self.assertEqual(w_default_ctx.run_options, '')
        self.assertEqual(w_default_ctx.downtime, '120s')
        self.assertEqual(w_default_ctx.streaming_duration, '1s')
        self.assertEqual(w_default_ctx.precreated, False)

    def test_endpoint_parser(self):
        parser = app.FunctionParser()
        endpoint = parser.parse('foo', ConfigFactory.parse_string("""
            class-name = "SimpleContext"
            context = "test-context-2"
        """))
        self.assertIsInstance(endpoint, models.Function)
        self.assertEqual(endpoint.name, 'foo')
        self.assertEqual(endpoint.default_context.name, 'test-context-2')
        self.assertEqual(endpoint.class_name, 'SimpleContext')
        default_endpoint = parser.parse('foo', ConfigFactory.parse_string("""
            class-name = "SimpleContext"
        """))
        self.assertEqual(default_endpoint.default_context.name, 'default')
