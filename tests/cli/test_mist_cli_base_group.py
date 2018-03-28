from unittest import TestCase

import click
import click.testing as testing
from mock import MagicMock

from mist import cli
from mist.app import MistApp
from mist.cli import get_mist_versions


class MockClass(object):
    def __init__(self):
        self.some_field = 'test'


class MistCliTest(TestCase):
    def test_mist_cli_group_with_recursive_subcommands(self):
        cmd = click.Group('test2', commands={
            'test3': click.Group('test3', commands={
                'yeah': click.Command('yeah', help='Test')
            })
        })
        root_cmd = cli.GroupWithGroupSubCommand('test', commands={'test2': cmd})
        res = root_cmd.get_commands_to_format(root_cmd)
        self.assertEqual(res[0], ('test test2 test3 yeah', 'Test'))

    def test_mist_cli_app_make_decorator(self):
        fn = cli.pass_ctx_and_custom_obj_decorator(MockClass, ensure=True)

        @click.command('test')
        @fn
        def some_test_fn(ctx, mc):
            click.echo(mc.__class__.__name__)

        runner = testing.CliRunner()
        res = runner.invoke(some_test_fn)
        self.assertEqual(res.output.strip(), 'MockClass')

    def test_mist_cli_get_mist_versions(self):
        mist = MistApp()
        actual_status = dict(mistVersion="1.2.3", sparkVersion="4.5.6", javaVersion=dict(runtimeVersion="7.8.9"))
        mist.get_status = MagicMock(return_value=actual_status)
        t = get_mist_versions(mist)

        self.assertEqual(t[0], "1.2.3")
        self.assertEqual(t[1], "4.5.6")
        self.assertEqual(t[2], "7.8.9")

        status_wo_java_version = dict(mistVersion="1.2.3", sparkVersion="4.5.6")
        mist.get_status = MagicMock(return_value=status_wo_java_version)

        t = get_mist_versions(mist)

        self.assertEqual(t[0], "1.2.3")
        self.assertEqual(t[1], "4.5.6")
        self.assertEqual(t[2], "UNKNOWN")

        status_wo_java_version = dict(mistVersion="1.2.3", sparkVersion="4.5.6", javaVersion=dict())
        mist.get_status = MagicMock(return_value=status_wo_java_version)

        t = get_mist_versions(mist)

        self.assertEqual(t[0], "1.2.3")
        self.assertEqual(t[1], "4.5.6")
        self.assertEqual(t[2], "UNKNOWN")

        status_wo_mist_spark_version = dict(javaVersion=dict(runtimeVersion="7.8.9"))
        mist.get_status = MagicMock(return_value=status_wo_mist_spark_version)

        t = get_mist_versions(mist)

        self.assertEqual(t[0], "UNKNOWN")
        self.assertEqual(t[1], "UNKNOWN")
        self.assertEqual(t[2], "7.8.9")

