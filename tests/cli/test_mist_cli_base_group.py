from unittest import TestCase

import click
import click.testing as testing

from mist import cli


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
