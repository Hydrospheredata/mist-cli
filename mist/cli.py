import fnmatch
import json
import math
import os
import random
from functools import update_wrapper

import click
import requests
from click.globals import get_current_context
from texttable import Texttable

from mist import app
from mist.models import Worker, Job, Function, Context, Deployment

CONTEXT_SETTINGS = dict(auto_envvar_prefix='MIST')


class GroupWithGroupSubCommand(click.Group):
    def get_commands_to_format(self, group):
        """
        :type group: click.Group
        :param group:
        :return: subcommands that should be listed
        :rtype: list of (click.Command, str)
        """
        rows = []
        for cmd in group.commands.values():
            # What is this, the tool lied about a command.  Ignore it
            if cmd is None:
                continue
            if isinstance(cmd, click.Group):
                rows += list(map(lambda t: (group.name + ' ' + t[0], t[1]), self.get_commands_to_format(cmd)))
            else:
                help = cmd.short_help or ''
                rows.append((group.name + ' ' + cmd.name, help))
        return rows

    def format_commands(self, ctx, formatter):
        """Extra format methods for multi methods that adds all the commands
        after the options.
        """
        rows = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command.  Ignore it
            if cmd is None:
                continue
            if isinstance(cmd, click.Group):
                rows += self.get_commands_to_format(cmd)
            else:
                help = cmd.short_help or ''
                rows.append((subcommand, help))

        if rows:
            with formatter.section('Commands'):
                formatter.write_dl(rows)

    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except requests.exceptions.RequestException as e:
            raise click.UsageError(str(e))


def pass_ctx_and_custom_obj_decorator(object_type, ensure=False):
    def decorator(f):
        def new_func(*args, **kwargs):
            ctx = get_current_context()
            if ensure:
                obj = ctx.ensure_object(object_type)
            else:
                obj = ctx.find_object(object_type)
            if obj is None:
                raise RuntimeError('Managed to invoke callback without a '
                                   'context object of type %r existing'
                                   % object_type.__name__)
            return ctx.invoke(f, ctx, obj, *args[2:], **kwargs)

        return update_wrapper(new_func, f)

    return decorator


pass_mist_app = pass_ctx_and_custom_obj_decorator(app.MistApp, ensure=True)


def draw_table(ctx, mist_app, items, header):
    items = list(items)
    table = Texttable()
    table.set_cols_align(list(map(lambda _: 'l', header)))
    table.set_deco(0)
    if mist_app.format_table:
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.HLINES | Texttable.VLINES)
    table.add_rows([header] + items)
    click.echo(table.draw())


__list_choices = {
    Worker: lambda mist_app, *args: mist_app.workers(),
    Job: lambda mist_app, *args: mist_app.jobs(*args),
    Function: lambda mist_app, *args: mist_app.functions(),
    Context: lambda mist_app, *args: mist_app.contexts()
}


def list_items(ctx, mist_app, item_type, *args):
    items = __list_choices.get(item_type, lambda _: [])
    rows = list(map(item_type.to_row, items(mist_app, *args)))
    header = item_type.header
    draw_table(ctx, mist_app, rows, header)


@click.group(context_settings=CONTEXT_SETTINGS, help="""
    Mist CLI interface for deploy endpoints and context config to mist server in production and development modes
""", cls=GroupWithGroupSubCommand)
@click.option('--host',
              default='localhost',
              show_default=True,
              help='Mist host value. Can be set with MIST_HOST environment variable',
              required=False)
@click.option('--port',
              default=2004,
              show_default=True,
              help='Mist port value. Can be set with MIST_PORT environment variable',
              required=False)
@click.option('-y', '--yes', is_flag=True, help='Say \'Yes\' to all confirmations')
@click.option('-f', '--format-table', is_flag=True, help='Format table')
@pass_mist_app
def mist_cli(ctx, mist_app, host, port, yes, format_table):  # pragma: no cover
    """
    :param format_table:
    :param yes:
    :type mist_app MistApp
    :type ctx click.core.Context
    :param ctx: context of click
    :param mist_app: mist configuration to be filled in later usage
    :param host: mist host
    :param port: mist port
    """
    mist_app.host = host
    mist_app.port = port
    mist_app.accept_all = yes
    mist_app.format_table = format_table


@mist_cli.group('kill')
def kill():  # pragma: no cover
    pass


@kill.command('worker', help='Kill worker by id')
@click.argument('worker_id')
@pass_mist_app
def kill_worker(ctx, mist_app, worker_id):
    if not mist_app.accept_all:
        click.confirm('Are you sure you want to stop worker {}?'.format(worker_id), abort=True, err=True)
    click.echo('Killing worker {}'.format(worker_id))
    mist_app.kill_worker(worker_id)


@kill.command('job', help='Cancel job by id or external id')
@click.argument('job_id')
@pass_mist_app
def kill_job(ctx, mist_app, job_id):
    if not mist_app.accept_all:
        click.confirm('Are you sure you want to cancel job {}?'.format(job_id), abort=True, err=True)
    click.echo('Killing job {}'.format(job_id))
    mist_app.cancel_job(job_id)

    click.echo('Killed job {}'.format(job_id))
    table = Texttable()
    table.header(['Job cancelled', 'JOB ID'])
    table.set_deco(0)
    if mist_app.format_table:
        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.HLINES | Texttable.VLINES)
    table.add_row(['', job_id])
    click.echo(table.draw())


@mist_cli.group('list')
def list_cmd():  # pragma: no cover
    pass


@list_cmd.command('workers', help='List workers')
@pass_mist_app
def list_workers(ctx, mist_app):
    list_items(ctx, mist_app, Worker)


@list_cmd.command('jobs', help='List jobs')
@click.option('--filter',
              required=False,
              default='started',
              help='Comma separated job statuses')
@pass_mist_app
def list_jobs(ctx, mist_app, filter):
    list_items(ctx, mist_app, Job, filter)


@list_cmd.command('functions', help='List all endpoints')
@pass_mist_app
def list_functions(ctx, mist_app):
    list_items(ctx, mist_app, Function)


@list_cmd.command('contexts', help='List all contexts')
@pass_mist_app
def list_contexts(ctx, mist_app):
    list_items(ctx, mist_app, Context)


@mist_cli.group('start')
def start():  # pragma: no cover
    pass


@start.command('job',
               help='Start job',
               short_help='start job <endpoint> <json request>')
@click.argument('function', required=True, nargs=1)
@click.argument('request', required=False, nargs=1, default='{}')
@click.option('--pretty', is_flag=True)
@pass_mist_app
def start_job(ctx, mist_app, function, request, pretty):
    if request[0] == '@':
        file_path_with_json = request[1:]
        with open(file_path_with_json, 'r') as f:
            request = json.load(f)

    kw = dict()
    if pretty:  # pragma: no cover
        kw['indent'] = 2
        kw['sort_keys'] = True

    click.echo(
        json.dumps(mist_app.start_job(function, request), **kw)
    )


def generate_request(endpoint_json):
    """
    :param endpoint_json:
    :return:
    """
    generated_obj = {}
    execute_instance = endpoint_json.get('execute', dict())
    for key in execute_instance.keys():
        generated_obj[key] = generate_value(execute_instance[key])

    return json.dumps(generated_obj)


def generate_value(param_type):
    t = param_type['type']
    args = param_type['args']

    if t == 'MString':
        return 'string'
    if t == 'MAny':
        return {}
    if t == 'MMap':
        return {
            generate_value(args[0]): generate_value(args[1])
        }
    if t == 'MInt':
        return math.ceil(random.random() * 10)
    if t == 'MDouble':
        return random.random()
    if t == 'MList':
        return [generate_value(args[0])]
    if t == 'MOption':
        return generate_value(args[0])


def print_examples(mist_app, deployment):
    """
    :type mist_app: mist.app.MistApp
    :param mist_app:
    :type deployment: Deployment
    :param deployment:
    :return:
    """
    url = 'http://{}:{}/v2/api'.format(mist_app.host, mist_app.port)
    if deployment.model_type == 'Function':
        click.echo('Get info of endpoint resource')
        click.echo('-' * 80)
        endpoint_name = deployment.get_name()
        click.echo("curl  -H 'Content-Type: application/json' -X GET {url}/endpoints/{name}\n".format(
            url=url, name=endpoint_name
        ))
        endpoint_json = mist_app.get_function_json(endpoint_name)
        if endpoint_json is not None:
            click.echo('Start job via mist-cli')
            click.echo('-' * 80)
            request = generate_request(endpoint_json)
            click.echo("mist-cli --host {host} --port {port} start job {endpoint} '{request}'\n".format(
                endpoint=endpoint_name, request=request, host=mist_app.host, port=mist_app.port))

            click.echo('Start job via curl')
            click.echo('-' * 80)
            curl_cmd = "curl --data '{request}' -H 'Content-Type: application/json' -X POST {url}/endpoints/{" \
                       "name}/jobs?force=true"

            click.echo(curl_cmd.format(request=request, url=url, name=endpoint_name))
    elif deployment.model_type == 'Context':
        click.echo('Get context info')
        click.echo('-' * 80)
        click.echo("curl -H 'Content-Type: application/json' -X POST {url}/contexts/{name}".format(
            url=url, name=deployment.get_name()
        ))
    click.echo('\n')


def easy_glob(root, pattern):
    matches = []
    for root, _, filenames in os.walk(root):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches


@mist_cli.command('apply', help="""
    Applying changes in given --file/-f parameter.
    Creates or updates existing configuration in Mist.
""")
@pass_mist_app
@click.option('-u', '--user',
              help='username prefix for deployment entry name',
              required=False,
              default=lambda: os.getenv('USER', ''))
@click.option('-f', '--file',
              help="""
                File path where configs are stored
              """,
              required=True, type=click.Path(exists=True, file_okay=True))
@click.option('--validate', type=bool, default=True)
def apply(ctx, mist_app, user, file, validate):
    mist_app.validate = validate

    if os.path.isfile(file):
        deployments = [mist_app.parse_deployment(file)]
    else:
        deployments = sorted(map(
            mist_app.parse_deployment,
            easy_glob(os.path.abspath(file), '*.conf')
        ), key=lambda t: t[0])
    click.echo("Process {} file entries".format(len(deployments)))
    depls = list(map(lambda t: t[1].with_user(user), deployments))
    mist_app.update_deployments(depls)
    for d in depls:
        print_examples(mist_app, d)
