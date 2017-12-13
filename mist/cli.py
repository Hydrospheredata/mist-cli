import json
import os
import random
from functools import update_wrapper
import glob
import click
import math
from click.globals import get_current_context
from pyhocon import ConfigFactory
from texttable import Texttable

from mist import app
from mist.models import Worker, Job, Endpoint, Context, Deployment

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


def validate_version(ctx, param, value):
    try:
        return '.'.join(map(str, map(int, value.split('.'))))
    except ValueError:
        raise click.BadParameter('version should be in format x.[y.] or at least one integer')


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
    Endpoint: lambda mist_app, *args: mist_app.endpoints(),
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
def mist_cli(ctx, mist_app, host, port, yes, format_table):
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
def kill():
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
def list_cmd():
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


@list_cmd.command('endpoints', help='List all endpoints')
@pass_mist_app
def list_endpoints(ctx, mist_app):
    list_items(ctx, mist_app, Endpoint)


@list_cmd.command('contexts', help='List all contexts')
@pass_mist_app
def list_contexts(ctx, mist_app):
    list_items(ctx, mist_app, Context)


@mist_cli.group('start')
def start():
    pass


@start.command('job',
               help='Start job',
               short_help='start job <endpoint> <json request>')
@click.argument('endpoint', required=True, nargs=1)
@click.argument('request', required=False, nargs=1)
@click.option('--pretty', is_flag=True)
@pass_mist_app
def start_job(ctx, mist_app, endpoint, request, pretty):
    kw = dict()
    if pretty:
        kw['indent'] = 2
        kw['sort_keys'] = True

    click.echo(
        json.dumps(mist_app.start_job(endpoint, request), **kw)
    )


def validate_artifact(mist_app, file_path, artifact_name=None):
    if artifact_name is None:
        artifact_name = os.path.basename(file_path)

    if not os.path.exists(file_path):
        raise RuntimeError("job deployment should exists by path {}".format(file_path))

    should_be_updated = False
    uploaded_job_sha = mist_app.get_sha1(artifact_name)
    if uploaded_job_sha is not None:
        calculated_sha = app.calculate_sha1(file_path)
        if calculated_sha != uploaded_job_sha:
            raise RuntimeError('Artifact {} content is differ with remote version, please specify different '
                               'version of artifact: path {}'.format(artifact_name, file_path))
        else:
            click.echo('Artifact {} already exists'.format(artifact_name))
    else:
        click.echo('Artifact {} is valid and will be updated'.format(artifact_name))
        should_be_updated = True

    return should_be_updated


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
    if deployment.model_type == 'Endpoint':
        click.echo('Get info of endpoint resource')
        click.echo('-' * 80)
        endpoint_name = deployment.get_name()
        click.echo("curl  -H 'Content-Type: application/json' -X GET {url}/endpoints/{name}\n".format(
            url=url, name=endpoint_name
        ))
        endpoint_json = mist_app.get_endpoint_json(endpoint_name)
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
    elif deployment.model_type == 'Artifact':
        click.echo('Get artifact file')
        click.echo('-' * 80)
        click.echo("curl -H 'Content-Type: application/json' -X POST {url}/artifacts/{name}".format(
            url=url, name=deployment.get_name()
        ))
    elif deployment.model_type == 'Context':
        click.echo('Get context info')
        click.echo('-' * 80)
        click.echo("curl -H 'Content-Type: application/json' -X POST {url}/contexts/{name}".format(
            url=url, name=deployment.get_name()
        ))
    click.echo('\n')


@mist_cli.command('apply')
@pass_mist_app
@click.option('-f', '--folder',
              help="""
              Folder either containing directories with configuration or 
              folder with configuration of deployment stages
              """,
              required=True, type=click.Path(exists=True, file_okay=False))
@click.option('--validate', type=bool, default=True)
def apply(ctx, mist_app, folder, validate):
    """

    :param ctx:
    :type mist_app: mist.app.MistApp
    :param mist_app:
    :param folder:
    :param validate:
    :return:
    """
    mist_app.validate = validate
    glob_expr = os.path.abspath(folder) + '/**/*.conf'
    deployments = sorted(map(mist_app.parse_deployment, glob.glob(glob_expr, recursive=True)), key=lambda t: t[0])
    click.echo("Proccess {} file entries".format(deployments))
    try:
        mist_app.update_deployments(list(map(lambda t: t[1], deployments)))
    except Exception as ex:
        raise click.UsageError(str(ex))
