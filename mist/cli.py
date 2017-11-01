import json
import os
from functools import update_wrapper

import click
from click.globals import get_current_context
from pyhocon import ConfigFactory
from texttable import Texttable
import hashlib

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


def _deploy(mist_app, job_version=None, dev=False, user=None):
    mist_cfg = mist_app.config_path
    try:
        endpoints, contexts = mist_app.parse_config(mist_cfg)
        if dev:
            return mist_app.dev_deploy(endpoints, contexts, user, job_version)
        else:
            if not mist_app.accept_all:
                message = "Are you sure you want to deploy {} endpoints and {} contexts".format(len(endpoints),
                                                                                                len(contexts))
                click.confirm(message, abort=True, err=True)
            return mist_app.deploy(endpoints, contexts, job_version)
    except app.BadConfigException as e:
        raise click.UsageError('\n'.join(map(lambda x: x.message, e.errors)))
    except app.DeployFailedException as ex:
        error = '\n'.join(map(lambda t: 'Failed to update {} with trace msg: {}'.format(t[0], t[1]), ex.errors))
        click.echo(error)
        raise click.Abort
    except app.FileExistsException as fe:
        message = """You cannot deploy job with old filename: tried to deploy {}. 
        Please specify --job-version or change it""".format(fe.filename)
        raise click.UsageError(message)


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


@mist_cli.command('deploy', help="""
    Production deploy of passed config and job
""")
@click.option('--job-version',
              help='Suffix for job name',
              default='',
              required=False)
@click.option('--job-path',
              type=click.Path(exists=True),
              required=True,
              help='Job file path')
@click.option('--config-path',
              default='./configs/default.conf',
              show_default=True,
              help='Config in HOCON format according to data format of <TODO: link here>',
              type=click.Path(exists=True),
              required=False)
@pass_mist_app
def deploy(ctx, mist_app, job_version, job_path, config_path):
    mist_app.job_path = job_path
    mist_app.config_path = config_path
    endpoints, contexts = _deploy(mist_app, job_version, False)
    contexts = map(Context.to_row, contexts)
    endpoints = map(Endpoint.to_row, endpoints)
    click.echo('Deployed contexts')
    draw_table(ctx, mist_app, contexts, Context.header)
    click.echo('Deployed endpoints')
    draw_table(ctx, mist_app, endpoints, Endpoint.header)


@mist_cli.command('deploy-dev', help="""
    Development deploy of config and job. User and Job version options will be added to job file name, 
    contexts and endpoints.
""")
@click.option('--user',
              default=lambda: os.environ.get('USER', ''),
              help="""
                  Prefix for context name, endpoint and job name. 
                  Default value will be gathered from MIST_USER or USER environment variable.
              """,
              prompt='Enter user')
@click.option('--job-version',
              help='Suffix for context name, endpoint and job name',
              default='0.0.0',
              callback=validate_version,
              prompt='Enter job version')
@click.option('--job-path',
              type=click.Path(exists=True),
              required=True,
              help='Job file path')
@click.option('--config-path',
              default='./configs/default.conf',
              show_default=True,
              help='Config in HOCON format according to data format of <TODO: link here>',
              type=click.Path(exists=True),
              required=False)
@pass_mist_app
def dev_deploy(ctx, mist_app, user, job_version, job_path, config_path):
    mist_app.job_path = job_path
    mist_app.config_path = config_path
    endpoints, contexts = _deploy(mist_app, job_version, True, user)
    contexts = map(Context.to_row, contexts)
    endpoints = map(Endpoint.to_row, endpoints)
    click.echo('Deployed contexts')
    draw_table(ctx, mist_app, contexts, Context.header)
    click.echo('Deployed endpoints')
    draw_table(ctx, mist_app, endpoints, Endpoint.header)


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


@mist_cli.command('create')
@pass_mist_app
@click.option('-f', '--file', type=click.Path(exists=True, dir_okay=False))
def create(ctx, mist_app, file):
    deployment = mist_app.parse_deployment(file)
    try:
        deployments = validate_deployments_and_unlink_refs(mist_app, (int(os.path.basename(file)[:2]), deployment,))
        for _, deployment in deployments:
            mist_app.update(deployment)
    except RuntimeError as e:
        click.UsageError(e)


__allowed_extensions = ['.jar', '.py']


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


def validate_deployments_and_unlink_refs(mist_app, *deployments):
    """
    :type mist_app: mist.app.MistApp
    :param mist_app:
    :type deployments: (int, Deployment)
    :param deployments:
    :return:
    """
    res = []
    context_names = dict((d[1].name, d[1]) for d in deployments if d[1].model_type == 'Context')
    artifact_names = dict((d[1].name, d[1]) for d in deployments if d[1].model_type == 'Artifact')

    for priority, deployment in deployments:
        if deployment.model_type == 'Artifact':
            job_path = deployment.data['file-path']
            artifact_name = deployment.get_name()
            if validate_artifact(mist_app, job_path, artifact_name):
                res.append((priority, deployment))

        elif deployment.model_type == 'Context':
            if mist_app.validate:
                click.echo('Validating context {}'.format(deployment.get_name()))
                context = mist_app.get_context(deployment.get_name())
                if context is not None:
                    raise RuntimeError(
                        'Found context by name {} and version {}. Aborting'.format(deployment.name, deployment.version))

            click.echo('Context {} validated'.format(deployment.get_name()))
            res.append((priority, deployment))
        elif deployment.model_type == 'Endpoint':
            if mist_app.validate:
                click.echo('Validating endpoint {}'.format(deployment.get_name()))
                remote_ep = mist_app.get_endpoint(deployment.get_name())
                if remote_ep is not None:
                    raise RuntimeError(
                        "Endpoint {} with version {} exists remotely. Aborting".format(deployment.name, deployment.version))

            ctx_name = deployment.data['context']
            remote_ctx = mist_app.get_context(ctx_name)
            if ctx_name not in context_names and remote_ctx is None:
                raise RuntimeError("Context not exists by name {}".format(ctx_name))

            deployment.data['context'] = context_names[ctx_name].get_name()
            job_link = deployment.data['path']
            _, ext = os.path.splitext(job_link)

            if ext == '' and job_link not in artifact_names:
                raise RuntimeError('Artifact not found by link {}'.format(job_link))

            if ext == '':
                artifact_name = artifact_names[job_link].get_name()
            else:  # job_link is considered to be job_path
                filename = os.path.basename(job_link)
                if validate_artifact(mist_app, job_link, filename):
                    res.append((-1000, Deployment(
                        os.path.basename(job_link),
                        'Artifact',
                        ConfigFactory.from_dict(dict(filePath=job_link))
                    )))
                    artifact_name = filename

            click.echo('Endpoint {} path {}'.format(deployment.get_name(), artifact_name))
            deployment.data['path'] = artifact_name
            res.append((priority, deployment))
        else:
            continue
    return res


def process_dir(mist_app, folder):
    click.echo('processing {}'.format(folder))
    things_to_update = []
    for f in os.listdir(folder):
        if f.endswith('.conf'):
            priority = int(f[0: 2])
            deployment = mist_app.parse_deployment(os.path.join(folder, f))
            things_to_update.append((priority, deployment))
    try:
        # for consistency:
        # 1) job exists by path;
        # 2) uploaded job version match repository one (validate sha)
        # 3) context exists by label or present in deployments to update
        # 4) update with same version but different contents
        deployments = validate_deployments_and_unlink_refs(mist_app, *things_to_update)
        deployments = list(sorted(deployments, key=lambda t: t[0]))
        for _, deployment in deployments:
            mist_app.update(deployment)
    except Exception as e:
        raise click.UsageError(e.message)


@mist_cli.command('apply')
@pass_mist_app
@click.option('-f', '--folder', type=click.Path(exists=True, file_okay=False))
@click.option('--validate', type=bool, default=True)
def apply(ctx, mist_app, folder, validate):
    mist_app.validate = validate
    dirs = []
    with_files = False
    for f in os.listdir(folder):
        path = os.path.join(folder, f)
        if os.path.isfile(path):
            with_files = True
        else:
            dirs.append(path)

    with_dirs = len(dirs) != 0

    if with_dirs and with_files:
        raise click.UsageError('folder {} must contain only directories or only files'.format(folder), ctx)

    if not with_dirs and not with_files:
        raise click.UsageError('{} should not be empty'.format(folder), ctx)

    if with_files:
        dirs.append(folder)

    for d in dirs:
        process_dir(mist_app, d)
