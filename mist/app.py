import hashlib
import json
import os

import click
import requests
from pyhocon import ConfigFactory, ConfigTree

from mist.models import Function, Context, Worker, Job, Deployment, Artifact

try:  # pragma: no cover
    from urllib.parse import quote
except ImportError:
    from urllib import quote


class NamedConfigParser(object):
    def parse(self, name, cfg):  # pragma: no cover
        pass


class FunctionParser(NamedConfigParser):
    def parse(self, name, cfg):
        """
        :type name str
        :param name:
        :type cfg pyhocon.config_tree.ConfigTree
        :param cfg:
        :return:
        """
        return Function(
            name,
            cfg.get_string('class-name'),
            Context(cfg.get_string('context', 'default')),
            cfg.get_string('path', None)
        )


class ContextParser(NamedConfigParser):
    def parse(self, name, cfg):
        """
        :type name str
        :param name:
        :type cfg pyhocon.config_tree.ConfigTree
        :param cfg:
        :return:
        """

        def parse_spark_config(value, key_prefix):
            if isinstance(value, ConfigTree):
                res = dict()
                for k in value.keys():
                    v = value[k]
                    new_key = k if key_prefix == '' else key_prefix + '.' + k
                    res.update(parse_spark_config(v, new_key))
                return res
            else:
                return {
                    key_prefix: str(value)
                }

        return Context(
            name, cfg.get_int('max-parallel-jobs', 20), cfg.get_string('downtime', '120s'),
            parse_spark_config(cfg.get_config('spark-conf', ConfigTree()), ''), cfg.get_string('worker-mode', 'shared'),
            cfg.get_string('run-options', ''), cfg.get_bool('precreated', False),
            cfg.get_string('streaming-duration', '1s')
        )


class ArtifactParser(NamedConfigParser):
    def parse(self, name, cfg):
        return Artifact(
            name,
            cfg.get_string('file-path')
        )


class FileExistsException(Exception):
    def __init__(self, filename):
        self.filename = filename


def calculate_sha1(file_path):
    sha1sum = hashlib.sha1()
    with open(file_path, 'rb') as source:
        block = source.read(2 ** 16)
        while len(block) != 0:
            sha1sum.update(block)
            block = source.read(2 ** 16)
    return sha1sum.hexdigest()


class MistApp(object):
    def __init__(
            self,
            host='localhost',
            port=2004,
            accept_all=False,
            format_table=False,
            validate=True
    ):
        self.host = host
        self.port = port
        self.accept_all = accept_all
        self.format_table = format_table
        self.function_parser = FunctionParser()
        self.context_parser = ContextParser()
        self.artifact_parser = ArtifactParser()
        self.validate = validate

    @staticmethod
    def parse_deployment(deployment_conf):

        cfg = ConfigFactory.parse_file(deployment_conf)
        model_type = cfg['model']
        name = cfg.get_string('name', os.path.basename(os.path.dirname(deployment_conf)))
        version = None
        if model_type == 'Artifact':
            version = cfg['version']
        order = MistApp.__safe_get_order(deployment_conf)

        return order, Deployment(
            name,
            model_type,
            cfg.get_config('data', ConfigTree()),
            version
        )

    @staticmethod
    def __safe_get_order(deployment_file_path):
        try:
            order = int(os.path.basename(deployment_file_path)[0:2])
        except ValueError:  # pragma: no cover
            order = 1000
        return order

    def __resolve_by_model_type(self, model_type):
        """
        :param model_type:
        :raise RuntimeError
        :return:
        :rtype NamedConfigParser
        """
        if model_type == 'Artifact':
            parser = self.artifact_parser
            update_fn = self.__upload_artifact
            validate_fn = self._validate_artifact
        elif model_type == 'Function':
            parser = self.function_parser
            update_fn = self.update_function
            validate_fn = self._validate_function
        elif model_type == 'Context':
            parser = self.context_parser
            update_fn = self.update_context
            validate_fn = self._validate_context
        else:
            raise RuntimeError('unknown model type')
        return parser, validate_fn, update_fn

    def update(self, deployment):
        """
        :type deployment: Deployment
        :param deployment:
        :return: updated item
        :rtype:
        """
        model_type = deployment.model_type
        print("updating {} {}".format(deployment.model_type, deployment.get_name()))
        parser, validate_fn, update_fn = self.__resolve_by_model_type(model_type)

        item = parser.parse(deployment.name, deployment.data)
        item.with_version(deployment.version)
        if self.validate:
            validate_fn(item)

        return update_fn(item)

    def __upload_artifact(self, artifact):
        with open(artifact.file_path, 'rb') as fn_file:
            url = 'http://{}:{}/v2/api/artifacts'.format(self.host, self.port)
            artifact_filename = artifact.artifact_key
            files = {'file': (artifact_filename, fn_file)}
            resp = requests.post(url, files=files, params={'force': not self.validate})
            if resp.status_code == 409:
                raise FileExistsException(artifact_filename)
            resp.raise_for_status()
            job_path = resp.text
            return Artifact(artifact.name, job_path)

    def update_function(self, fn):
        url = 'http://{}:{}/v2/api/endpoints'.format(self.host, self.port)
        data = fn.to_json()
        resp = requests.post(url, json=data, params={'force': not self.validate})
        resp.raise_for_status()
        return Function.from_json(resp.json())

    def update_context(self, context):
        url = 'http://{}:{}/v2/api/contexts'.format(self.host, self.port)
        data = context.to_json()
        resp = requests.post(url, json=data)
        resp.raise_for_status()
        return Context.from_json(resp.json())

    def workers(self):
        url = 'http://{}:{}/v2/api/workers'.format(self.host, self.port)
        resp = requests.get(url)
        return list(map(Worker.from_json, resp.json()))

    def functions(self):
        url = 'http://{}:{}/v2/api/endpoints'.format(self.host, self.port)
        resp = requests.get(url)
        return list(map(Function.from_json, resp.json()))

    def jobs(self, status_filter):
        filters = list(map(lambda s: s.strip(), status_filter.split(',')))
        url = 'http://{}:{}/v2/api/jobs'.format(self.host, self.port)
        resp = requests.get(url, params={'status': filters})
        return list(map(Job.from_json, resp.json()))

    def contexts(self):
        url = 'http://{}:{}/v2/api/contexts'.format(self.host, self.port)
        resp = requests.get(url)
        return list(map(Context.from_json, resp.json()))

    def cancel_job(self, job_id):
        url = 'http://{}:{}/v2/api/jobs/{}'.format(self.host, self.port, quote(job_id, safe=''))
        resp = requests.delete(url)
        resp.raise_for_status()

    def kill_worker(self, worker_id):
        url = 'http://{}:{}/v2/api/workers/{}'.format(self.host, self.port, quote(worker_id, safe=''))
        resp = requests.delete(url)
        resp.raise_for_status()

    def start_job(self, function, req):
        if isinstance(req, str):
            req = json.loads(req)

        url = 'http://{}:{}/v2/api/endpoints/{}/jobs?force=true'.format(self.host, self.port, quote(function, safe=''))
        resp = requests.post(url, json=req)
        resp.raise_for_status()
        return resp.json()

    def get_sha1(self, artifact_name):
        url = 'http://{}:{}/v2/api/artifacts/{}/sha'.format(self.host, self.port, quote(artifact_name, safe=''))
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.text
        return None

    def get_context(self, context_name):
        url = 'http://{}:{}/v2/api/contexts/{}'.format(self.host, self.port, quote(context_name, safe=''))
        resp = requests.get(url)
        if resp.status_code == 200:
            return Context.from_json(resp.json())
        return None

    def get_function(self, function_name):
        fn = self.get_function_json(function_name)
        if fn is not None:
            return Function.from_json(fn)
        return None

    def get_function_json(self, fn_name):
        url = 'http://{}:{}/v2/api/endpoints/{}'.format(self.host, self.port, quote(fn_name, safe=''))
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        return None

    def update_deployments(self, deployments):
        for depl in deployments:
            try:
                self.update(depl)
                click.echo('Success: {} {}'.format(depl.model_type, depl.get_name()))
            except Exception as e:
                click.echo('Error: ' + str(e))

    def _validate_artifact(self, a):
        """
        :type a: Artifact
        :param a:
        :return:
        """
        remote_file_sha = self.get_sha1(a.artifact_key)
        if remote_file_sha is not None:
            raise ValueError("Artifact key {} has to be unique".format(a.artifact_key))

    def _validate_context(self, c):
        pass

    def _validate_function(self, e):
        """
        :type e: Function
        :param e:
        :return:
        """
        remote_ctx = self.get_context(e.default_context.name)
        artifact_sha = self.get_sha1(e.path)

        message_tmpl = "{} {} is not valid. Please check: {}"

        if remote_ctx is None:
            msg = 'Context {} should exists remotely'.format(e.default_context.name)
            raise ValueError(message_tmpl.format('Function', e.name, msg))

        if artifact_sha is None:
            msg = 'Artifact {} should exists remotely'.format(e.path)
            raise ValueError(message_tmpl.format('Function', e.name, msg))
