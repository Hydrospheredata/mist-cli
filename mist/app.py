import hashlib
import json
import os
from collections import defaultdict

import requests
from pyhocon import ConfigFactory, ConfigTree
from requests.exceptions import HTTPError

from mist.models import Endpoint, Context, Worker, Job, Deployment, Artifact

try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote


class NamedConfigParser(object):
    def parse(self, name, cfg):
        pass


class EndpointParser(NamedConfigParser):
    def parse(self, name, cfg):
        """
        :type name str
        :param name:
        :type cfg pyhocon.config_tree.ConfigTree
        :param cfg:
        :return:
        """
        return Endpoint(
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


class BadConfigException(Exception):
    def __init__(self, errors):
        self.errors = errors


class DeployFailedException(Exception):
    def __init__(self, errors):
        self.errors = errors


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


def add_suffixes(file_path, *suffixes):
    filename = os.path.basename(file_path)
    filename, ext = os.path.splitext(filename)
    parts = list(map(lambda suffix: suffix.replace('.', '_'), suffixes))
    return filename + '_' + '_'.join(parts) + ext


class MistApp(object):
    def __init__(
            self,
            host='localhost',
            port=2004,
            job_path=None,
            config_path=None,
            accept_all=False,
            format_table=False,
            validate=True
    ):
        self.host = host
        self.port = port
        self.job_path = job_path
        self.config_path = config_path
        self.accept_all = accept_all
        self.format_table = format_table
        self.endpoint_parser = EndpointParser()
        self.context_parser = ContextParser()
        self.artifact_parser = ArtifactParser()
        self.validate = validate

    @staticmethod
    def parse_deployment(deployment_conf):
        cfg = ConfigFactory.parse_file(deployment_conf)
        model_type = cfg.get_string('model')
        name = cfg.get_string('name', os.path.basename(os.path.dirname(deployment_conf)))
        return Deployment(
            name,
            model_type,
            cfg.get_config('data', ConfigTree()),
            cfg['version']
        )

    def __resolve_parser_and_update_fn(self, model_type):
        """
        :param model_type:
        :raise RuntimeError
        :return:
        :rtype NamedConfigParser
        """
        if model_type == 'Artifact':
            parser = self.artifact_parser
            update_fn = self.__upload_artifact
        elif model_type == 'Endpoint':
            parser = self.endpoint_parser
            update_fn = self.deploy_endpoint
        elif model_type == 'Context':
            parser = self.context_parser
            update_fn = self.deploy_context
        else:
            raise RuntimeError('unknown model type')
        return parser, update_fn

    def update(self, deployment):
        """
        :type deployment: Deployment
        :param deployment:
        :return: updated item
        :rtype:
        """
        model_type = deployment.model_type
        print("updating {}".format(model_type))
        parser, update_fn = self.__resolve_parser_and_update_fn(model_type)

        item = parser.parse(deployment.name, deployment.data)
        item.with_version(deployment.version)
        return update_fn(item)

    def parse_config(self, file_path):
        """
        :param file_path:

        :return: endpoints and contexts in tuple
        :rtype: (list of Endpoint, list of Context)
        """
        config = ConfigFactory.parse_file(file_path)
        endpoints = []
        errors = []
        contexts = defaultdict(lambda: Context('default'))
        contexts_cfg = config.get_config('mist.contexts')
        for k in contexts_cfg.keys():
            try:
                v = contexts_cfg[k]
                contexts[k] = self.context_parser.parse(k, v)
            except Exception as e:
                errors.append(e)
        endpoints_cfg = config.get_config('mist.endpoints')
        for k in endpoints_cfg.keys():
            try:
                v = endpoints_cfg[k]
                endpoint = self.endpoint_parser.parse(k, v)
                endpoint.default_context = contexts.get(endpoint.default_context.name, endpoint.default_context)
                endpoints.append(endpoint)
            except Exception as e:
                errors.append(e)

        if len(errors) != 0:
            raise BadConfigException(errors)

        return endpoints, contexts.values()

    def upload_job(self, filename=None):
        if filename is None:
            filename, _ = os.path.splitext(os.path.basename(self.job_path))
        return self.__upload_artifact(Artifact(filename, self.job_path)).file_path

    def __upload_artifact(self, artifact):
        with open(artifact.file_path, 'rb') as job:
            _, ext = os.path.splitext(artifact.file_path)
            url = 'http://{}:{}/v2/api/artifacts'.format(self.host, self.port)
            artifact_filename = artifact.name + ext
            print(artifact_filename)
            files = {'file': (artifact_filename, job)}
            with requests.post(url, files=files) as resp:
                if resp.status_code == 409:
                    raise FileExistsException(artifact_filename)
                resp.raise_for_status()
                job_path = resp.text
                return Artifact(artifact.name, job_path)

    def deploy(self, endpoints, contexts, job_version=''):
        uploaded_file_path = self.upload_job(self.__format_job_name(job_version))

        for e in endpoints:
            e.with_path(uploaded_file_path)

        return self.__deploy(endpoints, contexts)

    def dev_deploy(self, endpoints, contexts, dev, job_version):
        uploaded_file_path = self.upload_job(self.__format_job_name(job_version, dev))

        for c in contexts:
            c.with_dev(dev).with_version(job_version)

        for e in endpoints:
            e.with_dev(dev) \
                .with_version(job_version) \
                .with_path(uploaded_file_path)

        return self.__deploy(endpoints, contexts)

    def deploy_endpoint(self, endpoint):
        url = 'http://{}:{}/v2/api/endpoints'.format(self.host, self.port)
        data = endpoint.to_json()
        with requests.post(url, json=data) as resp:
            resp.raise_for_status()
            return Endpoint.from_json(resp.json())

    def deploy_context(self, context):
        url = 'http://{}:{}/v2/api/contexts'.format(self.host, self.port)
        data = context.to_json()
        with requests.post(url, json=data) as resp:
            resp.raise_for_status()
            return Context.from_json(resp.json())

    def workers(self):
        url = 'http://{}:{}/v2/api/workers'.format(self.host, self.port)
        with requests.get(url) as resp:
            return list(map(Worker.from_json, resp.json()))

    def endpoints(self):
        url = 'http://{}:{}/v2/api/endpoints'.format(self.host, self.port)
        with requests.get(url) as resp:
            return list(map(Endpoint.from_json, resp.json()))

    def jobs(self, status_filter):
        filters = list(map(lambda s: s.strip(), status_filter.split(',')))
        url = 'http://{}:{}/v2/api/jobs'.format(self.host, self.port)
        with requests.get(url, params={'status': filters}) as resp:
            return list(map(Job.from_json, resp.json()))

    def contexts(self):
        url = 'http://{}:{}/v2/api/contexts'.format(self.host, self.port)
        with requests.get(url) as resp:
            return list(map(Context.from_json, resp.json()))

    def cancel_job(self, job_id):
        url = 'http://{}:{}/v2/api/jobs/{}'.format(self.host, self.port, quote(job_id, safe=''))
        with requests.delete(url) as resp:
            resp.raise_for_status()

    def kill_worker(self, worker_id):
        url = 'http://{}:{}/v2/api/workers/{}'.format(self.host, self.port, quote(worker_id, safe=''))
        with requests.delete(url) as resp:
            resp.raise_for_status()

    def start_job(self, endpoint, request):
        url = 'http://{}:{}/v2/api/endpoints/{}/jobs?force=true'.format(self.host, self.port, quote(endpoint, safe=''))
        req = json.loads(request)
        with requests.post(url, json=req) as resp:
            resp.raise_for_status()
            return resp.json()

    def get_sha1(self, artifact_name):
        url = 'http://{}:{}/v2/api/artifacts/{}/sha'.format(self.host, self.port, quote(artifact_name))
        with requests.get(url) as resp:
            if resp.status_code == 200:
                return resp.text
            return None

    def get_context(self, context_name):
        url = 'http://{}:{}/v2/api/contexts/{}'.format(self.host, self.port, quote(context_name))
        with requests.get(url) as resp:
            if resp.status_code == 200:
                return Context.from_json(resp.json())
            return None

    def get_endpoint(self, endpoint_name):
        url = 'http://{}:{}/v2/api/endpoints/{}'.format(self.host, self.port, quote(endpoint_name))
        with requests.get(url) as resp:
            if resp.status_code == 200:
                return Endpoint.from_json(resp.json())
            return None

    def __format_job_name(self, version, dev=''):
        args = []
        if dev != '':
            args.append(dev)
        args.append(version)
        return add_suffixes(self.job_path, *args)

    def __deploy(self, endpoints, contexts):
        errors = []
        updated_ctx = []

        for c in contexts:
            try:
                self.deploy_context(c)
                updated_ctx.append(c)
            except HTTPError as err:
                errors.append(('Context ' + c.name, err))

        updated_ctx_name = list(map(lambda c: c.name, updated_ctx))

        def updated_ctx_or_default(endpoint):
            return endpoint.default_context.name in updated_ctx_name or endpoint.default_context.name == 'default'

        filtered = filter(updated_ctx_or_default, endpoints)
        deployed_endpoints = []
        for e in filtered:
            try:
                deployed_endpoints.append(self.deploy_endpoint(e))
            except HTTPError as err:
                errors.append(('Endpoint ' + e.name, err))
        if len(errors) != 0:
            raise DeployFailedException(errors)
        return deployed_endpoints, updated_ctx

    def get_full_endpoint(self, endpoint_name):
        url = 'http://{}:{}/v2/api/endpoints/{}'.format(self.host, self.port, quote(endpoint_name))
        with requests.get(url) as resp:
            if resp.status_code == 200:
                return resp.json()
            return None
