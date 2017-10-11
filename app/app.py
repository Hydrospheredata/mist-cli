import json
import os
from collections import defaultdict

import requests
from pyhocon import ConfigFactory, ConfigTree
from requests.exceptions import HTTPError

from models import Endpoint, Context, Worker, Job

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
            name, cfg.get_string('class-name'), Context(cfg.get_string('context', 'default'))
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
                for k, v in value.iteritems():
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


class BadConfigException(Exception):
    def __init__(self, errors):
        self.errors = errors


class DeployFailedException(Exception):
    def __init__(self, errors):
        self.errors = errors


class FileExistsException(Exception):
    def __init__(self, filename):
        self.filename = filename


class MistApp(object):
    def __init__(
            self,
            host='localhost',
            port=2004,
            job_path=None,
            config_path=None,
            accept_all=False,
            format_table=False
    ):
        self.host = host
        self.port = port
        self.job_path = job_path
        self.config_path = config_path
        self.accept_all = accept_all
        self.format_table = format_table
        self.endpoint_parser = EndpointParser()
        self.context_parser = ContextParser()

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
        for k, v in config.get_config('mist.contexts').iteritems():
            try:
                contexts[k] = self.context_parser.parse(k, v)
            except Exception as e:
                errors.append(e)
        for k, v in config.get_config('mist.endpoints').iteritems():
            try:
                endpoint = self.endpoint_parser.parse(k, v)
                endpoint.default_context = contexts.get(endpoint.default_context.name, endpoint.default_context)
                endpoints.append(endpoint)
            except Exception as e:
                errors.append(e)

        if len(errors) != 0:
            raise BadConfigException(errors)

        return endpoints, contexts.values()

    def upload_job(self, filename=None):
        with open(self.job_path, 'rb') as job:
            if filename is None:
                filename = os.path.basename(self.job_path)
            url = 'http://{}:{}/v2/api/artifacts'.format(self.host, self.port)
            files = {'file': (filename, job)}
            with requests.post(url, files=files) as resp:
                if resp.status_code == 409:
                    raise FileExistsException(filename)
                resp.raise_for_status()
                job_path = resp.text
                return job_path

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
            return map(Worker.from_json, resp.json())

    def endpoints(self):
        url = 'http://{}:{}/v2/api/endpoints'.format(self.host, self.port)
        with requests.get(url) as resp:
            return map(Endpoint.from_json, resp.json())

    def jobs(self, status_filter):
        filters = map(lambda s: s.strip(), status_filter.split(','))
        url = 'http://{}:{}/v2/api/jobs'.format(self.host, self.port)
        with requests.get(url, params={'status': filters}) as resp:
            return map(Job.from_json, resp.json())

    def contexts(self):
        url = 'http://{}:{}/v2/api/contexts'.format(self.host, self.port)
        with requests.get(url) as resp:
            return map(Context.from_json, resp.json())

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

    def __format_job_name(self, version, dev=''):
        filename = os.path.basename(self.job_path)
        filename, ext = os.path.splitext(filename)
        parts = []
        if dev != '':
            parts.append(dev)
        parts.append(filename)
        if version != '':
            parts.append(version.replace('.', '_'))
        return '_'.join(parts) + ext

    def __deploy(self, endpoints, contexts):
        errors = []
        updated_ctx = []

        for c in contexts:
            try:
                self.deploy_context(c)
                updated_ctx.append(c)
            except HTTPError as err:
                errors.append(('Context ' + c.name, err))

        updated_ctx_name = map(lambda c: c.name, updated_ctx)

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
