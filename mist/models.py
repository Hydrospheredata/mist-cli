import datetime
import os
import re
from abc import ABCMeta, abstractmethod


def dashed_case_to_camel_case(param_name):
    pn = param_name.split('-')
    first, rest = pn[0], pn[1:]
    return first + ''.join(word.capitalize() for word in rest)


def snake_case_to_camel_case(param_name):
    pn = param_name.split('_')
    first, rest = pn[0], pn[1:]
    return first + ''.join(word.capitalize() for word in rest)


def camel_case_to_dashed_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()


class PrettyRow(object):
    __metaclass__ = ABCMeta
    header = []

    @staticmethod
    @abstractmethod
    def to_row(item):
        raise NotImplementedError


class JsonConfig(object):
    __metaclass__ = ABCMeta

    def to_json(self):
        obj = self.__dict__
        return dict([(snake_case_to_camel_case(k), v) for k, v in obj.items()])

    @staticmethod
    @abstractmethod
    def from_json(data):
        """
        :type data: dict
        :param data:
        :return:
        """
        raise NotImplementedError


class NamedConfig(JsonConfig):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        self.name = name

    def with_dev(self, dev_name):
        self.name = '{}_{}'.format(dev_name, self.name)
        return self

    def with_version(self, version):
        if version is None:
            return self

        self.name = '{}_{}'.format(self.name, version)
        return self


class Context(NamedConfig, PrettyRow):
    header = ['ID', 'WORKER MODE']

    @staticmethod
    def to_row(item):
        """
        :type item: Context
        :param item:
        :return:
        """
        worker_mode = item.context_config.get('worker-mode', item.context_config.get('workerMode', 'exclusive'))
        return [item.name, worker_mode]

    def __init__(self, name, context_config=None):
        """
        :type name: str
        :param name:
        :type context_config: dict
        :param context_config:
        """
        super(Context, self).__init__(name)
        if context_config is None:
            context_config = dict()

        self.context_config = context_config

    @staticmethod
    def from_json(data):
        name = data['name']
        del data['name']
        context_config = [(camel_case_to_dashed_case(k), v) for k, v in data.items()]
        return Context(name, dict(context_config))

    def to_json(self):
        name = self.name
        result = dict([(dashed_case_to_camel_case(k), v) for k, v in self.context_config.items()])
        result['name'] = name
        return result


class Function(NamedConfig, PrettyRow):
    header = ['FUNCTION', 'DEFAULT CONTEXT', 'PATH', 'CLASS NAME']

    def __init__(self, name, class_name=None, context=None, path=None):
        super(Function, self).__init__(name)

        if isinstance(context, str):
            context = Context(context)
        if context is None:
            context = Context('default')

        self.default_context = context
        self.class_name = class_name
        self.path = path

    def to_json(self):
        obj = super(NamedConfig, self).to_json()
        obj['defaultContext'] = self.default_context.name
        return obj

    def with_path(self, path):
        self.path = path
        return self

    @staticmethod
    def from_json(data):
        return Function(
            data['name'],
            data['className'],
            Context(data.get('defaultContext', 'default')),
            data['path']
        )

    @staticmethod
    def to_row(item):
        """
        :type item: Function
        :param item:
        :return:
        """
        return [item.name, item.default_context.name, item.path, item.class_name]


class Job(JsonConfig, PrettyRow):
    header = ['UID', 'START TIME', 'NAMESPACE', 'EXT ID', 'FUNCTION', 'SOURCE', 'STATUS']

    def __init__(self, job_id, function_id, context, source, status, external_id=None, start_time=None):
        self.job_id = job_id
        self.function = function_id
        self.context = context
        self.source = source
        self.status = status
        if external_id is None:
            external_id = ''
        self.external_id = external_id
        if start_time is not None:
            start_time = datetime.datetime.fromtimestamp(start_time / 1000.0)
        self.start_time = start_time

    @staticmethod
    def to_row(job):
        return [
            job.job_id,
            str(job.start_time) if job.start_time else '-',
            job.context,
            job.external_id,
            job.function,
            job.source,
            job.status
        ]

    @staticmethod
    def from_json(data):
        return Job(
            data['jobId'], data['function'], data['context'],
            data['source'], data['status'], data.get('externalId', ''),
            data.get('startTime', None)
        )


class Worker(JsonConfig, PrettyRow):
    header = ['ID', 'ADDRESS', 'SPARK UI']

    @staticmethod
    def to_row(item):
        """
        :type item: Worker
        :param item:
        :return:
        """
        return [item.name, item.address, item.spark_ui]

    def __init__(self, name, address, spark_ui=None):
        self.name = name
        self.address = address
        if spark_ui is None:
            spark_ui = ''
        self.spark_ui = spark_ui

    @staticmethod
    def from_json(data):
        return Worker(
            data['name'], data['address'], data.get('sparkUi', '')
        )


class Artifact(NamedConfig):
    @staticmethod
    def from_json(data):
        raise NotImplementedError

    def __init__(self, name, file_path):
        super(Artifact, self).__init__(name)
        self.file_path = file_path

    @property
    def artifact_key(self):
        _, ext = os.path.splitext(self.file_path)
        artifact_filename = self.name + ext
        return artifact_filename


class Deployment(object):
    model_type_choices = ('Artifact', 'Function', 'Context')

    def __init__(self, name, model_type, data, version=None):
        """
        :type name: str
        :param name:
        :type model_type: str
        :param model_type:
        :type data: pyhocon.config_tree.ConfigTree
        :param data:
        :param version:
        """
        self.name = name
        if model_type not in self.model_type_choices:
            raise ValueError('Model type should be equal one of {}'.format(self.model_type_choices))

        self.model_type = model_type
        self.data = data
        self.version = version

    def get_name(self):
        name = self.name

        if self.version is not None:
            name += '_' + self.version

        if self.model_type == 'Artifact':
            _, ext = os.path.splitext(self.data['file-path'])
            name += ext

        return name

    def with_user(self, user_name):
        if len(user_name) is not 0:
            # ar this point it is a dirty hack where we should change name for internal items.
            # it should happen for function deployment where both context name and job name should be prefixed too.
            if self.model_type == 'Function':
                self.data['path'] = '{}_{}'.format(user_name, self.data['path'])
                context_is_not_default = not 'default' == self.data.get('context', 'default')
                if context_is_not_default:
                    self.data['context'] = '{}_{}'.format(user_name, self.data['context'])

            self.name = '{}_{}'.format(user_name, self.name)
        return self

    def __str__(self):
        return 'Deployment({}, {}, {}, {})'.format(self.name, self.model_type, self.data, self.version)
