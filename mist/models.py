import datetime
import os
from abc import ABCMeta, abstractmethod


def snake_to_camel_case(param_name):
    pn = param_name.split('_')
    first, rest = pn[0], pn[1:]
    return first + ''.join(word.capitalize() for word in rest)


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
        return dict(zip(map(lambda x: snake_to_camel_case(x), obj.keys()), obj.values()))

    @staticmethod
    @abstractmethod
    def from_json(data):
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

        version = version.replace('.', '_')
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
        return [item.name, item.worker_mode]

    def __init__(
            self,
            name,
            max_jobs=None,
            downtime=None,
            spark_conf=None,
            worker_mode='shared',
            run_options='',
            precreated=False,
            streaming_duration=None
    ):
        super(Context, self).__init__(name)
        self.max_jobs = max_jobs
        self.downtime = downtime
        if spark_conf is None:
            spark_conf = dict()
        self.spark_conf = spark_conf
        self.worker_mode = worker_mode
        self.run_options = run_options
        self.precreated = precreated
        self.streaming_duration = streaming_duration

    @staticmethod
    def from_json(data):
        return Context(
            data['name'],
            data.get('maxJobs', None),
            data.get('downtime', None),
            data.get('sparkConf', dict()),
            data['workerMode'],
            data.get('runOptions', ''),
            data.get('precreated', None),
            data.get('streamingDuration', None)
        )


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

    def __init__(self, job_id, endpoint, context, source, status, external_id=None, start_time=None):
        self.job_id = job_id
        self.endpoint = endpoint
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
            job.endpoint,
            job.source,
            job.status
        ]

    @staticmethod
    def from_json(data):
        return Job(
            data['jobId'], data['endpoint'], data['context'],
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
            name += '_' + self.version.replace('.', '_')

        if self.model_type == 'Artifact':
            _, ext = os.path.splitext(self.data['file-path'])
            name += ext

        return name

    def with_user(self, user_name):
        if len(user_name) is not 0:
            # ar this point it is a dirty hack where we should change name for internal items.
            # it should happen for endpoint deployment where both context name and job name should be prefixed too.
            if self.model_type == 'Function':
                self.data['path'] = '{}_{}'.format(user_name, self.data['path'])
                self.data['context'] = '{}_{}'.format(user_name, self.data['context'])
            self.name = '{}_{}'.format(user_name, self.name)
        return self

    def __str__(self):
        return 'Deployment({}, {}, {}, {})'.format(self.name, self.model_type, self.data, self.version)
