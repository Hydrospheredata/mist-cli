from unittest import TestCase

import datetime

from mist import models


class JobTest(TestCase):
    def test_job_create(self):
        job = models.Job('test', 'foo', 'bar', 'http', 'finished', 'test', 0)
        self.assertEqual(job.job_id, 'test')
        self.assertEqual(job.endpoint, 'foo')
        self.assertEqual(job.context, 'bar')
        self.assertEqual(job.source, 'http')
        self.assertEqual(job.status, 'finished')
        self.assertEqual(job.external_id, 'test')
        self.assertEqual(job.start_time, datetime.datetime.fromtimestamp(0))

    def test_job_to_json(self):
        job = models.Job('test', 'foo', 'bar', 'http', 'finished', 'test', 0)
        res = job.to_json()

        self.assertTrue('jobId' in res)
        self.assertTrue('context' in res)
        self.assertTrue('endpoint' in res)
        self.assertTrue('source' in res)
        self.assertTrue('status' in res)
        self.assertTrue('externalId' in res)
        self.assertTrue('startTime' in res)

    def test_job_from_json(self):
        job = models.Job.from_json(dict(
            jobId='test-id',
            endpoint='bar',
            context='foo',
            source='http',
            status='finished',
            externalId='test',
            startTime=0
        ))
        self.assertEqual(job.job_id, 'test-id')
        self.assertEqual(job.endpoint, 'bar')
        self.assertEqual(job.context, 'foo')
        self.assertEqual(job.status, 'finished')
        self.assertEqual(job.source, 'http')
        self.assertEqual(job.external_id, 'test')
        self.assertEqual(job.start_time, datetime.datetime.fromtimestamp(0))

    def test_job_to_row(self):
        job = models.Job('test', 'foo', 'bar', 'http', 'finished', 'test', 0)
        row = models.Job.to_row(job)
        self.assertListEqual(row, ['test', str(datetime.datetime.fromtimestamp(0)), 'bar', 'test', 'foo', 'http',
                                   'finished'])

    def test_job_header(self):
        self.assertListEqual(models.Job.header,
                             ['UID', 'START TIME', 'NAMESPACE', 'EXT ID', 'FUNCTION', 'SOURCE', 'STATUS'])