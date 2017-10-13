from unittest import TestCase

from mist import models


class WorkerTest(TestCase):
    def test_worker_create(self):
        worker = models.Worker('test', 'akka.tcp://localhost:2551/', 'http://192.168.0.1:4040/')
        self.assertEqual(worker.name, 'test')
        self.assertEqual(worker.address, 'akka.tcp://localhost:2551/')
        self.assertEqual(worker.spark_ui, 'http://192.168.0.1:4040/')

    def test_worker_to_json(self):
        worker = models.Worker('test', 'akka.tcp://localhost:2551/', 'http://192.168.0.1:4040/')
        res = worker.to_json()

        self.assertTrue('name' in res)
        self.assertTrue('address' in res)
        self.assertTrue('sparkUi' in res)

    def test_worker_from_json(self):
        worker = models.Worker.from_json(dict(
            name='test',
            address='test',
            sparkUi='sparkui'
        ))
        self.assertEqual(worker.name, 'test')
        self.assertEqual(worker.address, 'test')
        self.assertEqual(worker.spark_ui, 'sparkui')

    def test_worker_to_row(self):
        worker = models.Worker('test', 'akka.tcp://localhost:2551/', 'http://192.168.0.1:4040/')
        row = models.Worker.to_row(worker)
        self.assertListEqual(row, ['test', 'akka.tcp://localhost:2551/', 'http://192.168.0.1:4040/'])

    def test_worker_header(self):
        self.assertListEqual(models.Worker.header, ['ID', 'ADDRESS', 'SPARK UI'])
