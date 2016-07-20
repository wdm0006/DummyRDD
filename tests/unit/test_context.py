# -*- coding: utf-8 -*-

import unittest

from datetime import datetime
from tempfile import NamedTemporaryFile

from dummy_spark import SparkContext
from dummy_spark import RDD


class SparkContextTests (unittest.TestCase):

    TEST_RANGES = [
        (0, 10, 1),
        (0, 10, 2),
        (0, 100, 13),
        (0, 1000, 17),
        (0, 10000, 31),
    ]

    def test_start_time(self):
        now = datetime.now()
        ctx = SparkContext()
        delta = datetime.fromtimestamp(ctx.startTime) - now
        seconds = float(delta.total_seconds())
        self.assertTrue(seconds <= 1.0)

    def test_empty_RDD(self):
        ctx = SparkContext()
        rdd = ctx.emptyRDD()
        self.assertEquals(type(rdd), RDD)
        l = rdd.collect()
        self.assertEqual(type(l), list)
        self.assertEquals(len(l), 0)

    def test_range(self):
        ctx = SparkContext()
        for start, stop, step in self.TEST_RANGES:
            l = list(range(start, stop, step))
            rdd = ctx.range(start, stop, step)
            self.assertEquals(l, rdd.collect())

    def test_parallelize_list(self):
        ctx = SparkContext()
        for start, stop, step in self.TEST_RANGES:
            l = list(range(start, stop, step))
            rdd = ctx.parallelize(l)
            self.assertEquals(l, rdd.collect())

    def test_parallelize_set(self):
        ctx = SparkContext()
        for start, stop, step in self.TEST_RANGES:
            l = list(range(start, stop, step))
            rdd = ctx.parallelize(set(l))
            self.assertEquals(sorted(l), sorted(rdd.collect()))

    def test_text_file(self):
        ctx = SparkContext()
        for start, stop, step in self.TEST_RANGES:
            with NamedTemporaryFile(mode='w') as f:
                l = ['{}\n'.format(x) for x in range(start, stop, step)]
                for x in l:
                    f.write(x)
                f.flush()
                f.seek(0)
                rdd = ctx.textFile(f.name)
                self.assertEquals(l, rdd.collect())

    def test_version(self):
        ctx = SparkContext()
        self.assertEquals(ctx.version, SparkContext.DUMMY_VERSION)

    def test_with_block(self):
        with SparkContext():
            pass
        self.assertTrue(True)

    def test_add_py_file(self):
        with SparkContext() as ctx:
            ctx.addPyFile(__file__)
        self.assertTrue(True)

    def test_hadoop_config(self):
        ctx = SparkContext()
        jvm = ctx._jsc
        hc = jvm.hadoopConfiguration()
        hc.set('key', 'value')
        self.assertEquals(hc.get('key'), 'value')

    def test_not_implemented_methods(self):
        ctx = SparkContext()

        with self.assertRaises(NotImplementedError):
            ctx._checkpointFile(None, None)

        with self.assertRaises(NotImplementedError):
            ctx._dictToJavaMap(None)

        with self.assertRaises(NotImplementedError):
            ctx._getJavaStorageLevel(None)

        with self.assertRaises(NotImplementedError):
            ctx.accumulator(None)

        with self.assertRaises(NotImplementedError):
            ctx.addFile(None)

        with self.assertRaises(NotImplementedError):
            ctx.binaryFiles(None)

        with self.assertRaises(NotImplementedError):
            ctx.binaryRecords(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.broadcast(None)

        with self.assertRaises(NotImplementedError):
            ctx.cancelAllJobs()

        with self.assertRaises(NotImplementedError):
            ctx.cancelJobGroup(None)

        with self.assertRaises(NotImplementedError):
            ctx.clearFiles()

        with self.assertRaises(NotImplementedError):
            ctx.dump_profiles(None)

        with self.assertRaises(NotImplementedError):
            ctx.getLocalProperty(None)

        with self.assertRaises(NotImplementedError):
            ctx.hadoopFile(None, None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.hadoopRDD(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.newAPIHadoopFile(None, None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.pickleFile(None)

        with self.assertRaises(NotImplementedError):
            ctx.runJob(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.sequenceFile(None)

        with self.assertRaises(NotImplementedError):
            ctx.setCheckpointDir(None)

        with self.assertRaises(NotImplementedError):
            ctx.setJobGroup(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.setLocalProperty(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.show_profiles()

        with self.assertRaises(NotImplementedError):
            ctx.sparkUser()

        with self.assertRaises(NotImplementedError):
            ctx.statusTracker()

        with self.assertRaises(NotImplementedError):
            ctx.union(None)

        with self.assertRaises(NotImplementedError):
            ctx.wholeTextFiles(None)
