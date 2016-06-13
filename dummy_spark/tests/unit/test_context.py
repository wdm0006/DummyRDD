# -*- coding: utf-8 -*-

import unittest

from datetime import datetime

from dummy_spark import SparkContext
from dummy_spark import RDD


class SparkContextTests (unittest.TestCase):

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

        l1 = range(0, 10, 1)
        rdd1 = ctx.range(0, 10, 1)
        self.assertEquals(l1, rdd1.collect())

        l2 = range(0, 10, 2)
        rdd2 = ctx.range(0, 10, 2)
        self.assertEquals(l2, rdd2.collect())

        l3 = range(0, 1000, 17)
        rdd3 = ctx.range(0, 1000, 17)
        self.assertEquals(l3, rdd3.collect())

    def test_parallelize_list(self):
        ctx = SparkContext()

        l1 = range(0, 10, 1)
        rdd1 = ctx.parallelize(l1)
        self.assertEquals(l1, rdd1.collect())

        l2 = range(0, 10, 2)
        rdd2 = ctx.parallelize(l2)
        self.assertEquals(l2, rdd2.collect())

        l3 = range(0, 1000, 17)
        rdd3 = ctx.parallelize(l3)
        self.assertEquals(l3, rdd3.collect())

    def test_parallelize_set(self):
        ctx = SparkContext()

        l1 = range(0, 10, 1)
        rdd1 = ctx.parallelize(set(l1))
        self.assertEquals(sorted(l1), sorted(rdd1.collect()))

        l2 = range(0, 10, 2)
        rdd2 = ctx.parallelize(set(l2))
        self.assertEquals(sorted(l2), sorted(rdd2.collect()))

        l3 = range(0, 1000, 17)
        rdd3 = ctx.parallelize(set(l3))
        self.assertEquals(sorted(l3), sorted(rdd3.collect()))
