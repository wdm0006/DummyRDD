# -*- coding: utf-8 -*-

import unittest

from dummy_spark import RDD
from dummy_spark import SparkContext
from dummy_spark import SparkConf


class RDDTests (unittest.TestCase):

    SPARK_CONTEXT = SparkContext(master='', conf=SparkConf())
    TEST_RANGES = [
        (0, 10, 1),
        (0, 10, 2),
        (0, 100, 13),
        (0, 1000, 17),
        (0, 10000, 31),
    ]

    def test_init(self):
        for start, stop, step in self.TEST_RANGES:
            l = list(range(start, stop, step))
            rdd = RDD(l, self.SPARK_CONTEXT)
            self.assertEquals(l, rdd.collect())

            s = set(range(100))
            rdd = RDD(s, self.SPARK_CONTEXT)
            self.assertEquals(sorted(list(s)), sorted(rdd.collect()))

        t = (1, 2, 3)
        with self.assertRaises(AttributeError):
            RDD(t, self.SPARK_CONTEXT)

        with self.assertRaises(AttributeError):
            RDD('', self.SPARK_CONTEXT)

    def test_ctx(self):
        rdd = RDD([], self.SPARK_CONTEXT)
        self.assertEquals(rdd.ctx, self.SPARK_CONTEXT)

    def test_map(self):
        for start, stop, step in self.TEST_RANGES:
            f = lambda x: x**2
            l1 = range(start, stop, step)
            l2 = map(f, l1)
            rdd = RDD(l1, self.SPARK_CONTEXT)
            rdd = rdd.map(f)
            self.assertEquals(rdd.collect(), l2)

    def test_flat_map(self):
        for start, stop, step in self.TEST_RANGES:
            f = lambda x: [x, x, x]
            l1 = range(start, stop, step)
            l2 = map(f, l1)
            l3 = []
            for sl in l2:
                l3.extend(sl)
            rdd = RDD(l1, self.SPARK_CONTEXT)
            rdd = rdd.flatMap(f)
            self.assertEquals(rdd.collect(), l3)

    def test_filter(self):
        for start, stop, step in self.TEST_RANGES:
            f = lambda x: (x == x**2)
            l1 = range(start, stop, step)
            l2 = filter(f, l1)
            rdd = RDD(l1, self.SPARK_CONTEXT)
            rdd = rdd.filter(f)
            self.assertEquals(rdd.collect(), l2)

    def test_distinct(self):
        for start, stop, step in self.TEST_RANGES:
            f = lambda x: 1
            l1 = range(start, stop, step)
            rdd = RDD(l1, self.SPARK_CONTEXT)
            rdd = rdd.map(f)
            rdd = rdd.distinct()
            self.assertEquals(rdd.collect(), [1])

    def test_word_count_1(self):

        lines = [
            'grape banana apple',
        ]

        expected_output = [
            ('apple', 1),
            ('banana', 1),
            ('grape', 1),
        ]

        sc = SparkContext(master='', conf=SparkConf())

        rdd = sc.parallelize(lines)
        rdd = rdd.flatMap(lambda x: x.split(' '))
        rdd = rdd.map(lambda word: (word, 1))
        rdd = rdd.reduceByKey(lambda a, b: a + b)

        output = rdd.collect()
        self.assertEquals(sorted(output), sorted(expected_output))

    def test_word_count_2(self):

        lines = [
            'apple',
            'apple banana',
            'apple banana',
            'apple banana grape',
        ]

        expected_output = [
            ('apple', 4),
            ('banana', 3),
            ('grape', 1),
        ]

        sc = SparkContext(master='', conf=SparkConf())

        rdd = sc.parallelize(lines)
        rdd = rdd.flatMap(lambda x: x.split(' '))
        rdd = rdd.map(lambda word: (word, 1))
        rdd = rdd.reduceByKey(lambda a, b: a + b)

        output = rdd.collect()
        self.assertEquals(sorted(output), sorted(expected_output))

    def test_word_count_3(self):

        lines = [
            'apple',
            'apple banana',
            'apple banana',
            'apple banana grape',
            'banana grape',
            'banana'
        ]

        expected_output = [
            ('apple', 4),
            ('banana', 5),
            ('grape', 2),
        ]

        sc = SparkContext(master='', conf=SparkConf())

        rdd = sc.parallelize(lines)
        rdd = rdd.flatMap(lambda x: x.split(' '))
        rdd = rdd.map(lambda word: (word, 1))
        rdd = rdd.reduceByKey(lambda a, b: a + b)

        output = rdd.collect()
        self.assertEquals(sorted(output), sorted(expected_output))
