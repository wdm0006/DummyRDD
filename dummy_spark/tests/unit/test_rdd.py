# -*- coding: utf-8 -*-

import unittest

from dummy_spark import SparkContext, SparkConf


class RDDTests (unittest.TestCase):

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
