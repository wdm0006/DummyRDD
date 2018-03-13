# -*- coding: utf-8 -*-

import unittest

from dummy_spark import RDD
from dummy_spark import SparkContext
from dummy_spark import SparkConf


class RDDTests (unittest.TestCase):

    SPARK_CONTEXT = SparkContext(master='', conf=SparkConf())
    TEST_RANGES = [
        (0, 0, 1),
        (0, 10, 1),
        (0, 10, 2),
        (0, 100, 13),
        (0, 1000, 17),
        (0, 10000, 31),
    ]
    SAMPLE_FRACTION = 0.10
    SAMPLE_SEED = 1234

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

    @staticmethod
    def square(x):
        return x**2

    def test_map(self):
        for start, stop, step in self.TEST_RANGES:
            l1 = range(start, stop, step)
            l2 = map(RDDTests.square, l1)
            rdd = RDD(list(l1), self.SPARK_CONTEXT)
            rdd = rdd.map(RDDTests.square)
            self.assertEquals(rdd.collect(), list(l2))

    @staticmethod
    def triplicate(x):
        return [x, x, x]

    def test_flat_map(self):
        for start, stop, step in self.TEST_RANGES:
            l1 = range(start, stop, step)
            l2 = map(RDDTests.triplicate, l1)
            l3 = []
            for sl in l2:
                l3.extend(sl)
            rdd = RDD(list(l1), self.SPARK_CONTEXT)
            rdd = rdd.flatMap(RDDTests.triplicate)
            self.assertEquals(rdd.collect(), list(l3))

    @staticmethod
    def is_square(x):
        return x == x**2

    def test_filter(self):
        for start, stop, step in self.TEST_RANGES:
            l1 = range(start, stop, step)
            l2 = filter(RDDTests.is_square, l1)
            rdd = RDD(list(l1), self.SPARK_CONTEXT)
            rdd = rdd.filter(RDDTests.is_square)
            self.assertEquals(rdd.collect(), list(l2))

    @staticmethod
    def return_one(x):
        return x - x + 1

    def test_distinct(self):
        for start, stop, step in self.TEST_RANGES:
            l = range(start, stop, step)
            rdd = RDD(list(l), self.SPARK_CONTEXT)
            rdd = rdd.map(RDDTests.return_one)
            rdd = rdd.distinct()
            if len(l) > 0:
                self.assertEquals(rdd.collect(), [1])
            else:
                self.assertEquals(rdd.collect(), [])

    def test_sample_with_replacement(self):
        for start, stop, step in self.TEST_RANGES:
            l = range(start, stop, step)
            rdd = RDD(list(l), self.SPARK_CONTEXT)
            sample = rdd.sample(True, self.SAMPLE_FRACTION).collect()
            self.assertEquals(len(sample), int(len(l) * self.SAMPLE_FRACTION))
            for item in sample:
                self.assertTrue(item in l)

    def test_sample_with_replacement_with_seed(self):
        for start, stop, step in self.TEST_RANGES:
            l = range(start, stop, step)
            rdd = RDD(list(l), self.SPARK_CONTEXT)
            sample1 = rdd.sample(True, self.SAMPLE_FRACTION, self.SAMPLE_SEED).collect()
            sample2 = rdd.sample(True, self.SAMPLE_FRACTION, self.SAMPLE_SEED).collect()
            self.assertEquals(sorted(sample1), sorted(sample2))
            sample = sample1
            self.assertEquals(len(sample), int(len(l) * self.SAMPLE_FRACTION))
            for item in sample:
                self.assertTrue(item in l)

    def test_sample_without_replacement(self):
        for start, stop, step in self.TEST_RANGES:
            l = range(start, stop, step)
            rdd = RDD(list(l), self.SPARK_CONTEXT)
            sample = rdd.sample(False, self.SAMPLE_FRACTION).collect()
            self.assertEquals(len(sample), int(len(l) * self.SAMPLE_FRACTION))
            self.assertEquals(sorted(l), sorted(set(l)))
            for item in sample:
                self.assertTrue(item in l)

    def test_sample_without_replacement_with_seed(self):
        for start, stop, step in self.TEST_RANGES:
            l = range(start, stop, step)
            rdd = RDD(list(l), self.SPARK_CONTEXT)
            sample1 = rdd.sample(False, self.SAMPLE_FRACTION, self.SAMPLE_SEED).collect()
            sample2 = rdd.sample(False, self.SAMPLE_FRACTION, self.SAMPLE_SEED).collect()
            self.assertEquals(sorted(sample1), sorted(sample2))
            sample = sample1
            self.assertEquals(len(sample), int(len(l) * self.SAMPLE_FRACTION))
            self.assertEquals(sorted(l), sorted(set(l)))
            for item in sample:
                self.assertTrue(item in l)

    def test_union(self):
        for start1, stop1, step1 in self.TEST_RANGES:
            for start2, stop2, step2 in self.TEST_RANGES:
                l1 = range(start1, stop1, step1)
                l2 = range(start2, stop2, step2)
                rdd1 = RDD(list(l1), self.SPARK_CONTEXT)
                rdd2 = RDD(list(l2), self.SPARK_CONTEXT)
                rdd = rdd1.union(rdd2)
                self.assertEquals(sorted(rdd.collect()), sorted(list(l1) + list(l2)))

    def test_intersection(self):
        for start1, stop1, step1 in self.TEST_RANGES:
            for start2, stop2, step2 in self.TEST_RANGES:
                l1 = range(start1, stop1, step1)
                l2 = range(start2, stop2, step2)
                rdd1 = RDD(list(l1), self.SPARK_CONTEXT)
                rdd2 = RDD(list(l2), self.SPARK_CONTEXT)
                rdd = rdd1.intersection(rdd2)
                self.assertEquals(sorted(rdd.collect()), sorted([x for x in l1 if x in l2]))

    def test_group_by_key(self):
        l = [(1, 1), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3)]
        rdd = RDD(l, self.SPARK_CONTEXT)
        rdd = rdd.groupByKey()
        r = rdd.collect()
        r = [(kv[0], list(kv[1])) for kv in r]
        self.assertEquals(sorted(r), sorted([(1, [1]), (2, [1, 2]), (3, [1, 2, 3])]))

    def test_reduce_by_key(self):
        l = [(1, 1), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3)]
        rdd = RDD(l, self.SPARK_CONTEXT)
        rdd = rdd.reduceByKey(lambda a, b: a + b)

        print(rdd)

        self.assertEquals(sorted(rdd.collect()), sorted([(1, 1), (2, 3), (3, 6)]))

    def test_cartesian(self):
        for start1, stop1, step1 in self.TEST_RANGES:
            for start2, stop2, step2 in self.TEST_RANGES:
                l1 = range(start1, stop1, step1)
                l2 = range(start2, stop2, step2)
                rdd1 = RDD(list(l1), self.SPARK_CONTEXT)
                rdd2 = RDD(list(l2), self.SPARK_CONTEXT)
                rdd = rdd1.cartesian(rdd2)
                r = rdd.collect()
                self.assertEquals(len(r), len(l1) * len(l2))
                for t, u in r:
                    self.assertTrue(t in l1)
                    self.assertTrue(u in l2)

    def test_cogroup(self):
        l1 = [(1, 1), (2, 1), (2, 2), (3, 1), (3, 2), (3, 3)]
        l2 = [(2, 10), (2, 20), (3, 10), (3, 20), (3, 30), (4, 40)]
        rdd1 = RDD(l1, self.SPARK_CONTEXT)
        rdd2 = RDD(l2, self.SPARK_CONTEXT)
        rdd = rdd1.cogroup(rdd2)
        l = rdd.collect()
        self.assertEquals(
            sorted(l),
            sorted([(1, [1], []), (2, [1, 2], [10, 20]), (3, [1, 2, 3], [10, 20, 30]), (4, [], [40])])
        )

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

    def test_left_outer_join(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd1 = sc.parallelize([('A', [1, 2, 3]), ('B', [2,3,4])])
        rdd2 = sc.parallelize([('A', [1, 2, 3]), ('B', [2,3,4]), ('B', [4,5,6])])
        out = rdd1.leftOuterJoin(rdd2).collect()
        print(out)
        self.assertEqual(len(out), 2)

    def test_keys(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = sc.parallelize([('A', 1), ('B', 2), ('C', 3)])
        self.assertListEqual(rdd.keys().collect(), ['A', 'B', 'C'])

    def test_values(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = sc.parallelize([('A', 1), ('B', 2), ('C', 3)])
        self.assertListEqual(rdd.values().collect(), [1, 2, 3])

    def test_combineByKey(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = sc.parallelize([
            ('A', 1),
            ('B', 2),
            ('B', 3),
            ('C', 4),
            ('C', 5),
            ('A', 6),
        ])

        def create_combiner(a):
            return [a]

        def merge_value(a, b):
            a.append(b)
            return a

        def merge_combiners(a, b):
            a.extend(b)
            return a

        rdd = rdd.combineByKey(create_combiner, merge_value, merge_combiners)
        self.assertListEqual(
            rdd.collect(),
            [('A', [1, 6]), ('B', [2, 3]), ('C', [4, 5])],
        )

    def test_sortByKey_ascending(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = (
            sc.parallelize([
                ('e', 5),
                ('d', 4),
                ('c', 3),
                ('b', 2),
                ('a', 1),
            ])
            .sortByKey(ascending=True)
        )
        self.assertListEqual(
            rdd.collect(),
            [
                ('a', 1),
                ('b', 2),
                ('c', 3),
                ('d', 4),
                ('e', 5),
            ],
        )

    def test_sortByKey_descending(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = (
            sc.parallelize([
                ('a', 1),
                ('b', 2),
                ('c', 3),
                ('d', 4),
                ('e', 5),
            ])
            .sortByKey(ascending=False)
        )
        self.assertListEqual(
            rdd.collect(),
            [
                ('e', 5),
                ('d', 4),
                ('c', 3),
                ('b', 2),
                ('a', 1),
            ],
        )

    def test_sortBy_ascending(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = (
            sc.parallelize([5, 4, 3, 2, 1])
            .sortBy(lambda x: x, ascending=True)
        )
        self.assertListEqual(rdd.collect(), [1, 2, 3, 4, 5])

    def test_sortBy_descending(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = (
            sc.parallelize([1, 2, 3, 4, 5])
            .sortBy(lambda x: x, ascending=False)
        )
        self.assertListEqual(rdd.collect(), [5, 4, 3, 2, 1])

    def test_subtractByKey(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd1 = sc.parallelize([('A', 1), ('B', 2), ('C', 3)])
        rdd2 = sc.parallelize([('A', None), ('C', None)])
        self.assertListEqual(rdd1.subtractByKey(rdd2).collect(), [('B', 2)])

    def test_not_implemented_methods(self):
        sc = SparkContext(master='', conf=SparkConf())
        rdd = sc.parallelize([])

        with self.assertRaises(NotImplementedError):
            rdd._pickled()

        with self.assertRaises(NotImplementedError):
            rdd.mapPartitionsWithIndex(None, None,)

        with self.assertRaises(NotImplementedError):
            rdd._computeFractionForSampleSize(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.pipe(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.reduce(None)

        with self.assertRaises(NotImplementedError):
            rdd.treeReduce(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.fold(None, None,)

        with self.assertRaises(NotImplementedError):
            rdd.aggregate(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.treeAggregate(None, None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.stats()

        with self.assertRaises(NotImplementedError):
            rdd.histogram(None)

        with self.assertRaises(NotImplementedError):
            rdd.variance()

        with self.assertRaises(NotImplementedError):
            rdd.stdev()

        with self.assertRaises(NotImplementedError):
            rdd.sampleStdev()

        with self.assertRaises(NotImplementedError):
            rdd.sampleVariance()

        with self.assertRaises(NotImplementedError):
            rdd.countByValue()

        with self.assertRaises(NotImplementedError):
            rdd.top(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.takeOrdered(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsNewAPIHadoopDataset(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsNewAPIHadoopFile(None, None, None, None, None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsHadoopDataset(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsHadoopFile(None, None, None, None, None, None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsSequenceFile(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsPickleFile(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.saveAsTextFile(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.collectAsMap()

        with self.assertRaises(NotImplementedError):
            rdd.reduceByKeyLocally(None)

        with self.assertRaises(NotImplementedError):
            rdd.countByKey()

        with self.assertRaises(NotImplementedError):
            rdd.join(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.rightOuterJoin(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.fullOuterJoin(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.foldByKey(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd._can_spill()

        with self.assertRaises(NotImplementedError):
            rdd._memory_limit()

        with self.assertRaises(NotImplementedError):
            rdd.groupWith(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.sampleByKey(None, None, None)

        with self.assertRaises(NotImplementedError):
            rdd.subtract(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.coalesce(None, None)

        with self.assertRaises(NotImplementedError):
            rdd.toDebugString()

        with self.assertRaises(NotImplementedError):
            rdd.getStorageLevel()

        with self.assertRaises(NotImplementedError):
            rdd._to_java_object_rdd()



