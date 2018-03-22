# -*- coding: utf-8 -*-

import random
import uuid

from collections import OrderedDict
from functools import reduce

from dummy_spark.resultsiterable import ResultIterable

__author__ = 'willmcginnis'


class RDD(object):
    """
    A Resilient Distributed Dataset (RDD) is the basic abstraction in Spark. It represents an immutable, partitioned
    collection of elements that can be operated on in parallel. This is a dummy version of that, that is just a list
    under the hood.  To be used for testing, and maybe development if you play fast and loose.

    Important note: the dummy RDD is NOT lazily loaded.

    """

    def __init__(self, jrdd, ctx, jrdd_deserializer=None):
        """

        :param jrdd:
        :param ctx:
        :param jrdd_deserializer:
        :return:
        """
        # ported
        self._id = str(uuid.uuid4())

        if jrdd is None:
            self._jrdd = []
        else:
            if isinstance(jrdd, list):
                self._jrdd = jrdd
            elif isinstance(jrdd, set):
                self._jrdd = list(jrdd)
            else:
                raise AttributeError('Type %s for jrdd not supported' % (type(jrdd), ))

        self.ctx = ctx
        self.is_cached = True
        self._name = 'dummpy-rdd'

        # not ported

        self.is_checkpointed = False

        self._jrdd_deserializer = jrdd_deserializer
        self.partitioner = None

    def id(self):
        """

        :return:
        """
        return self._id

    @property
    def context(self):
        """

        :return:
        """
        return self.ctx

    def name(self):
        """

        :return:
        """
        return self._name

    def setName(self, name):
        """

        :param name:
        :return:
        """
        self._name = name
        return self

    def __repr__(self):
        """

        :return:
        """
        return str(self._jrdd)

    def cache(self):
        """

        :return:
        """
        return self

    def persist(self, storageLevel=None):
        """

        :param storageLevel:
        :return:
        """
        return self

    def unpersist(self):
        """

        :return:
        """
        return self

    def _reserialize(self, serializer=None):
        """

        :param serializer:
        :return:
        """
        return self

    def checkpoint(self):
        """

        :return:
        """
        pass

    def isCheckpointed(self):
        """

        :return:
        """
        return True

    def getCheckpointFile(self):
        """

        :return:
        """
        return None

    def map(self, f, preservesPartitioning=False):
        """

        :param f:
        :param preservesPartitioning:
        :return:
        """
        data = list(map(f, self._jrdd))
        return RDD(data, self.ctx)

    def flatMap(self, f, preservesPartitioning=False):
        """

        :param f:
        :param preservesPartitioning:
        :return:
        """
        data = [item for sl in map(f, self._jrdd) for item in sl]
        return RDD(data, self.ctx)

    def mapPartitions(self, f, preservesPartitioning=False):
        """

        :param f:
        :param preservesPartitioning:
        :return:
        """
        return self.map(f, preservesPartitioning=preservesPartitioning)

    def getNumPartitions(self):
        """

        :return:
        """
        return 1

    def filter(self, f):
        """

        :param f:
        :return:
        """
        data = list(filter(f, self._jrdd))
        return RDD(data, self.ctx)

    def distinct(self, numPartitions=None):
        """

        :param numPartitions:
        :return:
        """
        data = set(self._jrdd)
        return RDD(data, self.ctx)

    def sample(self, withReplacement, fraction, seed=None):
        """

        :param withReplacement:
        :param fraction:
        :param seed:
        :return:
        """
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction

        if seed is not None:
            random.seed(seed)

        idx_list = list(range(len(self._jrdd)))
        if withReplacement:
            data = [self._jrdd[random.choice(idx_list)] for _ in list(range(int(fraction * len(self._jrdd))))]
        else:
            random.shuffle(idx_list)
            data = [self._jrdd[idx] for idx in idx_list[:int(fraction * len(self._jrdd))]]
        return RDD(data, self.ctx)

    def randomSplit(self, weights, seed=None):
        """

        :param weights:
        :param seed:
        :return:
        """
        pass

    def takeSample(self, withReplacement, num, seed=None):
        """

        :param withReplacement:
        :param num:
        :param seed:
        :return:
        """
        assert num >= 0.0, "Negative sample num: %s" % num

        if seed is not None:
            random.seed(seed)

        if withReplacement:
            out = [self._jrdd[random.choice(list(range(len(self._jrdd))))] for _ in num]
        else:
            idx_list = list(range(len(self._jrdd)))
            random.shuffle(idx_list)
            out = [self._jrdd[idx] for idx in idx_list[:num]]
        return out

    def union(self, other):
        """

        :param other:
        :return:
        """
        return RDD(self._jrdd + other._jrdd, self.ctx)

    def intersection(self, other):
        """

        :param other:
        :return:
        """
        data = [item for item in self._jrdd if item in other._jrdd]
        return RDD(data, self.ctx)

    def __add__(self, other):
        """

        :param other:
        :return:
        """
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    def repartitionAndSortWithinPartitions(self, numPartitions=None, partitionFunc=None, ascending=True, keyfunc=lambda x: x):
        """

        :param numPartitions:
        :param partitionFunc:
        :param ascending:
        :param keyfunc:
        :return:
        """
        data = sorted(self._jrdd, key=keyfunc, reverse=ascending)
        return RDD(data, self.ctx)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        """

        :param ascending:
        :param numPartitions:
        :param keyfunc:
        :return:
        """
        data = sorted(self._jrdd, key=keyfunc, reverse=not ascending)
        return RDD(data, self.ctx)

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        """

        :param keyfunc:
        :param ascending:
        :param numPartitions:
        :return:
        """
        data = sorted(self._jrdd, key=keyfunc, reverse=not ascending)
        return RDD(data, self.ctx)

    def glom(self):
        """

        :return:
        """
        return self._jrdd

    def cartesian(self, other):
        """

        :param other:
        :return:
        """
        data = [(t, u) for t in self._jrdd for u in other._jrdd]
        return RDD(data, self.ctx)

    def groupBy(self, f, numPartitions=None):
        """

        :param f:
        :param numPartitions:
        :return:
        """
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions)

    def foreach(self, f):
        """

        :param f:
        :return:
        """
        return self.map(f)

    def foreachPartition(self, f):
        """

        :param f:
        :return:
        """
        return f(self._jrdd)

    def collect(self):
        """

        :return:
        """
        return self._jrdd

    def sum(self):
        """

        :return:
        """
        return sum(self._jrdd)

    def count(self):
        """

        :return:
        """
        return len(self._jrdd)

    def mean(self):
        """

        :return:
        """
        return float(sum(self._jrdd)) / len(self._jrdd)

    def take(self, num):
        """

        :param num:
        :return:
        """
        return self._jrdd[:num]

    def first(self):
        """

        :return:
        """
        return self._jrdd[0]

    def isEmpty(self):
        """

        :return:
        """
        return len(self._jrdd) == 0

    def reduceByKey(self, func, numPartitions=None):
        """

        :param func:
        :param numPartitions:
        :return:
        """
        keys = {kv[0] for kv in self._jrdd}
        data = [(key, reduce(func, [kv[1] for kv in self._jrdd if kv[0] == key])) for key in keys]

        return RDD(data, self.ctx)

    def groupByKey(self, numPartitions=None):
        """

        :param numPartitions:
        :return:
        """
        keys = {x[0] for x in self._jrdd}
        out = {k: ResultIterable([x[1] for x in self._jrdd if x[0] == k]) for k in keys}
        data = list(out.items())
        return RDD(data, self.ctx)

    def flatMapValues(self, f):
        """

        :param f:
        :return:
        """
        flat_map_fn = lambda kv: ((kv[0], x) for x in f(kv[1]))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def mapValues(self, f):
        """

        :param f:
        :return:
        """
        map_values_fn = lambda kv: (kv[0], f(kv[1]))
        return self.map(map_values_fn, preservesPartitioning=True)

    def cogroup(self, other, numPartitions=None):
        """

        :param other:
        :param numPartitions:
        :return:
        """
        vs = {x[0] for x in self._jrdd}
        us = {x[0] for x in other._jrdd}
        keys = vs.union(us)
        data = [
            (
                k,
                ([v[1] for v in self._jrdd if v[0] == k]),
                ([u[1] for u in other._jrdd if u[0] == k])
            )
            for k in keys
        ]
        return RDD(data, self.ctx)

    def zip(self, other):
        """

        :param other:
        :return:
        """
        data = list(zip(other, self._jrdd))
        return RDD(data, self.ctx)

    def zipWithIndex(self):
        """

        :return:
        """
        data = [(b, a) for a, b in list(enumerate(self._jrdd))]
        return RDD(data, self.ctx)

    def _defaultReducePartitions(self):
        """

        :return:
        """
        return 1

    def lookup(self, key):
        """

        :param key:
        :return:
        """
        return [x for x in self._jrdd if x[0] == key]

    def countApprox(self, timeout, confidence=0.95):
        """

        :param timeout:
        :param confidence:
        :return:
        """
        return len(self._jrdd)

    def sumApprox(self, timeout, confidence=0.95):
        """

        :param timeout:
        :param confidence:
        :return:
        """
        return sum(self._jrdd)

    def meanApprox(self, timeout, confidence=0.95):
        """

        :param timeout:
        :param confidence:
        :return:
        """
        return float(sum(self._jrdd)) / len(self._jrdd)

    def countApproxDistinct(self, relativeSD=0.05):
        """

        :param relativeSD:
        :return:
        """
        return len(set(self._jrdd))

    def toLocalIterator(self):
        """

        :return:
        """
        for row in self._jrdd:
            yield row

    def max(self, key=None):
        """

        :param key:
        :return:
        """
        if key is None:
            return max(self._jrdd)
        else:
            raise NotImplementedError

    def min(self, key=None):
        """

        :param key:
        :return:
        """
        if key is None:
            return min(self._jrdd)
        else:
            raise NotImplementedError

    def _pickled(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        """
        NotImplemented

        :param f:
        :param preservesPartitioning:
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def _computeFractionForSampleSize(sampleSizeLowerBound, total, withReplacement):
        """
        NotImplemented

        :param sampleSizeLowerBound:
        :param total:
        :param withReplacement:
        :return:
        """
        raise NotImplementedError

    def pipe(self, command, env=None):
        """
        NotImplemented

        :param command:
        :param env:
        :return:
        """
        raise NotImplementedError

    def reduce(self, f):
        """
        NotImplemented

        :param f:
        :return:
        """
        raise NotImplementedError

    def treeReduce(self, f, depth=2):
        """
        NotImplemented

        :param f:
        :param depth:
        :return:
        """
        raise NotImplementedError

    def fold(self, zeroValue, op):
        """
        NotImplemented

        :param zeroValue:
        :param op:
        :return:
        """
        raise NotImplementedError

    def aggregate(self, zeroValue, seqOp, combOp):
        """
        NotImplemented

        :param zeroValue:
        :param seqOp:
        :param combOp:
        :return:
        """
        raise NotImplementedError

    def treeAggregate(self, zeroValue, seqOp, combOp, depth=2):
        """
        NotImplemented

        :param zeroValue:
        :param seqOp:
        :param combOp:
        :param depth:
        :return:
        """
        raise NotImplementedError

    def stats(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def histogram(self, buckets):
        """
        NotImplemented

        :param buckets:
        :return:
        """
        raise NotImplementedError

    def variance(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def stdev(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def sampleStdev(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def sampleVariance(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def countByValue(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def top(self, num, key=None):
        """
        NotImplemented

        :param num:
        :param key:
        :return:
        """
        raise NotImplementedError

    def takeOrdered(self, num, key=None):
        """
        NotImplemented

        :param num:
        :param key:
        :return:
        """
        raise NotImplementedError

    def saveAsNewAPIHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        """
        NotImplemented

        :param conf:
        :param keyConverter:
        :param valueConverter:
        :return:
        """
        raise NotImplementedError

    def saveAsNewAPIHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None):
        """
        NotImplemented

        :param path:
        :param outputFormatClass:
        :param keyClass:
        :param valueClass:
        :param keyConverter:
        :param valueConverter:
        :param conf:
        :return:
        """
        raise NotImplementedError

    def saveAsHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        """
        NotImplemented

        :param conf:
        :param keyConverter:
        :param valueConverter:
        :return:
        """
        raise NotImplementedError

    def saveAsHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None):
        """
        NotImplemented

        :param path:
        :param outputFormatClass:
        :param keyClass:
        :param valueClass:
        :param keyConverter:
        :param valueConverter:
        :param conf:
        :param compressionCodecClass:
        :return:
        """
        raise NotImplementedError

    def saveAsSequenceFile(self, path, compressionCodecClass=None):
        """
        NotImplemented

        :param path:
        :param compressionCodecClass:
        :return:
        """
        raise NotImplementedError

    def saveAsPickleFile(self, path, batchSize=10):
        """
        NotImplemented

        :param path:
        :param batchSize:
        :return:
        """
        raise NotImplementedError

    def saveAsTextFile(self, path, compressionCodecClass=None):
        """
        NotImplemented

        :param path:
        :param compressionCodecClass:
        :return:
        """
        raise NotImplementedError

    def collectAsMap(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def keys(self):
        """Get keys of a pair RDD.

        :return: A new RDD that contains only the original pair RDD keys

        """
        return RDD(list(OrderedDict(self._jrdd).keys()), self.ctx)

    def values(self):
        """Get values of a pair RDD.

        :return: A new RDD that contains only the original pair RDD values

        """
        return RDD(list(OrderedDict(self._jrdd).values()), self.ctx)

    def reduceByKeyLocally(self, func):
        """
        NotImplemented

        :param func:
        :return:
        """
        raise NotImplementedError

    def countByKey(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def join(self, other, numPartitions=None):
        """
        NotImplemented

        :param other:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def leftOuterJoin(self, other, numPartitions=None):
        """

        :param other:
        :param numPartitions:
        :return:
        """

        result_dict = {}
        for x in other._jrdd:
            result_dict[x[0]] = result_dict.setdefault(x[0], []) + x[1]

        data = [(k, (v, result_dict.get(k))) for k, v in self._jrdd]
        return RDD(data, self.ctx)

    def rightOuterJoin(self, other, numPartitions=None):
        """
        NotImplemented

        :param other:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def fullOuterJoin(self, other, numPartitions=None):
        """
        NotImplemented

        :param other:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def partitionBy(self, numPartitions, partitionFunc=None):
        """

        :param numPartitions:
        :param partitionFunc:
        :return:
        """

        return self

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None):
        """Generic function to combine the elements for each key using a custom set of aggregation functions.

        Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined
        type" C.

        :param createCombiner:
            which turns a V into a C (e.g., creates a one-element list)
        :param mergeValue:
            to merge a V into a C (e.g., adds it to the end of a list)
        :param mergeCombiners:
            to combine two C’s into a single one (e.g., merges the lists)
        :param numPartitions:
        :return:
        """
        # numPartitions is ignored for now
        # data is split into two partitions anyway,
        # to exercise the mergeCombiners function
        split_point = len(self._jrdd) // 2
        partitions = [
            self._jrdd[:split_point],
            self._jrdd[split_point:],
        ]

        def combine_partition_by_key(partition):
            """Combine all values in a given partition."""
            pairs = OrderedDict()
            for key, value in partition:
                if key not in pairs:
                    pairs[key] = createCombiner(value)
                else:
                    pairs[key] = mergeValue(pairs[key], value)
            return pairs

        combined_partitions = [
            combine_partition_by_key(partition)
            for partition in partitions
        ]

        def merge_combined_partitions(partition_a, partition_b):
            """Merge all values between two partitions."""
            merged_partition = partition_a.copy()
            for key, combiner_b in partition_b.items():
                if key not in partition_a:
                    merged_partition[key] = combiner_b
                else:
                    combiner_a = partition_a[key]
                    merged_partition[key] = mergeCombiners(
                        combiner_a, combiner_b)
            return merged_partition

        data = reduce(merge_combined_partitions, combined_partitions)
        return RDD(list(data.items()), self.ctx)

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None):
        """
        NotImplemented

        :param zeroValue:
        :param seqFunc:
        :param combFunc:
        :param numPartitions:
        :return:
        """

        new_data = {row[0]: zeroValue for row in self._jrdd}
        for row in self._jrdd:
            new_data[row[0]] = seqFunc(new_data[row[0]], row[1])

        new_data = [(k, v) for k, v in new_data.items()]
        rdd = RDD(new_data, self.ctx)
        return rdd

    def foldByKey(self, zeroValue, func, numPartitions=None):
        """
        NotImplemented

        :param zeroValue:
        :param func:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def _can_spill(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def _memory_limit(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def groupWith(self, other, *others):
        """
        NotImplemented

        :param other:
        :param others:
        :return:
        """
        raise NotImplementedError

    def sampleByKey(self, withReplacement, fractions, seed=None):
        """
        NotImplemented

        :param withReplacement:
        :param fractions:
        :param seed:
        :return:
        """
        raise NotImplementedError

    def subtractByKey(self, other, numPartitions=None):
        """Remove the keys in a pair RDD found in another pair RDD.
        NotImplemented

        :param other: The pair RDD whose keys will be removed.
        :param numPartitions:
        :return: A new pair RDD with keys found in the other pair RDD removed.
        """
        self_pairs = OrderedDict(self._jrdd)
        other_pairs = dict(other._jrdd)

        new_keys = set(self_pairs.keys()) - set(other_pairs.keys())
        return RDD(
            [
                (key, value)
                for key, value in self_pairs.items()
                if key in new_keys
            ],
            self.ctx,
        )

    def subtract(self, other, numPartitions=None):
        """
        NotImplemented

        :param other:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def keyBy(self, f):
        """

        :param f:
        :return:
        """
        data = [(f(x), x) for x in self._jrdd]
        return RDD(data, self.ctx)

    def repartition(self, numPartitions):
        """

        :param numPartitions:
        :return:
        """
        return self

    def coalesce(self, numPartitions, shuffle=False):
        """
        NotImplemented

        :param numPartitions:
        :param shuffle:
        :return:
        """
        raise NotImplementedError

    def zipWithUniqueId(self):
        """

        :return:
        """
        data = [(b, a) for a, b in list(enumerate(self._jrdd))]
        return RDD(data, self.ctx)

    def toDebugString(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def getStorageLevel(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def _to_java_object_rdd(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError
