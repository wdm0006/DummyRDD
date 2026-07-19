# -*- coding: utf-8 -*-

import unittest
import uuid
import sys
from dummy_spark import SparkConf


class SparkConfTests (unittest.TestCase):
    if sys.version_info[0] == 2:
        RANDOM_KEY = str(uuid.uuid4().get_hex().upper()[0:6])
        RANDOM_VALUE = str(uuid.uuid4().get_hex().upper()[0:6])
        RANDOM_KEY2 = str(uuid.uuid4().get_hex().upper()[0:6])
        RANDOM_VALUE2 = str(uuid.uuid4().get_hex().upper()[0:6])
    elif sys.version_info[0] == 3:
        RANDOM_KEY = str(uuid.uuid4().hex.upper()[0:6])
        RANDOM_VALUE = str(uuid.uuid4().hex.upper()[0:6])
        RANDOM_KEY2 = str(uuid.uuid4().hex.upper()[0:6])
        RANDOM_VALUE2 = str(uuid.uuid4().hex.upper()[0:6])

    def test_named_properties(self):
        conf = SparkConf()

        conf.setMaster(self.RANDOM_VALUE)
        self.assertEqual(conf.get('master'), self.RANDOM_VALUE)

        conf.setAppName(self.RANDOM_VALUE)
        self.assertEqual(conf.get('appName'), self.RANDOM_VALUE)

        conf.setSparkHome(self.RANDOM_VALUE)
        self.assertEqual(conf.get('sparkHome'), self.RANDOM_VALUE)

    def test_set_if_missing(self):
        conf = SparkConf()
        conf.set(self.RANDOM_KEY, self.RANDOM_VALUE)
        conf.setIfMissing(self.RANDOM_KEY, self.RANDOM_VALUE2)
        self.assertEqual(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)
        conf.setIfMissing(self.RANDOM_KEY2, self.RANDOM_VALUE2)
        self.assertEqual(conf.get(self.RANDOM_KEY2), self.RANDOM_VALUE2)

    def test_set_executor_env1(self):
        conf = SparkConf()
        conf.setExecutorEnv(key=self.RANDOM_KEY, value=self.RANDOM_VALUE)
        self.assertEqual(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)

    def test_set_executor_env2(self):
        conf = SparkConf()
        conf.setExecutorEnv(
            key=self.RANDOM_KEY,
            value=self.RANDOM_VALUE,
            pairs=[(self.RANDOM_KEY2, self.RANDOM_VALUE2)]
        )
        self.assertEqual(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)
        self.assertEqual(conf.get(self.RANDOM_KEY2), self.RANDOM_VALUE2)

    def test_set_all(self):
        conf = SparkConf()
        conf.setAll(
            pairs=[(self.RANDOM_KEY, self.RANDOM_VALUE),
                   (self.RANDOM_KEY2, self.RANDOM_VALUE2)]
        )
        self.assertEqual(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)
        self.assertEqual(conf.get(self.RANDOM_KEY2), self.RANDOM_VALUE2)

    def test_contains(self):
        conf = SparkConf()
        conf.setAll(
            pairs=[(self.RANDOM_KEY, self.RANDOM_VALUE),
                   (self.RANDOM_KEY2, self.RANDOM_VALUE2)]
        )
        self.assertTrue(conf.contains(self.RANDOM_KEY))
        self.assertTrue(conf.contains(self.RANDOM_KEY2))

    def test_get_all(self):
        conf = SparkConf()
        pairs = [(self.RANDOM_KEY, self.RANDOM_VALUE),
               (self.RANDOM_KEY2, self.RANDOM_VALUE2)]
        conf.setAll(pairs)
        self.assertEqual(sorted(conf.getAll()), sorted(pairs))

    def test_to_debug_string(self):
        conf = SparkConf()
        self.assertEqual(conf.toDebugString(), SparkConf.DEBUG_STRING)
