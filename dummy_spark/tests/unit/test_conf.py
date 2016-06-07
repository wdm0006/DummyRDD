# -*- coding: utf-8 -*-

import unittest
import uuid

from dummy_spark import SparkConf


class SparkConfTests (unittest.TestCase):

    RANDOM_KEY = str(uuid.uuid4().get_hex().upper()[0:6])
    RANDOM_VALUE = str(uuid.uuid4().get_hex().upper()[0:6])

    RANDOM_KEY2 = str(uuid.uuid4().get_hex().upper()[0:6])
    RANDOM_VALUE2 = str(uuid.uuid4().get_hex().upper()[0:6])

    def test_named_properties(self):
        conf = SparkConf()

        conf.setMaster(self.RANDOM_VALUE)
        self.assertEquals(conf.get('master'), self.RANDOM_VALUE)

        conf.setAppName(self.RANDOM_VALUE)
        self.assertEquals(conf.get('appName'), self.RANDOM_VALUE)

        conf.setSparkHome(self.RANDOM_VALUE)
        self.assertEquals(conf.get('sparkHome'), self.RANDOM_VALUE)

    def test_set_if_missing(self):
        conf = SparkConf()
        conf.set(self.RANDOM_KEY, self.RANDOM_VALUE)
        conf.setIfMissing(self.RANDOM_KEY, self.RANDOM_VALUE2)
        self.assertEquals(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)

    def test_set_executor_env1(self):
        conf = SparkConf()
        conf.setExecutorEnv(self.RANDOM_KEY, self.RANDOM_VALUE)
        self.assertEquals(conf.get(self.RANDOM_KEY), self.RANDOM_VALUE)
