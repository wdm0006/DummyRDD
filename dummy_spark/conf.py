# -*- coding: utf-8 -*-

__author__ = 'willmcginnis'
__all__ = ['SparkConf']


class SparkConf(object):

    DEBUG_STRING = 'no string for dummy version'

    def __init__(self, loadDefaults=True, _jvm=None, _jconf=None):
        self.conf = {}

    def set(self, key, value):
        self.conf.update({key: value})
        return self

    def setIfMissing(self, key, value):
        v = self.conf.get(key, None)
        if v is None:
            self.set(key, value)

        return self

    def setMaster(self, value):
        self.set('master', value)
        return self

    def setAppName(self, value):
        self.set('appName', value)
        return self

    def setSparkHome(self, value):
        self.set('sparkHome', value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        if key is not None and value is not None:
            self.set(key, value)

        if pairs is not None:
            for k, v in pairs:
                self.set(k, v)

        return self

    def setAll(self, pairs):
        if pairs is not None:
            for k, v in pairs:
                self.set(k, v)

        return self

    def get(self, key, defaultValue=None):
        return self.conf.get(key, defaultValue)

    def getAll(self):
        return [(k, v) for k, v in self.conf.items()]

    def contains(self, key):
        return key in self.conf.keys()

    def toDebugString(self):
        return self.DEBUG_STRING
