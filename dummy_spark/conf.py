# -*- coding: utf-8 -*-

__author__ = 'willmcginnis'
__all__ = ['SparkConf']


class SparkConf(object):

    DEBUG_STRING = 'no string for dummy version'

    def __init__(self, loadDefaults=True, _jvm=None, _jconf=None):
        """

        :param loadDefaults:
        :param _jvm:
        :param _jconf:
        :return:
        """
        self.conf = {}

    def set(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        self.conf.update({key: value})
        return self

    def setIfMissing(self, key, value):
        """

        :param key:
        :param value:
        :return:
        """
        v = self.conf.get(key, None)
        if v is None:
            self.set(key, value)

        return self

    def setMaster(self, value):
        """

        :param value:
        :return:
        """
        self.set('master', value)
        return self

    def setAppName(self, value):
        """

        :param value:
        :return:
        """
        self.set('appName', value)
        return self

    def setSparkHome(self, value):
        """

        :param value:
        :return:
        """
        self.set('sparkHome', value)
        return self

    def setExecutorEnv(self, key=None, value=None, pairs=None):
        """

        :param key:
        :param value:
        :param pairs:
        :return:
        """
        if key is not None and value is not None:
            self.set(key, value)

        if pairs is not None:
            for k, v in pairs:
                self.set(k, v)

        return self

    def setAll(self, pairs):
        """

        :param pairs:
        :return:
        """
        if pairs is not None:
            for k, v in pairs:
                self.set(k, v)

        return self

    def get(self, key, defaultValue=None):
        """

        :param key:
        :param defaultValue:
        :return:
        """
        return self.conf.get(key, defaultValue)

    def getAll(self):
        """

        :return:
        """
        return [(k, v) for k, v in self.conf.items()]

    def contains(self, key):
        """

        :param key:
        :return:
        """
        return key in self.conf.keys()

    def toDebugString(self):
        """

        :return:
        """
        return self.DEBUG_STRING
