# -*- coding: utf-8 -*-

from __future__ import print_function

import time
import sys

from threading import Lock

from dummy_spark.rdd import RDD

try:
    import tinys3
    has_tinys3 = True
except ImportError as e:
    has_tinys3 = False


__all__ = ['SparkContext']
__author__ = 'willmcginnis'


class hadoopConfiguration(object):
    def __init__(self):
        pass

    def set(self, a, b):
        setattr(self, a, b)
        return True

    def get(self, a):
        return getattr(self, a, None)


class jvm(object):
    def __init__(self):
        self.hc = hadoopConfiguration()

    def hadoopConfiguration(self):
        return self.hc

    def textFile(self, file_name):
        if file_name.startswith('s3'):
            if has_tinys3:
                file_name = file_name.split('://')[1]
                bucket_name = file_name.split('/')[0]
                key_name = file_name.replace(bucket_name, '')[1:]
                access_key = self.hc.get('fs.s3n.awsAccessKeyId')
                secret_key = self.hc.get('fs.s3n.awsSecretAccessKey')
                conn = tinys3.Connection(access_key, secret_key)
                file = conn.get(key_name, bucket_name)
                return file.content.decode('utf-8').split('\n')
            else:
                raise Exception('Need TinyS3 to use s3 files')
        else:
            with open(file_name, 'r') as f:
                return f.readlines()


class SparkContext(object):

    """
    Main entry point for Spark functionality. A SparkContext represents the
    connection to a Spark cluster, and can be used to create L{RDD} and
    broadcast variables on that cluster.
    """

    _gateway = None
    _jvm = None
    _next_accum_id = 0
    _active_spark_context = None
    _lock = Lock()
    _jsc = jvm()
    _python_includes = None  # zip and egg files that need to be added to PYTHONPATH

    PACKAGE_EXTENSIONS = ('.zip', '.egg', '.jar')

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None, environment=None, batchSize=0, serializer=None, conf=None, gateway=None, jsc=None, profiler_cls=None):
        self._callsite = None
        SparkContext._ensure_initialized(self, gateway=gateway)
        self.start_time = time.time()
        try:
            self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, jsc, profiler_cls)
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _do_init(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, jsc, profiler_cls):
        # TODO: add included files to python path
        return

    def _initialize_context(self, jconf):
        return None

    @classmethod
    def _ensure_initialized(cls, instance=None, gateway=None):
        return True

    def __enter__(self):
        return self

    def __exit__(self, type, value, trace):
        self.stop()

    def setLogLevel(self, logLevel):
        pass

    @classmethod
    def setSystemProperty(cls, key, value):
        pass

    @property
    def version(self):
        return 'dummy version'

    @property
    def startTime(self):
        return self.start_time

    @property
    def defaultParallelism(self):
        return 1

    @property
    def defaultMinPartitions(self):
        return 1

    def stop(self):
        pass

    def emptyRDD(self):
        return RDD([], self, None)

    def range(self, start, end=None, step=1, numSlices=None):
        return RDD(list(range(start, end, step)), self, None)

    def parallelize(self, c, numSlices=None):
        return RDD(c, self, None)

    def textFile(self, name, minPartitions=None, use_unicode=True):
        data = self._jsc.textFile(name)
        rdd = RDD(data, self, None)
        return rdd

    def pickleFile(self, name, minPartitions=None):
        raise NotImplementedError

    def wholeTextFiles(self, path, minPartitions=None, use_unicode=True):
        raise NotImplementedError

    def binaryFiles(self, path, minPartitions=None):
        raise NotImplementedError

    def binaryRecords(self, path, recordLength):
        raise NotImplementedError

    def _dictToJavaMap(self, d):
        raise NotImplementedError

    def sequenceFile(self, path, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, minSplits=None, batchSize=0):
        raise NotImplementedError

    def newAPIHadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None, batchSize=0):
        raise NotImplementedError

    def newAPIHadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None, batchSize=0):
        raise NotImplementedError

    def hadoopFile(self, path, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None, batchSize=0):
        raise NotImplementedError

    def hadoopRDD(self, inputFormatClass, keyClass, valueClass, keyConverter=None, valueConverter=None, conf=None, batchSize=0):
        raise NotImplementedError

    def _checkpointFile(self, name, input_deserializer):
        raise NotImplementedError

    def union(self, rdds):
        raise NotImplementedError

    def broadcast(self, value):
        raise NotImplementedError

    def accumulator(self, value, accum_param=None):
        raise NotImplementedError

    def addFile(self, path):
        raise NotImplementedError

    def clearFiles(self):
        raise NotImplementedError

    def addPyFile(self, path):
        sys.path.append(path)

    def setCheckpointDir(self, dirName):
        raise NotImplementedError

    def _getJavaStorageLevel(self, storageLevel):
        raise NotImplementedError

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        raise NotImplementedError

    def setLocalProperty(self, key, value):
        raise NotImplementedError

    def getLocalProperty(self, key):
        raise NotImplementedError

    def sparkUser(self):
        raise NotImplementedError

    def cancelJobGroup(self, groupId):
        raise NotImplementedError

    def cancelAllJobs(self):
        raise NotImplementedError

    def statusTracker(self):
        raise NotImplementedError

    def runJob(self, rdd, partitionFunc, partitions=None, allowLocal=False):
        raise NotImplementedError

    def show_profiles(self):
        raise NotImplementedError

    def dump_profiles(self, path):
        raise NotImplementedError
