from setuptools import setup, find_packages
from codecs import open
from os import path

VERSION = '0.0.3'

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dummyrdd',
    version=VERSION,
    description='A pure python mocked version of pyspark\'s rdd class',
    long_description=long_description,
    url='https://github.com/wdm0006/dummyrdd',
    download_url='https://github.com/wdm0006/dummyrdd/tarball/' + VERSION,
    license='MIT',
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Developers',
      'Programming Language :: Python :: 3',
    ],
    keywords='pyspark spark testing',
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    author='Will McGinnis',
    install_requires=[],
    author_email='will@pedalwrencher.com'
)