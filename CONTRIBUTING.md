Contributing Code
=================

DummyRDD is a library intended to mock out the functionality of pyspark in pure python for testing and low-barrier-to-entry
experimentation. It's biased pretty heavily towards the particular portions of the API that the developers need at any
given time, because the spark API is pretty large and rapidly being developed further.

With that in mind, we welcome and in fact would love some help.

How to Contribute
=================

The preferred workflow to contribute to DummyRDD is:

 1. Fork this repository into your own github account.
 2. Clone the fork on your account onto your local disk:
 
    $ git clone git@github.com:YourLogin/DummyRDD.git
    $ cd DummyRDD
    
 3. Create a branch for your new awesome feature, do not work in the master branch:
 
    $ git checkout -b new-awesome-feature
    
 4. Write some code, or docs, or tests.
 5. When you are done, submit a pull request.
 
Running Tests
=============

Test code coverage is still low right now, so help out by writing tests for new code. There are also some open issues pertaining
 to unit testing, so if you're interested in a quick way to make an impact on the project, check those out. 
 
To run the tests, use:

    $ nosetests --with-coverage --cover-package=dummy_spark
    $ coverage html
    
Easy Issues / Getting Started
=============================

There are an enormous amount of NotImplimented functions in the RDD class, and documentation/testing are pretty nearly 
non-existent.  Any help in rectifying any of these would be greatly appreciated, so open an issue or pull request and we
can help you get started.
