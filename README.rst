taskdb
======

TaskDB runs common methods in a single thread

Motivation
----------

Threads sometimes need to run compute-heavy or network-heavy methods
that are expected to return a similar output. Webcrawlers and bots
acting on those crawlers may wish to decrease network load by limiting
requests to a single thread. TaskDB allows multiple threads to ask for
the result of a common function without requiring each thread to execute
the function.

Installation
------------

Install this package through pip:

::

    $ pip install taskdb

Basic Usage
-----------

.. code:: python

    from taskdb import TaskDB

    def some_intesive_method(some_argument, some_other_argument):
        # Do something

    task_db = TaskDB()
    result = task_db.monitor('SOME_TASK_ID', some_intensive_method, (15, 20))
    print(result)