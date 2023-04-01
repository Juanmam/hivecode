Hivecode
========
Hivecode is a Python library designed as a toolkit for data engineering and data science. The library includes a collection of useful functions and tools for reading, mounting, and storing data in Azure DataBricks. Hivecode also includes patterns, decorators, and analytics tools to help make your data processing workflow more efficient and streamlined. With Hivecode, you can simplify your data engineering and analysis tasks, freeing up your time to focus on more strategic work. Whether you're a data engineer, data analyst, or data scientist, Hivecode has something to offer.

Packages
========
- Hivecore: A set of many vanilla python tools.
- Hiveadb: A set of tools to work in Azure Databricks.
- Hivesignal: A set of tools for signal analysis.

Installation
============
To install the library, you will need pip.

|

.. code-block::

    pip install hivecode

|

.. note::
     Hivecode is constantly changing as new versions try to make it a more stable library. It is highly recommended to specify the version you want to import.

|

Requirements
------------
Hivecode requires Python 3.8.0+ to run.

Development
===========
Requirements
------------
Build is used to build the packages that will be deployed into pypi.

|

.. code-block::

    pip install build

|

Twine is used to deploy packages to pypi.

|

.. code-block::

    pip install twine

|
    
Sphinx is used to build the documentation.

|

.. code-block::

    pip install Sphinx

|

Develop
-------
You can build the project by running the following console command.

|

.. code-block::

    python -m build

|

You can then use twine to upload the newest version to Pypi.

|

.. code-block::

    py -m twine upload --skip-existing --repository pypi dist/* -u {User} -p {password}

|

I personally recommend to cascade both commands using a pipe, like this.

|

.. code-block::

    python -m build | py -m twine upload --skip-existing --repository pypi dist/* -u {User} -p {password}

|

When working with documentations, you will need to run it like this.

|

.. code-block::

    .\doc\make.bat html

|
