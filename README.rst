.. |PyPI version Hivecode| image:: https://img.shields.io/pypi/v/hivecode.svg
   :target: https://pypi.org/project/hivecode/

.. |Documentation Status| image:: https://readthedocs.org/projects/hivecode/badge/?version=latest
   :target: http://hivecode.readthedocs.io/?badge=latest

.. |MIT license| image:: https://img.shields.io/badge/License-MIT-blue.svg
   :target: https://lbesson.mit-license.org/

.. |PyPI license| image:: https://img.shields.io/pypi/l/hivecode.svg
   :target: https://pypi.org/project/hivecode/

Hivecode
========
|PyPI version Hivecode| |Documentation Status| |MIT license|

Hivecode is a versatile and comprehensive Python library, with a focus on efficiency and reusability, Hivecode empowers developers and data enthusiasts alike to streamline their projects. This library boasts a wide array of practical and innovative functions, ranging from seamless integration with Azure Databricks to implementing essential design patterns, data preprocessing, visualization, exploration, and more. Whether you're looking to supercharge your data analysis or accelerate your Python projects, Hivecode is the go-to toolkit for unlocking the full potential of your endeavors.

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
