LibraryManager
==============

.. autoclass:: hiveadb.functions.LibraryManager
    :members: install, uninstall

Example
-------

To use the `LibraryManager` class, simply call it as follows:

.. code-block:: python

    from hiveadb.functions import LibraryManager

    manager = LibraryManager('user_token')

    manager.install(library="numpy") # Install a library from PyPI

    manager.install(library="com.crealytics:spark-excel_2.13", version="3.3.1_0.18.7", index="maven")  # Install a library from maven

    manager.uninstall(library="pandas") # Uninstall Library

Expected Output
---------------

Installs numpy in the cluster, installs spark-excel from Maven Central and then uninstalls pandas.