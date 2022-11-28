lib_required
============

.. role:: method
.. role:: param

hivecore.function. :method:`lib_required` (:param:`lib_name: str`)

Checks if a library is installed and tries to install it if it's not.

Parameters
^^^^^^^^^^
* lib_name: The name of the library as a string.

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.function import check_library
    
    lib_required('pandas')
    from pandas import DataFrame