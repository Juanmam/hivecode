check_library
=============

.. role:: method
.. role:: param

hivecore.function. :method:`check_library` (:param:`lib_name: str`)

Returns True if a library is installed, False if not.

Parameters
^^^^^^^^^^
* lib_name: The name of the library as a string.

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.function import check_library

    check_library('pandas')