:orphan:

Delete
======

.. role:: method
.. role:: param

hivecore.pattern.Observer. :method:`delete` ( :param:`key: str` )

A delete method, used to remove a key, value pair.

Example
^^^^^^^

..  code-block:: python
    
    from hivecore.pattern import Observer

    Observer().delete("my_variable")