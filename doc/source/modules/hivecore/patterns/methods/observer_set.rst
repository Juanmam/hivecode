:orphan:

Set
===

.. role:: method
.. role:: param

hivecore.pattern.Observer. :method:`set` ( :param:`key: str, val: Any` )

A setter method, used to store data with an alias.

Example
^^^^^^^

..  code-block:: python
    
    from hivecore.pattern import Observer

    Observer().set("my_variable", 10)