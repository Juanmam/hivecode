:orphan:

Get
===

.. role:: method
.. role:: param

hivecore.pattern.Observer. :method:`get` ( :param:`key: str` )

A getter method, used to return data given an alias.

Example
^^^^^^^

..  code-block:: python
    
    from hivecore.pattern import Observer

    Observer().get("my_variable")