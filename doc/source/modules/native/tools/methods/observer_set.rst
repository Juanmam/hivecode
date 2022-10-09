:orphan:
Set
===

.. role:: method
.. role:: param

hivecode.native.tools.Observer. :method:`set` ( :param:`key: str, val: Any` )

    A setter method, used to store data with an alias.

Example
^^^^^^^

..  code-block:: python
    
    from hivecode.native.tools import Observer

    Observer().set("my_variable", 10)