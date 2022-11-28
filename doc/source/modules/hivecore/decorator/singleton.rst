Singleton
=========

.. role:: method

hivecore.decorator. :method:`Singleton`

Defines a class as a Singleton. This makes it so that trying to instanciate a class more than once, will always return a single instance of the class.

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import Singleton

    @Singleton
    class my_class:
        def __init__(self):
            my_attr = None