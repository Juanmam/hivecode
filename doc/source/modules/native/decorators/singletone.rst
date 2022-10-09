Singletone
==========

.. role:: method

hivecode.native.decorators. :method:`Singletone`

    Defines a class as a Singleton. This makes it so that trying to 
    instanciate a class more than once, will always return a single
    instance of the class.

..  code-block:: python
    
    @Singleton
    class my_class:
        def __init__(self):
            my_attr = None