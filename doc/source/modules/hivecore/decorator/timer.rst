Timer
=====

.. autofunction:: hivecore.decorator.timer
   :noindex:

Basic Example
^^^^^^^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import timer

    @timer
    def suma(n):
        return sum(range(n))

    suma(500000000)

In Miliseconds Example
^^^^^^^^^^^^^^^^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import timer

    @timer(in_milis = True)
    def suma(n):
        return sum(range(n))

    suma(500000000)