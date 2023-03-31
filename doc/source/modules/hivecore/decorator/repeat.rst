Repeat
======

.. autofunction:: hivecore.decorator.repeat
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import repeat
    
    @repeat(10)
    def print_hello():
        print('Hello')
        
    print_hello()