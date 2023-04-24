generate_key
============

.. autofunction:: hivecore.function.generate_key
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.function import generate_key
    
    generate_key(1234) #'m0P8A'
    
    generate_key(5678, charset='all') #'48XH7&$+IZ'