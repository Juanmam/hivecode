Rate Limit
==========

.. autofunction:: hivecore.decorator.rate_limited
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import rate_limited
    import random
    
    @rate_limited(2, 5)
    def random_value():
        value = random.randint(1, 5)
        print(value)
        if value == 3:
            pass
            #raise ValueError("Value cannot be 3")
        return value