Retry
=====

.. autofunction:: hivecore.decorator.retry
   :noindex:


Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import retry

    import random
    @retry(num_retries=3)
    def random_value():
        value = random.randint(1, 5)
        if value == 3:
            raise ValueError("Value cannot be 3")
        return value

    random_value()