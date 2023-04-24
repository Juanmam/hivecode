Single Use
==========

.. autofunction:: hivecore.decorator.single_use
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.decorator import single_use
    from hivecore.function import generate_key

    unique_seed = 69

    key = generate_key(seed=unique_seed)
    
    @single_use(seed=unique_seed)
    def fun(key=None):
        return "Hello World"

    fun(key=key) #"Hello World"
    fun(key=key) # ValueError

If your have a key paramater called something different from key, you can specify this using the unique_key parameter. Here is a small example of how to use it.

..  code-block:: python
    
    from hivecore.decorator import single_use
    from hivecore.function import generate_key

    @single_use(seed=unique_seed, key_param="my_key")
    def fun(my_key=None):
        return "Hello World"