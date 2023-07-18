Strategy
========

.. autofunction:: hivecore.patterns.Strategy
   :noindex:

Context
^^^^^^^

.. autofunction:: hivecore.patterns.Context
   :noindex:

ConcreteStrategy
^^^^^^^^^^^^^^^^

.. autofunction:: hivecore.patterns.ConcreteStrategy
   :noindex:

Example
^^^^^^^

To implement the strategy pattern, the library provides decorators to define strategies and a context class to set and execute the chosen strategy. The decorators allow for multi-method strategies or single-method strategies. The example demonstrates the usage of both types of strategies.

Defining Strategies
~~~~~~~~~~~~~~~~~~~

We define a multi-method strategy using the ``@ConcreteStrategy`` decorator and the ``outside_strategy`` function:

.. code-block:: python

    def outside_strategy(self, *args, **kwargs):
        print("Executing outside_strategy")
        print("Args:", args)
        print("Kwargs:", kwargs)

    @ConcreteStrategy({"process_a": outside_strategy, "process_b": "internal_strategy_a"})
    class ConcreteStrategyA:
        def internal_strategy_a(self, *args, **kwargs):
            print("Executing ConcreteStrategyA's internal_strategy")
            print("Args:", args)
            print("Kwargs:", kwargs)

We define a single-method strategy using the ``@ConcreteStrategy`` decorator and the ``internal_strategy`` method:

.. code-block:: python

    @ConcreteStrategy("internal_strategy", "my_strategy")
    class ConcreteStrategyA:
        def internal_strategy(self, *args, **kwargs):
            print("Executing ConcreteStrategyA's internal_strategy")
            print("Args:", args)
            print("Kwargs:", kwargs)

Setting and Executing Strategies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We create a context class using the ``@Context`` decorator:

.. code-block:: python

    @Context
    class Context_:
        def __init__(self):
            pass

We create an instance of the context class and set the strategy to an instance of ``ConcreteStrategyA``:

.. code-block:: python

    context = Context_()
    context.set_strategy(ConcreteStrategyA())
    print(context.__dir__())
    context.process_a("Argument 1", "Argument 2", key1="Value 1", key2="Value 2")
    context.process_b("Argument 1", "Argument 2", key1="Value 1", key2="Value 2")

Output
------

.. code-block:: text

    ----------------------------------------------------------------------------------------------------
    EXAMPLE: Multi Method
    ----------------------------------------------------------------------------------------------------
    ['strategy', 'internal_strategy_a', 'process_a', 'process_b', '__module__', '__init__', '__dict__', '__weakref__', '__doc__', 'set_strategy', '__new__', '__repr__', '__hash__', '__str__', '__getattribute__', '__setattr__', '__delattr__', '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__', '__reduce_ex__', '__reduce__', '__subclasshook__', '__init_subclass__', '__format__', '__sizeof__', '__dir__', '__class__']
    Executing ConcreteStrategyA's process_b
    Executing ConcreteStrategyA's process_b
    ----------------------------------------------------------------------------------------------------
    EXAMPLE: Single Method
    ----------------------------------------------------------------------------------------------------
    Executing ConcreteStrategyA's internal_strategy
    Args: ('Argument 1', 'Argument 2')
    Kwargs: {'key1': 'Value 1', 'key2': 'Value 2'}