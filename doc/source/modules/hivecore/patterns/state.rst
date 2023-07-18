State
=====

.. autofunction:: hivecore.patterns.state

State Context
^^^^^^^^^^^^^

.. autofunction:: hivecore.patterns.state_context

Example
^^^^^^^

To create state classes, use the `@state` decorator and specify a unique state key associated with each class.

.. code-block:: python

    from hivecore.patterns import state

    @state("red")
    class Red:
        def handle(self, context):
            print("Red light - stop!")
            context.set_state("green")

    @state("yellow")
    class Yellow:
        def handle(self, context):
            print("Yellow light - get ready!")
            context.set_state("red")

    @state("green")
    class Green:
        def handle(self, context):
            print("Green light - go!")
            context.set_state("yellow")

Creating State Context
~~~~~~~~~~~~~~~~~~~~~~

To create the state context class, use the `@state_context` decorator and specify the initial state key.

.. code-block:: python

    from hivecore.patterns import state_context

    @state_context("red")
    class TrafficLight:
        def change(self):
            self.request()

Performing Operations
~~~~~~~~~~~~~~~~~~~~~

To perform operations on the state context object, create an instance of the state context class and call the desired method (e.g., `change()`). This will trigger the appropriate state transition and invoke the corresponding `handle()` method.

.. code-block:: python

    # Create context object
    traffic_light = TrafficLight()

    # Perform operations
    traffic_light.change()  # Outputs: Red light - stop!
    traffic_light.change()  # Outputs: Green light - go!
    traffic_light.change()  # Outputs: Yellow light - get ready!
    traffic_light.change()  # Outputs: Red light - stop!

Output
~~~~~~

The output of the above example would be:

.. code-block:: text

    Red light - stop!
    Green light - go!
    Yellow light - get ready!
    Red light - stop!