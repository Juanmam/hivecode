Visitor
=======

.. autofunction:: hivecore.patterns.visitor
   :noindex:

Visitable
^^^^^^^^^

.. autofunction:: hivecore.patterns.visitable
   :noindex:

Example
^^^^^^^

To create visitable classes, use the `@visitable` decorator and specify the visitor keys associated with the class.

.. code-block:: python

    from hivecore.patterns import visitable

    @visitable('Startup', 'Shutdown')
    class CPU:
        pass

    @visitable('Startup', 'Shutdown', 'Reset')
    class Memory:
        pass

    @visitable('Startup', 'Shutdown')
    class Disk:
        pass

Creating Visitor Classes
~~~~~~~~~~~~~~~~~~~~~~~~

To create visitor classes, use the `@visitor` decorator and specify the visitor key associated with the visitable classes to visit.

.. code-block:: python

    from hivecore.patterns import visitor

    @visitor('Startup')
    class StartOperation:
        pass

    @visitor('Shutdown')
    class ShutdownOperation:
        pass

    @visitor('Reset')
    class ResetOperation:
        pass

Visiting Visitable Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~

To visit the visitable classes, create instances of both visitable and visitor objects. Use the `accept` method of the visitable objects to accept a visitor.

.. code-block:: python

    # Create visitable and visitor objects
    cpu = CPU()
    memory = Memory()
    disk = Disk()

    startup = StartOperation()
    shutdown = ShutdownOperation()
    reset = ResetOperation()

    # Use the accept method of the visitable objects
    cpu.accept(startup)
    cpu.accept(shutdown)
    cpu.accept(reset)
    memory.accept(reset)
    disk.accept(startup)

Output
------

The output of the above example would be:

.. code-block:: text

    StartOperation visited CPU
    ShutdownOperation visited CPU
    ResetOperation visited an unknown visitable
    ResetOperation visited Memory
    StartOperation visited Disk