
Chain of Responsabilities
=========================

.. autofunction:: hivecore.patterns.chain_of_responsibility
   :noindex:

Responsable
^^^^^^^^^^^

.. autofunction:: hivecore.patterns.responsable
   :noindex:

Example
^^^^^^^

The `@responsable` decorator helps define responsabilities for the chain of responsabilities.

.. code-block:: python

    from hivecore.patterns import responsable

    @responsable("currency")
    class Hundredresponsable:
        def handle(self, amount):
            if amount >= 100:
                num = amount // 100
                remainder = amount % 100
                print(f"Dispensing {num} $100 note")
                return remainder
            return amount

    @responsable("currency")
    class Fiftyresponsable:
        def handle(self, amount):
            if amount >= 50:
                num = amount // 50
                remainder = amount % 50
                print(f"Dispensing {num} $50 note")
                return remainder
            return amount

    @responsable("currency")
    class Twentyresponsable:
        def handle(self, amount):
            if amount >= 20:
                num = amount // 20
                remainder = amount % 20
                print(f"Dispensing {num} $20 note")
                return remainder
            return amount

Creating a chain of responsabilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `@chain_of_responsibility` decorator helps define the class that is going to orchastrate the responsabilities. Note that execution order is defined by the order the responsabilities are defined unless the order parameter is used for the `@responsable` decorator.

.. code-block:: python

    from hivecore.patterns import chain_of_responsibility

    @chain_of_responsibility("currency")
        class ATMDispenser:
            def dispense(self, amount):
                self.handle_request(amount)

Usage
~~~~~

.. code-block:: python

    # Create chain object
    atm = ATMDispenser()

    # Perform operations
    atm.dispense(580)

Output
------

.. code-block:: text

    Dispensing 5 $100 note
    Dispensing 1 $50 note
    Dispensing 1 $20 note