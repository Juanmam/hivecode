Factory
=======

.. autofunction:: hivecore.patterns.Factory
   :noindex:

Product
^^^^^^^

.. autofunction:: hivecore.patterns.Product
   :noindex:

Example
^^^^^^^

To implement the factory pattern, the library provides a way to register classes as products and a factory to fabricate those products. The Product decorator is used to register a class as a product in a specific factory. The Factory decorator is used to create a factory that can fabricate the registered products. The user can then use the fabricate method to create instances of the products.

Registering Products
~~~~~~~~~~~~~~~~~~~~

We register the following products with the "ElectronicDevices" factory:

.. code-block:: python

   from hivecore.patterns import Product, Factory

   @Product(factory="ElectronicDevices", constructor="create_smartphone")
   class Smartphone:
       def create_smartphone(self):
           print("Creating a Smartphone")
           return Smartphone()

       def make_call(self, number):
           print(f"Making a call to {number}")

   @Product(factory="ElectronicDevices", constructor="create_laptop")
   class Laptop:
       def create_laptop(self):
           print("Creating a Laptop")
           return Laptop()

       def run_program(self, program_name):
           print(f"Running {program_name} on the laptop")

   @Product(factory="ElectronicDevices")
   class Tablet:
       def __init__(self):
           print("Creating a Tablet")

       def play_game(self, game_name):
           print(f"Playing {game_name} on the tablet")

Creating Factory
~~~~~~~~~~~~~~~~

We create a factory named "ElectronicDeviceFactory" using the Factory decorator:

.. code-block:: python

   @Factory(factory="ElectronicDevices")
   class ElectronicDeviceFactory:
       def __init__(self):
           pass

Using the Factory
~~~~~~~~~~~~~~~~~

We create an instance of the ElectronicDeviceFactory and use it to create different electronic devices:

.. code-block:: python

   # Create an instance of ElectronicDeviceFactory
   device_factory = ElectronicDeviceFactory()

   # Use the factory to create different electronic devices
   smartphone = device_factory.fabricate("create_smartphone")
   smartphone.make_call("1234567890")

   laptop = device_factory.fabricate("create_laptop")
   laptop.run_program("Python")

   tablet = device_factory.fabricate()
   tablet.play_game("Candy Crush")

Output
------

The output of the above example would be:

.. code-block:: text

   Creating a Smartphone
   Making a call to 1234567890
   Creating a Laptop
   Running Python on the laptop
   Creating a Tablet
   Playing Candy Crush on the tablet
