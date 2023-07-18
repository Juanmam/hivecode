Prototype
=========

.. autofunction:: hivecore.patterns.prototype
   :noindex:

Example
^^^^^^^

To implement the prototype pattern, the library provides a decorator to convert a class into a prototype. The prototype class encapsulates the logic of registering objects, unregistering objects, and cloning objects with attribute updates. The user can register objects, clone them with updated attributes, and access the methods of the cloned objects.

Creating Prototype
~~~~~~~~~~~~~~~~~~

We create a prototype class using the ``prototype`` decorator:

.. code-block:: python

   from hivecore.patterns import prototype

   @prototype
   class CarPrototype:
       def __init__(self, brand, color):
           self.brand = brand
           self.color = color

       def start_engine(self):
           print(f"Starting the engine of {self.brand} car...")

       def drive(self):
           print(f"Driving the {self.color} {self.brand} car...")

Registering and Cloning Objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We create an instance of the CarPrototype and register it as an object in the prototype:

.. code-block:: python

   # Create an instance of the CarPrototype
   car = CarPrototype("Toyota", "Red")

   # Register the car instance as an object in the prototype
   car.register_object("Car1", car)

   # Clone the registered car object and update its attributes
   car_clone = car.clone("Car1", color="Blue")

Accessing Cloned Object
~~~~~~~~~~~~~~~~~~~~~~~

We can access the attributes and methods of the cloned object:

.. code-block:: python

   # The cloned car object will have the updated attribute
   print(car_clone.brand)  # Output: Toyota
   print(car_clone.color)  # Output: Blue

   # The cloned car object will have the same methods as the original car
   car_clone.start_engine()  # Output: Starting the engine of Toyota car...
   car_clone.drive()  # Output: Driving the Blue Toyota car...

Output
------

The output of the above example would be:

.. code-block:: text

   Toyota
   Blue
   Starting the engine of Toyota car...
   Driving the Blue Toyota car...
   