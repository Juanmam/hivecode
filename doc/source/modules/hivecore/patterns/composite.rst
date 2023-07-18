Composite
=========

.. autofunction:: hivecore.patterns.composite
   :noindex:

Leaf
^^^^

.. autofunction:: hivecore.patterns.leaf
   :noindex:

Example
^^^^^^^

.. code-block:: python

    from hivecore.decorator import composite, leaf

    @composite("Pets", ["eat", "drink", "sleep"])
    class PetComposite:
        pass

    @composite("Cars", ["drive", "park"])
    class CarComposite:
        pass

    @leaf("Pets", ["eat", "drink", "sleep"])
    class Cat:
        def eat(self):
            print("Cat is eating")

        def drink(self):
            print("Cat is drinking")

        def sleep(self):
            print("Cat is sleeping")

    @leaf("Pets", ["eat", "drink", "sleep"])
    class Dog:
        def eat(self):
            print("Dog is eating")

        def drink(self):
            print("Dog is drinking")

        def sleep(self):
            print("Dog is sleeping")

    @leaf("Cars")
    class Car:
        def drive(self):
            print("Car is driving")

        def park(self):
            print("Car is parking")

    print("-" * 100)
    print("EXAMPLE: Case 1")
    print("-" * 100)

    # Now, you can create a composite of pets and call their methods:
    pet_composite = PetComposite()
    pet_composite.add(Cat())
    pet_composite.add(Dog())
    pet_composite.eat()
    pet_composite.sleep()

    print("-" * 100)
    print("EXAMPLE: Case 2")
    print("-" * 100)

    # And also a composite of cars:
    car_composite = CarComposite()
    car_composite.add(Car())
    car_composite.add(Car())
    car_composite.add(Car())
    car_composite.drive()
    car_composite.park()

Output
------

.. code-block:: text

    ----------------------------------------------------------------------------------------------------
    EXAMPLE: Case 1
    ----------------------------------------------------------------------------------------------------
    Cat is eating
    Dog is eating
    Cat is sleeping
    Dog is sleeping
    ----------------------------------------------------------------------------------------------------
    EXAMPLE: Case 2
    ----------------------------------------------------------------------------------------------------
    Car is driving
    Car is driving
    Car is driving
    Car is parking
    Car is parking
    Car is parking
