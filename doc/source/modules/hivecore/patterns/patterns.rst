Patterns
========

.. automodule:: hivecore.patterns

.. toctree::
    :maxdepth: 1
    :glob:

    observer
    singleton
    factory
    composite
    strategy
    prototype
    facade
    visitor
    state
    adapter
    responsability

This section describes various design patterns and provides examples and use cases for each pattern.

Observer Pattern
----------------

.. image:: /images/Observer_Pattern.png
   :alt: Observer Pattern Diagram
   :align: center

The Observer pattern defines a one-to-many dependency between objects, where multiple observers are notified when the subject's state changes.

Use Cases:
    - Event-driven systems where multiple components need to react to state changes.
    - GUI frameworks for handling user interface events and updates.

Singleton Pattern
-----------------

.. image:: /images/Singleton_Pattern.png
   :alt: Singleton Pattern Diagram
   :align: center

The Singleton pattern ensures that a class has only one instance and provides a global point of access to that instance.

Use Cases:
    - Managing global configuration settings.
    - Database connection pool.
    - Logging systems.

Factory Pattern
---------------

.. image:: /images/Factory_Pattern.png
   :alt: Factory Pattern Diagram
   :align: center

The Factory pattern provides an interface for creating objects, but allows subclasses to decide which class to instantiate.

Use Cases:
    - Object creation when the exact class is not known beforehand.
    - Creating objects based on user input or configuration.

Composite Pattern
-----------------

.. image:: /images/Composite_Pattern.png
   :alt: Composite Pattern Diagram
   :align: center

The Composite pattern composes objects into tree structures to represent part-whole hierarchies. It allows clients to treat individual objects and compositions uniformly.

Use Cases:
    - Representing hierarchical structures such as file systems, directories, and menus.
    - Graphics frameworks for building complex graphical elements.

Strategy Pattern
----------------

.. image:: /images/Strategy_Pattern.png
   :alt: Strategy Pattern Diagram
   :align: center

The Strategy pattern defines a family of interchangeable algorithms and encapsulates each algorithm to make them interchangeable. It allows the algorithm to vary independently from clients using it.

Use Cases:
    - Sorting algorithms where the choice of algorithm can vary.
    - Payment processing systems with different payment strategies (credit card, PayPal, etc.).

Prototype Pattern
-----------------

.. image:: /images/Prototype_Pattern.png
   :alt: Prototype Pattern Diagram
   :align: center

The Prototype pattern creates objects by cloning an existing object rather than creating new instances. It provides a way to create objects based on existing instances.

Use Cases:
    - Creating objects that are expensive to create or initialize.
    - Building a cache of pre-configured objects.

Facade Pattern
--------------

.. image:: /images/Facade_Pattern.png
   :alt: Facade Pattern Diagram
   :align: center

The Facade pattern provides a simplified interface to a complex system of classes, providing a higher-level interface that makes it easier to use the system.

Use Cases:
    - Hiding complex subsystems behind a simple interface for client code.
    - Libraries and frameworks that wrap complex functionality.

Visitor Pattern
---------------

.. image:: /images/Visitor_Pattern.png
   :alt: Visitor Pattern Diagram
   :align: center

The Visitor pattern allows adding new operations to an object structure without modifying the objects themselves. It separates the algorithms from the objects on which they operate.

Use Cases:
    - Object structures with a fixed set of classes but varying operations.
    - Compilers, interpreters, and abstract syntax trees.

State Pattern
-------------

.. image:: /images/State_Pattern.png
   :alt: State Pattern Diagram
   :align: center

The State pattern allows an object to alter its behavior when its internal state changes. It encapsulates state-specific behavior into separate classes and makes the object's behavior appear as if it changes at runtime.

Use Cases:
    - Stateful workflows and state machines.
    - Game development for handling different game states (start, play, pause, etc.).

Adapter Pattern
---------------

.. image:: /images/Adapter_Pattern.png
   :alt: Adapter Pattern Diagram
   :align: center

The Adapter pattern allows objects with incompatible interfaces to work together by wrapping the incompatible object with a compatible interface.

Use Cases:
    - Integrating existing systems with new systems.
    - Reusing existing classes that don't have the desired interface.

Responsibility Pattern
----------------------

.. image:: /images/Chain_of_responsability_Pattern.png
   :alt: Responsibility Pattern Diagram
   :align: center

The Responsibility pattern decouples senders and receivers by giving multiple objects a chance to handle a request. It avoids coupling the sender of a request to its receiver, giving multiple objects the opportunity to handle the request.

Use Cases:
    - Handling user input in a chain of command.
    - Event-driven systems with multiple event handlers.
