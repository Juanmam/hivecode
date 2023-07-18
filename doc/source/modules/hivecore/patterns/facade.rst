Facade
======

.. autofunction:: hivecore.patterns.Facade

Subsystem
^^^^^^^^^

.. autofunction:: hivecore.patterns.Subsystem

Example
^^^^^^^

To implement the facade pattern, the library provides a system to register new clases as subsystems and a system to facade those subsystems. The Subsystem decorator lets the user define a new subsystem and include it into a group and even map functions between the Facade and the Subsystem using kwargs. On the other hand the Facade decorator works as a facade for all subsystems in a given group. The user can then use the operation method to call methods as needed.

Registering Subsystems
~~~~~~~~~~~~~~~~~~~~~~

We register the following subsystems with the "StreamingFacade" group:

.. code-block:: python

    from hivecore.patterns import Subsystem

    @Subsystem("StreamingFacade")
    class AuthenticationSystem:
        def login(self):
            return "Logging in...\n"

        def logout(self):
            return "Logging out...\n"

    @Subsystem("StreamingFacade", login="signin", logout="signout")
    class BillingSystem:
        def signin(self):
            return "Signing in to BillingSystem...\n"

        def signout(self):
            return "Signing out from BillingSystem...\n"

        def pay(self):
            return "Paying...\n"

    @Subsystem("StreamingFacade")
    class StreamingSystem:
        def fetch_movie(self):
            return "Fetching movie...\n"

        def play_movie(self):
            return "Playing movie...\n"

Creating Facade
~~~~~~~~~~~~~~~

We create a facade class using the "StreamingFacade" group using the Facade decorator:

.. code-block:: python

    from hivecore.patterns import Subsystem, Facade

    @Facade("StreamingFacade")
    class StreamingFacade:
        pass

Performing Operations
~~~~~~~~~~~~~~~~~~~~~

We create an instance of the facade class and perform various operations:

.. code-block:: python

    # Create facade object
    streaming = StreamingFacade()

    # Perform operations
    login_result = streaming.operation("login")
    pay_result = streaming.operation("pay")
    fetch_result = streaming.operation("fetch_movie")
    play_result = streaming.operation("play_movie")
    logout_result = streaming.operation("logout")

    print(login_result)
    print(pay_result)
    print(fetch_result)
    print(play_result)
    print(logout_result)

Output
------

The output of the above example would be:

.. code-block:: text

    Logging in...
    Paying...
    Fetching movie...
    Playing movie...
    Logging out...
