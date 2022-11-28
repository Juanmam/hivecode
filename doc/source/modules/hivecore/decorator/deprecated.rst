Deprecated
==========

.. role:: method
.. role:: param

hivecore.decorator. :method:`deprecated` (:param:`reason: str`)

Defines when a callable is deprecated. This will give a warning telling the user that the function is not the best for the case.

Parameters
^^^^^^^^^^
* reason: Message you want to display to the user.

Basic Example
^^^^^^^^^^^^^

..  code-block:: python
    
    from hivecore.decorator import deprecated

    @deprecated
    def old_sum(*numbers):
        total = 0
        for number in numbers:
            total = total + number
        return total

    def new_sum(*numbers):
        return sum(*numbers)


Example Custom Message
^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: python
    
    from hivecore.decorator import deprecated

    @deprecated("An old implementation for sum, not as efficient for big len(numbers).")
    def old_sum(*numbers):
        total = 0
        for number in numbers:
            total = total + number
        return total

    def new_sum(*numbers):
        return sum(*numbers)