Deprecated
==========

.. autofunction:: hivecore.decorator.deprecated
   :noindex:

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