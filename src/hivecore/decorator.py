import functools
import inspect
import warnings

from functools import wraps
from time import time, sleep, perf_counter
from typing import List, Tuple, Union

"""
The hivecore.decorator module includes multiple decorators that can be used to enhance classes and functions.
"""

def singleton( class_: type ) -> object:
    """
    A decorator used to turn a class into a Singleton. This makes it so that if a class has already been instanciated one, that instance will be returned instead of a second one.

    :param class_: A class to apply the singleton to. Applied as a Decorator.
    :type class_: Class
    :return: Singleton instance of that Class.
    :rtype: Object
    """
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


def timer(in_milis: bool = True) -> Tuple[any, float]:
    """
    A decorator that can measure how long a function takes to run.
    
    :param in_milis: A flag to determine if the result should be returned as milis or in seconds. Default: False 
    :type in_milis: Optional[bool], optional
    :return: Tuple of (function result, execution time)
    :rtype: Tuple
    """
    def decorator(function) -> Tuple[any, float]:
        def wrapper(*args, **kwargs):
            # Get time before execution.
            start_time = round(time() * 1000)

            # Execute
            output = function(*args, **kwargs)

            # Get time after execution
            end_time = round(time() * 1000)

            execution_time = end_time - start_time

            if in_milis:
                return (output, execution_time)
            else:
                print('in')
                return (output, execution_time/1000)
        return wrapper
    
    if not isinstance(in_milis, bool):
        function = in_milis
        in_milis = False
        return decorator(function)
    
    return decorator


def repeat(number_of_times: int) -> List[any]:
    """
    A decorator that will repeat a function `n` times.
    
    :param number_of_times: Number of times to repeat the function.
    :type number_of_times: int
    :return: A list of results from each execution.
    :rtype: List[Any]
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            results = list()
            
            for _ in range(number_of_times):
                results.append(func(*args, **kwargs))
                
            return results
        return wrapper
    return decorate


def retry(num_retries: int, exception_to_check: Exception = Exception, sleep_time: Union[int, float] = 0):
    """
    Decorator that retries the execution of a function if it raises a specific exception.
    
    :param num_retries: Number of times to retry running the function.
    :type num_retries: int
    :param exception_to_check: Exception type to look for. Default: Exception.
    :type exception_to_check: Optional[Type[Exception]], optional
    :param sleep_time: Time to sleep between excecutions. Default: 0.
    :type sleep_time: Optional[Union[int, float]], optional
    :return: The result from the function execution.
    :rtype: Any
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, num_retries+1):
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    print(f"{func.__name__} raised {e.__class__.__name__}. Retrying...")
                    if i < num_retries:
                        sleep(sleep_time)
            # Raise the exception if the function was not successful after the specified number of retries
            raise e
        return wrapper
    return decorate


def rate_limited(max_per_second: int, period: Union[int, float] = 1) -> any:
    """
    A decorator that limits the amount of times you can call a function.
    
    :param max_per_second: Max number of runs per second.
    :type max_per_second: int
    :param period: Time between runs, in seconds. Default to 1. 
    :type period: Optional[Union[int, float]], optional
    :return: The result from the function execution.
    :rtype: Any
    """
    min_interval = period / float(max_per_second)
    def decorate(func):
        last_time_called = [0.0]
        @wraps(func)
        def rate_limited_function(*args, **kargs):
            elapsed = perf_counter() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                sleep(left_to_wait)
            ret = func(*args, **kargs)
            last_time_called[0] = perf_counter()
            return ret
        return rate_limited_function
    return decorate


def deprecated( reason: str = None ):
    """
    A decorator which can be used to mark functions as deprecated. It will result in a warning being emitted when the function is used.

    :param reason: The reason why this function/class is deprecated
    :type reason: Optional[str], optional
    :raises TypeError: This error will probably happen you pass a reason that is not a string type. Not passing it will also work.
    :return: A function or a class type.
    :rtype: Callable
    """

    string_types = (type(b''), type(u''))

    if isinstance(reason, string_types):

        # The @deprecated is used with a 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated("please, use another function")
        #    def old_function(x, y):
        #      pass

        def decorator(func1):
            if inspect.isclass(func1):
                fmt1 = "Call to deprecated class {name} ({reason})."
            else:
                fmt1 = "Call to deprecated function {name} ({reason})."

            @functools.wraps(func1)
            def new_func1(*args, **kwargs):
                warnings.simplefilter('always', DeprecationWarning)
                warnings.warn(
                    fmt1.format(name=func1.__name__, reason=reason),
                    category=DeprecationWarning,
                    stacklevel=2
                )
                warnings.simplefilter('default', DeprecationWarning)
                return func1(*args, **kwargs)

            return new_func1

        return decorator
    elif inspect.isclass(reason) or inspect.isfunction(reason):

        # The @deprecated is used without any 'reason'.
        #
        # .. code-block:: python
        #
        #    @deprecated
        #    def old_function(x, y):
        #      pass
    
        func2 = reason

        if inspect.isclass(func2):
            fmt2 = "Call to deprecated class {name}."
        else:
            fmt2 = "Call to deprecated function {name}."

        @functools.wraps(func2)
        def new_func2(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)
            warnings.warn(
                fmt2.format(name=func2.__name__),
                category=DeprecationWarning,
                stacklevel=2
            )
            warnings.simplefilter('default', DeprecationWarning)
            return func2(*args, **kwargs)

        return new_func2
    else:
        raise TypeError(repr(type(reason)))
