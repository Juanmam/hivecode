from typing import TypeVar

def Singleton(class_: TypeVar('Class')) -> object:
    """
    A decorator used to turn a class into a Singleton. This makes it
    so that if a class has already been instanciated one, that instance
    will be returned instead of a second one.

    Args:
        class_ (Class): A class to apply the singleton to. Applied as a Decorator.

    Returns:
        object: Singleton instance of that Class.
    """
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance
