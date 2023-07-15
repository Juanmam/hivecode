from functools import wraps, reduce
from typing import Any
from typing import Type, Optional, Dict, List, Callable, Any
from abc import ABC, abstractmethod


def singleton( class_: type ) -> object:
    """
    A decorator used to turn a class into a Singleton. This makes it so that if a class has already been instanciated one, that instance will be returned instead of a second one.

    :param class_: A class to apply the singleton to. Applied as a Decorator.
    :type class_: Class
    :return: Singleton instance of that Class.
    :rtype: Object
    """
    instances = {}

    @wraps(class_)
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class Observer:
    """
    An observer class. This type of class let's you to add attributes, fetch their values and remove them dynamiclly. Quite useful to access data inside a defined scope as it behaves as a Singleton.
    """

    def set(self, key: str, val: Any) -> None:
        """
        Let's you add and overwrite attributes given the name and value. You could also overwrite it by just accessing the value, but that may not be recommended as later versions may change attributes to be encapsulated.

        :param key: Name of the attribute to create/modify.
        :type key: str

        :param val: Value of the attribute to create/modify.
        :type val: Any
        
        :return: None
        :rtype: None
        """
        setattr(self, key, val)

    def get(self, key: str) -> Any:
        """
        Let's you fetch data from the Observer. Current version lets you access data directly, but later versions may not support this, so the use of this method is highly recommended.

        :param key: Name of the attribute you want to fetch.
        :type key: str
        
        :return: The value stored in the attribute.
        :rtype: Any
        """
        return getattr(self, key)

    def delete(self, key: str) -> None:
        """
        Let's you remove data from the Observer.

        :param key: Name of the attribute to remove.
        :type key: str
        
        :return: None
        :rtype: None
        """
        delattr(self, key)


# A dictionary to store registered factories
factory_registry: Dict[str, List[Dict[str, str]]] = {}


def Product(factory: Optional[str] = None, constructor: Optional[str] = None) -> Type:
    """
    Decorator to register a class as a product in a factory.

    :param factory: The name of the factory to associate the class with. If not provided, the class name will be used.
    :type factory: Optional[str]
    :param constructor: The name of the constructor to use for the class. If not provided, "init" will be used.
    :type constructor: Optional[str]
    :return: The class decorated with the product registration.
    :rtype: Type
    """

    def decorator(cls: Type) -> Type:
        # Get the factory name, either from the provided parameter or using the class name
        factory_name = factory or cls.__name__

        # If the factory doesn't exist in the registry, create an empty list for it
        if factory_name not in factory_registry:
            factory_registry[factory_name] = []

        # Get the constructor name, either from the provided parameter or using the default "init"
        constructor_name = constructor or "init"

        # Add the class and constructor information to the factory registry
        factory_registry[factory_name].append({"class": cls, "constructor": constructor_name})

        @wraps(cls)
        def wrapper(*args, **kwargs):
            return cls(*args, **kwargs)
        return wrapper
    return decorator


def Factory(factory: Optional[str] = None) -> Callable[[Type], Type]:
    """
    Decorator to associate a class with a factory and add a `fabricate` method to create products.

    :param factory: The name of the factory to associate the class with. If not provided, the class name will be used.
    :type factory: Optional[str]
    :return: The class decorated with the factory association and `fabricate` method.
    :rtype: Callable[[Type], Type]
    """

    def decorator(cls: Type) -> Type:
        # Get the factory name, either from the provided parameter or using the class name
        factory_name = factory or cls.__name__

        # Retrieve the factory dictionary from the registry
        factory_dict = factory_registry.get(factory_name)

        @wraps(cls)
        def wrapper(*args, **kwargs):
            # Create an instance of the class
            instance = cls(*args, **kwargs)

            def create_product(constructor_name: str = "init") -> Type:
                """
                Create a product using the specified constructor name.

                :param constructor_name: The name of the constructor to use for creating the product.
                                         Defaults to "init".
                :type constructor_name: str
                :return: The created product instance.
                :rtype: Type
                """
                for product in factory_dict:
                    if product["constructor"] == constructor_name:
                        return product["class"](*args, **kwargs)
                if constructor_name == "init":
                    return cls(*args, **kwargs)
                else:
                    raise ValueError(f"Unknown constructor: {constructor_name}")

            # Add the `fabricate` method to the instance
            setattr(instance, "fabricate", create_product)

            return instance
        return wrapper
    return decorator


def Strategy(cls):
    return cls

def Context(cls):
    # Check if the class has already been decorated
    if hasattr(cls, 'set_strategy'):
        # Remove the added method
        delattr(cls, 'set_strategy')

    def set_strategy(self, strategy):
        self.strategy = strategy
        for name in dir(strategy):
            if not name.startswith('__'):
                attr = getattr(strategy, name)
                if callable(attr):
                    setattr(self, name, attr)
    cls.set_strategy = set_strategy
    return cls


from typing import Type, Callable, Optional, Union


def ConcreteStrategy(methods: Union[str, Callable, dict], custom_strategy: Optional[str] = None) -> Callable[[Type], Type]:
    """
    Decorator to add strategy methods to a class, allowing it to dynamically switch behavior.

    :param methods: The strategy method(s) to add. Can be a single method, a dictionary of multiple methods, or a string
                 representing a method name.
    :type methods: Union[str, Callable, dict]
    :param custom_strategy: The custom name for the strategy method. Only applicable when methods is a single method.
    :type custom_strategy: Optional[str]
    :return: The class decorated with the added strategy method(s).
    :rtype: Callable[[Type], Type]
    """

    def undecorate(cls: Type) -> None:
        """
        Remove any existing decorated methods from the class.

        :param cls: The class to undecorate.
        :type cls: Type
        """
        if hasattr(cls, '_decorated_methods'):
            for name in cls._decorated_methods:
                delattr(cls, name)
            del cls._decorated_methods

    def decorator(cls: Type) -> Type:
        undecorate(cls)  # Remove any existing decorated methods
        cls._decorated_methods = set()  # Track the names of the methods we're adding

        if isinstance(methods, str) or callable(methods):
            # Single method case
            method = methods
            custom_name = custom_strategy

            def execute(self, *args, **kwargs) -> None:
                """
                Execute the strategy method.

                :param self: The instance of the class.
                :type self: Any
                :param args: Positional arguments to pass to the strategy method.
                :type args: Tuple
                :param kwargs: Keyword arguments to pass to the strategy method.
                :type kwargs: Dict
                """
                if isinstance(method, str):
                    if hasattr(self, method):
                        method_func = getattr(self, method)
                        method_func(*args, **kwargs)
                    else:
                        print(f"Executing {self.__class__.__name__}'s execute")
                elif callable(method):
                    method(self, *args, **kwargs)
                else:
                    raise TypeError("Invalid execute method provided")

            setattr(cls, custom_name or 'execute', execute)
            cls._decorated_methods.add(custom_name or 'execute')
        elif isinstance(methods, dict):
            # Multiple methods case
            for name, execute_method in methods.items():

                def method(self, execute_method=execute_method, *args, **kwargs) -> None:
                    """
                    Execute the strategy method.

                    :param self: The instance of the class.
                    :type self: Any
                    :param args: Positional arguments to pass to the strategy method.
                    :type args: Tuple
                    :param kwargs: Keyword arguments to pass to the strategy method.
                    :type kwargs: Dict
                    """
                    if isinstance(execute_method, str):
                        if hasattr(self, execute_method):
                            method_func = getattr(self, execute_method)
                            method_func(*args, **kwargs)
                        else:
                            print(f"Executing {self.__class__.__name__}'s {name}")
                    elif callable(execute_method):
                        execute_method(self, *args, **kwargs)
                    else:
                        raise TypeError("Invalid execute method provided")

                setattr(cls, name, method)
                cls._decorated_methods.add(name)

        return cls

    return decorator


