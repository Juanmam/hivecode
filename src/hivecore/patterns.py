from functools import wraps, reduce
from typing import Any
from typing import Type, Optional, Dict, List, Callable, Any, Union
from abc import ABC, abstractmethod
import copy
from collections import defaultdict

def singleton( class_: type ) -> object:
    """
    A decorator used to turn a class into a Singleton. This makes it so that if a class has already been instanciated one, that instance will be returned instead of a second one.

    :param class_: A class to apply the singleton to. Applied as a Decorator.
    :type class_: Class
    :return: Singleton instance of that Class.
    :rtype: Object
    """
    
    instances = {}
    class SingletonWrapper(class_):
        def _new_(cls, *args, **kwargs):
            if class_ not in instances:
                instances[class_] = super(SingletonWrapper, cls)._new_(cls)
            return instances[class_]
    return SingletonWrapper


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


def Strategy(cls: Type) -> Type:
    """
    Decorator to indicate that a class is a strategy.

    :param cls: The class to decorate.
    :type cls: Type
    :return: The decorated class indicating it as a strategy.
    :rtype: Type
    """
    return cls


def Context(cls: Type) -> Type:
    """
    Decorator to add a `set_strategy` method to a class, allowing it to set and inherit behavior from a strategy.

    :param cls: The class to decorate.
    :type cls: Type
    :return: The class decorated with the `set_strategy` method.
    :rtype: Type
    """

    # Check if the class has already been decorated
    if hasattr(cls, 'set_strategy'):
        # Remove the added method
        delattr(cls, 'set_strategy')

    def set_strategy(self, strategy) -> None:
        """
        Set the strategy for the context and inherit behavior from the strategy.

        :param self: The instance of the class.
        :type self: Any
        :param strategy: The strategy to set and inherit behavior from.
        :type strategy: Any
        """
        self.strategy = strategy
        for name in dir(strategy):
            if not name.startswith('__'):
                attr = getattr(strategy, name)
                if callable(attr):
                    setattr(self, name, attr)

    cls.set_strategy = set_strategy
    return cls


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


composite_registry: Dict[str, Type['CompositeClass']] = {}


class ComponentAbsClass(ABC):
    """
    Abstract base class representing the component in the composite pattern.
    """
    @abstractmethod
    def __init__(self) -> None:
        super().__init__()


class LeafClass(ComponentAbsClass):
    """
    Leaf class representing the individual components in the composite pattern.
    """
    def __init__(self, instance: Any):
        """
        Initialize the LeafClass.

        :param instance: The instance representing the leaf component.
        :type instance: Any
        """
        self.instance = instance


class CompositeClass(ComponentAbsClass):
    """
    Composite class representing the composite component in the composite pattern.
    """
    def __init__(self):
        """
        Initialize the CompositeClass.
        """
        self.children = []

    def add(self, component: ComponentAbsClass):
        """
        Add a child component to the composite.

        :param component: The child component to add.
        :type component: ComponentAbsClass
        """
        self.children.append(component)

    def remove(self, component: ComponentAbsClass):
        """
        Remove a child component from the composite.

        :param component: The child component to remove.
        :type component: ComponentAbsClass
        """
        self.children.remove(component)


def composite(name: str, methods: List[str]) -> Callable[[Type], Type]:
    """
    Decorator to create a composite class and dynamically add operation methods.

    :param name: The name of the composite class.
    :type name: str
    :param methods: The list of operation methods to add to the composite class.
    :type methods: List[str]
    :return: The decorated composite class.
    :rtype: Callable[[Type], Type]
    """
    def decorator(cls: Type) -> Type:
        # Create a new subclass of Composite with dynamic operation methods
        CompositeSubclass = type(name, (CompositeClass,), {})

        # Define operation methods dynamically
        for method in methods:
            def operation(self, method=method):
                """
                Perform the operation on the composite by executing the corresponding method on child components.

                :param self: The instance of the composite class.
                :type self: CompositeSubclass
                :param method: The name of the method to execute on child components.
                :type method: str
                """
                for child in self.children:
                    if hasattr(child.instance, method):
                        getattr(child.instance, method)()

            setattr(CompositeSubclass, method, operation)

        # Register the composite class
        composite_registry[name] = CompositeSubclass

        return CompositeSubclass

    return decorator


def leaf(composite_name: str = None, methods: List[str] = None) -> Callable[[Type], Type]:
    """
    Decorator to create a leaf class and optionally link it with a composite class.

    :param composite_name: The name of the composite class to link the leaf class with.
                           If not provided, the leaf class won't be associated with any composite.
    :type composite_name: str, optional
    :param methods: The list of operation methods to add to the leaf class when linking it with a composite.
                    Ignored if composite_name is not provided.
    :type methods: List[str], optional
    :return: The decorated leaf class.
    :rtype: Callable[[Type], Type]
    """
    def decorator(cls: Type) -> Type:
        # Create a new subclass of Leaf with a dynamic operation method
        def init_method(self, instance=None):
            """
            Initialize the LeafSubclass.

            :param self: The instance of the leaf class.
            :type self: LeafSubclass
            :param instance: The instance representing the leaf component.
                             If not provided, create a new instance of the original class.
            :type instance: Any, optional
            """
            LeafClass.__init__(self, instance or cls())

        LeafSubclass = type(cls.__name__, (LeafClass,), {"__init__": init_method})

        if composite_name and methods:
            # Link the leaf class with the appropriate composite class
            CompositeClass = composite_registry.get(composite_name)
            if CompositeClass:
                for method_name in methods:
                    if hasattr(cls, method_name):
                        def operation(self, method_name=method_name):
                            """
                            Perform the operation on the linked composite by executing the corresponding method
                            on the leaf instance.

                            :param self: The instance of the leaf class.
                            :type self: LeafSubclass
                            :param method_name: The name of the method to execute on the leaf instance.
                            :type method_name: str
                            """
                            method = getattr(self.instance, method_name)
                            method()

                        setattr(LeafSubclass, method_name, operation)
        return LeafSubclass

    return decorator


def prototype(cls):
    """
    Decorator to convert a class into a Prototype by creating a new PrototypeClass that encapsulates
    the logic of registering objects, unregistering objects, and cloning objects with attribute updates.

    :param cls: The class to decorate and convert into a Prototype.
    :type cls: class
    :return: The decorated class, now functioning as a Prototype.
    :rtype: class
    """
    class PrototypeClass(cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._objects = {}

        def register_object(self, name, obj):
            """
            Register an object with the Prototype.

            :param name: The name associated with the object.
            :type name: str
            :param obj: The object to register.
            :type obj: Any
            """
            self._objects[name] = obj

        def unregister_object(self, name):
            """
            Unregister an object from the Prototype.

            :param name: The name of the object to unregister.
            :type name: str
            """
            del self._objects[name]

        def clone(self, name, **attr):
            """
            Clone a registered object from the Prototype and update its attributes.

            :param name: The name of the object to clone.
            :type name: str
            :param attr: Keyword arguments to update the cloned object's attributes.
            :type attr: Any
            :return: The cloned object with updated attributes.
            :rtype: Any
            """
            obj = copy.deepcopy(self._objects[name])
            obj.__dict__.update(attr)
            return obj

    prototype = PrototypeClass

    return prototype


class SubsystemRegistry:
    """
    SubsystemRegistry class responsible for registering and retrieving subsystems.
    """
    _registry: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def register(cls, facade_name: str, subsystem_name: str, subsystem_instance: Any, operation_mapping: Dict[str, str]) -> None:
        """
        Register a subsystem with the SubsystemRegistry.

        :param facade_name: The name of the facade associated with the subsystem.
        :type facade_name: str
        :param subsystem_name: The name of the subsystem.
        :type subsystem_name: str
        :param subsystem_instance: The instance of the subsystem.
        :type subsystem_instance: Any
        :param operation_mapping: The mapping of operations to methods in the subsystem.
        :type operation_mapping: Dict[str, str]
        """
        if facade_name not in cls._registry:
            cls._registry[facade_name] = {}
        cls._registry[facade_name][subsystem_name] = (subsystem_instance, operation_mapping)

    @classmethod
    def get_subsystems(cls, facade_name: str) -> Dict[str, Any]:
        """
        Get the subsystems associated with a facade from the SubsystemRegistry.

        :param facade_name: The name of the facade.
        :type facade_name: str
        :return: The dictionary of subsystems associated with the facade.
        :rtype: Dict[str, Any]
        """
        return cls._registry.get(facade_name, {})


def Subsystem(facade_name: str, **operation_to_method: str):
    """
    Decorator to register a class as a subsystem and associate it with a facade.

    :param facade_name: The name of the facade to associate the subsystem with.
    :type facade_name: str
    :param operation_to_method: Keyword arguments representing the mapping of operations to methods in the subsystem.
    :type operation_to_method: Dict[str, str]
    :return: The decorated class.
    :rtype: class
    """
    def decorator(cls: Any) -> Any:
        subsystem_instance = cls()
        SubsystemRegistry.register(facade_name, cls.__name__, subsystem_instance, operation_to_method)
        return cls
    return decorator


def Facade(facade_name: str):
    """
    Decorator to create a facade wrapper class that interacts with registered subsystems.

    :param facade_name: The name of the facade.
    :type facade_name: str
    :return: The decorated class acting as a facade.
    :rtype: class
    """
    def decorator(cls: Any) -> Any:
        class FacadeWrapper(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.subsystems = SubsystemRegistry.get_subsystems(facade_name)

            def operation(self, operations: List[str]) -> str:
                """
                Perform the specified operations on the registered subsystems and return the results.

                :param operations: The list of operations to perform.
                :type operations: List[str]
                :return: The results of performing the operations.
                :rtype: str
                """
                if isinstance(operations, str):
                    operations = [operations]

                results = ""
                for operation_name in operations:
                    for subsystem_name, (subsystem, mapping) in self.subsystems.items():
                        method_name = mapping.get(operation_name, operation_name)
                        method = getattr(subsystem, method_name, None)
                        if method:
                            results += method()
                        else:
                            results += f"No operation named '{operation_name}' in subsystem '{subsystem_name}'\n"
                return results
        return FacadeWrapper
    return decorator


def visitable(*visitor_keys: str) -> Callable:
    """
    Decorator to mark a class as visitable by visitors.

    :param visitor_keys: The keys associated with the visitors that can visit the class.
    :type visitor_keys: str
    :return: The decorated class.
    :rtype: class
    """
    def decorator(cls: type) -> type:
        def accept(self, visitor) -> None:
            """
            Accept a visitor to visit the class.

            :param visitor: The visitor to accept.
            :type visitor: object
            """
            method_name = 'visit_' + cls.__name__
            method = getattr(visitor, method_name, visitor.visit_default)
            return method(self)

        cls.accept = accept
        cls.visitor_keys = visitor_keys or []
        return cls
    return decorator


def visitor(visitor_key: str = None) -> Callable:
    """
    Decorator to create a visitor class for visiting visitable classes.

    :param visitor_key: The key associated with the visitable classes to visit (default: class name).
    :type visitor_key: str
    :return: The decorated class acting as a visitor.
    :rtype: class
    """
    def decorator(cls: type) -> type:
        visitor_key_used = visitor_key or cls.__name__
        for visitable in (c for c in globals().values() if isinstance(c, type) and hasattr(c, 'visitor_keys') and visitor_key_used in c.visitor_keys):
            def visit(self, visitable) -> None:
                """
                Visit a visitable class.

                :param visitable: The visitable class to visit.
                :type visitable: object
                """
                print(f'{cls.__name__} visited {visitable.__class__.__name__}')

            setattr(cls, f'visit_{visitable.__name__}', visit)

        def visit_default(self, visitable) -> None:
            """
            Visit a visitable class when no specific visitor method is found.

            :param visitable: The visitable class to visit.
            :type visitable: object
            """
            print(f'{cls.__name__} visited an unknown visitable')

        setattr(cls, 'visit_default', visit_default)
        return cls
    return decorator


state_registry: Dict[str, Any] = {}


def state(state_key: str):
    """
    Decorator to register a class as a state.

    :param state_key: The key associated with the state.
    :type state_key: str
    :return: The decorated class.
    :rtype: class
    """
    def decorator(cls: type) -> type:
        state_registry[state_key] = cls()
        return cls

    return decorator


def state_context(initial_state_key: str):
    """
    Decorator to create a context wrapper class that maintains the current state.

    :param initial_state_key: The key associated with the initial state.
    :type initial_state_key: str
    :return: The decorated class acting as a context wrapper.
    :rtype: class
    """
    def decorator(cls: type) -> type:
        class ContextWrapper(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._state = state_registry[initial_state_key]

            def request(self):
                """
                Perform a request using the current state.

                This method delegates the request to the current state's handle method.

                :return: None
                :rtype: None
                """
                self._state.handle(self)

            def set_state(self, state_key: str):
                """
                Set the current state to the specified state key.

                :param state_key: The key associated with the desired state.
                :type state_key: str
                :return: None
                :rtype: None
                """
                self._state = state_registry[state_key]

        return ContextWrapper

    return decorator


adaptee_registry: dict[str, Type] = {}


def adaptee(name: str = None):
    """
    Decorator to register a class as an adaptee.

    :param name: The name of the adaptee (optional).
    :type name: str
    :return: The decorated class.
    :rtype: class
    """
    def decorator(cls):
        # Register the adaptee class
        # Use the provided name, if any, otherwise use the class name
        adaptee_registry[name or cls.__name__] = cls
        return cls
    return decorator


def adapter(adaptee_name: str, **method_map):
    """
    Decorator to create an adapter class that adapts an adaptee class.

    :param adaptee_name: The name of the adaptee class to be used.
    :type adaptee_name: str
    :param method_map: Keyword arguments mapping methods of the adapter class to methods of the adaptee class.
    :type method_map: dict
    :return: The decorated class acting as an adapter.
    :rtype: class
    """
    def decorator(cls):
        class AdapterWrapper(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # Get the adaptee class
                self._adaptee = adaptee_registry[adaptee_name]()
                # Save the method map
                self._method_map = method_map

            def __getattribute__(self, name):
                try:
                    return super().__getattribute__(name)
                except AttributeError:
                    # If the attribute doesn't exist in the adapter,
                    # check if it's in the method map and call the corresponding
                    # method on the adaptee
                    if name in self._method_map:
                        adaptee_method = self._method_map[name]
                        return getattr(self._adaptee, adaptee_method)
                    raise
        return AdapterWrapper
    return decorator


responsable_registry: defaultdict[list] = defaultdict(list)


def responsable(responsable_key: str, order: float = float('inf'), **method_map: str) -> Callable:
    """
    Decorator to mark a class as a responsable and associate it with a responsable key.

    :param responsable_key: The key associated with the responsable.
    :type responsable_key: str
    :param order: The order of execution for the responsable (default: float('inf')).
    :type order: float
    :param method_map: Keyword arguments representing the mapping of methods in the responsable.
    :type method_map: Dict[str, str]
    :return: The decorated class.
    :rtype: Callable
    """
    def decorator(cls: Type) -> Callable:
        # If the class has already been decorated, return it as is
        if hasattr(cls, '_decorated'):
            return cls

        cls._decorated_methods = set()  # track the names of the methods we're adding

        class ResponsableWrapper(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._order = order
                self._method_map = method_map

            def handle_request(self, request: Any) -> Any:
                """
                Handle the request using the defined method in the responsable.

                :param request: The request to handle.
                :type request: Any
                :return: The response from handling the request.
                :rtype: Any
                """
                method_name = self._method_map.get("handle", "handle")
                if hasattr(self, method_name):
                    method = getattr(self, method_name)
                    return method(request)
                return request

        outer_class = ResponsableWrapper.__base__
        base_classes = [responsable.__class__.__base__ for responsable in responsable_registry[responsable_key]]

        # Only add to the registry if it's not already there
        if str(outer_class) not in list(map(lambda _class: str(_class), base_classes)):
            responsable_registry[responsable_key].append(ResponsableWrapper())

        cls._decorated = True  # mark the class as decorated
        return cls
    return decorator


def chain_of_responsibility(responsable_key: str) -> Callable:
    """
    Decorator to create a chain of responsibility for the given responsable key.

    :param responsable_key: The key associated with the chain of responsibility.
    :type responsable_key: str
    :return: The decorated class acting as the chain of responsibility.
    :rtype: Callable
    """
    def decorator(cls: Type) -> Callable:
        class ChainWrapper(cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._responsables = sorted(responsable_registry[responsable_key], key=lambda h: h._order)

            def handle_request(self, request: Any) -> Any:
                """
                Handle the request through the chain of responsibility.

                :param request: The request to handle.
                :type request: Any
                :return: The final response from the chain of responsibility.
                :rtype: Any
                """
                for responsable in self._responsables:
                    request = responsable.handle_request(request)
                return request

        return ChainWrapper
    return decorator
