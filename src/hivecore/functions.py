import pydoc
from os import system, getpid
import hashlib
import uuid
from typing import Union, Optional, List
from pandas import Series
from pyspark.sql.column import Column
from psutil import Process
from hivecore.patterns import singleton
from importlib import import_module
from warnings import warn


def get_memory_usage() -> float:
    """
    Get the current memory usage of the process.
    
    :return: The current memory usage in megabytes.
    :rtype: float
    """
    process = Process(getpid())
    megabytes = process.memory_info().rss / (1024 * 1024)
    return megabytes


class LazyMeta(type):
    """
    Metaclass for Lazy. Handles attribute access to lazily import modules.
    """
    _modules = {}
    _aliases = {}

    def __getattr__(cls, name):
        """
        Overriding __getattr__ to perform lazy import.
        
        :param name: Name of the attribute being accessed.
        :type name: str
        :return: The requested attribute from the imported module.
        """
        if name in cls._aliases:
            return cls._aliases[name]
        if name not in cls._modules:
            cls._modules[name] = import_module(name)
        return cls._modules[name]
    

class ImportPromise:
    """
    A class that promises to import a module when an attribute is accessed.
    """
    def __init__(self, module_name, alias=None):
        """
        Initializes the ImportPromise instance.
        
        :param module_name: The name of the module to be imported.
        :type module_name: str
        :param alias: The specific attribute to import from the module, defaults to None.
        :type alias: str, optional
        """
        self.module_name = module_name
        self.alias = alias
        self._module = None

    def __getattr__(self, name):
        """
        Overriding __getattr__ to perform the import when an attribute is accessed.
        
        :param name: Name of the attribute being accessed.
        :type name: str
        :return: The requested attribute from the imported module.
        """
        if self._module is None:
            self._module = import_module(self.module_name)
            if self.alias is not None:
                self._module = getattr(self._module, self.alias)
        return getattr(self._module, name)

    def __call__(self, *args, **kwargs):
        """
        Overriding __call__ to perform the import when the instance itself is called.
        
        :param args: Positional arguments to pass to the called function.
        :param kwargs: Keyword arguments to pass to the called function.
        :return: The result of calling the imported function/module.
        """
        if self._module is None:
            self._module = import_module(self.module_name)
            if self.alias is not None:
                self._module = getattr(self._module, self.alias)
        return self._module(*args, **kwargs)


@singleton
class LazyImport(metaclass=LazyMeta):
    """
    A class for performing lazy imports. Uses LazyMeta as its metaclass and is a singleton. This class generates ImportPromises, a class that actually imports the library when called.
    """
    @classmethod
    def import_from(cls, module_name: str, **aliases) -> None:
        """
        Sets up lazy imports for the specified module and aliases.
        
        :param module_name: The name of the module to be imported.
        :type module_name: str
        :param aliases: Dictionary of aliases to set up for specific attributes from the module.
        """
        for alias, attribute_name in aliases.items():
            promise = ImportPromise(module_name, alias=attribute_name)
            cls._aliases[alias] = promise
            if alias in globals():
                warn(f"Global variable '{alias}' is being overwritten by Lazy")
            globals()[alias] = promise


def to_list(value: Union[list, Series, Column]) -> List:
    """
    Transform an input into a list.

    :param value: The input that needs to be transformed into a list.
    :type value: Any
    
    :return: A list with the elements from the object.
    :rtype: List
    """
    if isinstance(value, list):
        # If the input is a list, return it.
        return value
    elif 'pandas' in str(type(value)):
        # If the input is a Series, return it as a list.
        return value.tolist()
    elif 'pyspark' in str(type(value)):
        # If the input is a Column, return it as a list.
        return value.collect()
    else:
        # If the typing doesn't match, raise a ValueError.
        raise ValueError("Input must be of type list, pandas.Series, or pyspark.sql.column.Column")

def check_library(lib_name: str):
    """
    Check if a library is installed.
    
    :param lib_name: The name of the library to check for.
    :type lib_name: str
    
    :return: A boolean indicating whether the library is installed.
    :rtype: bool
    """
    try:
        try:
            pydoc.render_doc(lib_name, "Help on %s")
        except:
            pydoc.render_doc(lib_name.replace("-", "."), "Help on %s")
        return True
    except:
        return False


def lib_required(lib_name: str):
    """
    Ensure a library is installed.
    
    :param lib_name: The name of the library to check for and install if necessary.
    :type lib_name: str
    
    :return: None.
    :rtype: None
    """
    if not check_library(lib_name):
        try:
            system(f"pip install {lib_name}")
        except:
            try:
                system(f"pip3 install {lib_name}")
            except:
                if lib_name == "databricks.koalas":
                    try:
                        system("pip install koalas")
                    except:
                        pass
                        # raise Exception(f"Could not install {lib_name}")
                pass


def generate_key(seed: int, charset: Optional[str] = "alphanumeric") -> str:
    """
    Generates a random key based on the given seed and character set.
    
    :param seed: The seed value used to generate the key.
    :type seed: int
    
    :param charset: The character set to use for the key. Default is alphanumeric.
                    Supported values are: numeric, alpha, alphanumeric, all.
    :type charset: Optional[str]
    
    :return: A randomly generated key based on the given seed and character set.
    :rtype: str
    
    :raises ValueError: If an invalid charset is provided.
    """
    # Define character set based on the given charset
    if charset == "numeric":
        chars = "0123456789"
    elif charset == "alpha":
        chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    elif charset == "alphanumeric":
        chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    elif charset == "all":
        chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_$#@&-+()/*:;!?"
    else:
        raise ValueError("Invalid charset.")
        
    # Generate hash bytes using the seed value
    seed_str = str(seed).encode('utf-8')
    hash_bytes = hashlib.md5(seed_str).digest()
    
    # Generate UUID using the hash bytes and node value, then format and return the key
    namespace = uuid.UUID(int=uuid.getnode()).hex
    random_uuid = str(uuid.uuid5(uuid.UUID(bytes=hash_bytes), namespace)).replace('-', '')
    key = ''.join(c for c in random_uuid if c in chars)
    
    return key