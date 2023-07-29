import pydoc
from os import system, getpid
import hashlib
import uuid
from typing import Union, Optional, List, Any
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




class ImportPromise:
    """
    A class that acts as a placeholder for an import.
    """
    def __init__(self, module: str, item: Optional[str] = None):
        """
        Initialize the ImportPromise with the module and item names.
        
        :param module: The name of the module to be imported.
        :type module: str
        :param item: The name of the item to be imported from the module.
        :type item: str, optional
        """
        self.module = module
        self.item = item
        self._module_obj = None
        self._item_obj = None

    def __get__(self, instance: Any, owner: Any) -> Any:
        """
        Perform the import when the attribute is accessed.
        
        :param instance: The instance that the attribute was accessed on.
        :type instance: Any
        :param owner: The owner class.
        :type owner: Any
        :return: The item object if it's defined, otherwise the module object.
        :rtype: Any
        """
        if not self._module_obj:
            self._module_obj = import_module(self.module)
        if self.item and not self._item_obj:
            self._item_obj = getattr(self._module_obj, self.item)
        return self._item_obj if self._item_obj else self._module_obj

    def __getattribute__(self, name: str) -> Any:
        """
        Perform the import and access the attribute on the module or item.
        
        :param name: The name of the attribute.
        :type name: str
        :return: The attribute object.
        :rtype: Any
        """
        if name in ["_module_obj", "module", "item", "_item_obj", "__class__", "__get__"]:
            return object.__getattribute__(self, name)
        if not self._module_obj:
            self._module_obj = import_module(self.module)

        # Create a new ImportPromise for the attribute.
        module = self.item if self.item else self.module
        return ImportPromise(module, name)
    
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Perform the import and call the module or item with the given arguments and keyword arguments.
        
        :param args: The positional arguments to pass to the module or item.
        :type args: Any
        :param kwargs: The keyword arguments to pass to the module or item.
        :type kwargs: Any
        :return: The result of calling the module or item.
        :rtype: Any
        """
        if not self._module_obj:
            self._module_obj = import_module(self.module)
        if self.item and not self._item_obj:
            self._item_obj = getattr(self._module_obj, self.item)
        obj = self._item_obj if self._item_obj else self._module_obj
        return obj(*args, **kwargs)
    
    def __repr__(self) -> str:
        """
        Return a string that represents the ImportPromise instance.
        
        :return: A string that represents the ImportPromise instance.
        :rtype: str
        """
        if not self._module_obj:
            self._module_obj = import_module(self.module)
        if self.item and not self._item_obj:
            self._item_obj = getattr(self._module_obj, self.item)
        obj = self._item_obj if self._item_obj else self._module_obj
        return repr(obj)

    def __str__(self) -> str:
        """
        Return a string that represents the ImportPromise instance.
        
        :return: A string that represents the ImportPromise instance.
        :rtype: str
        """
        if not self._module_obj:
            self._module_obj = import_module(self.module)
        if self.item and not self._item_obj:
            self._item_obj = getattr(self._module_obj, self.item)
        obj = self._item_obj if self._item_obj else self._module_obj
        return str(obj)


@singleton
class LazyImport:
    """
    A class that performs lazy import.
    """
    def __init__(self):
        """
        Initialize an empty dictionary to store the ImportPromise instances.
        """
        self._imports = {}

    def import_(self, module: str, as_: Optional[str] = None):
        """
        Perform a lazy import of a module.
        
        :param module: The name of the module to be imported.
        :type module: str
        :param as_: The name to assign to the module.
        :type as_: str, optional
        """
        as_ = as_ if as_ else module
        if as_ in globals():
            print(f"Warning: '{as_}' is already defined in the global namespace.")
            return
        globals()[as_] = ImportPromise(module)
        self._imports[as_] = module

    def from_(self, module: str, *args, **kwargs):
        """
        Perform a lazy import of items from a module.
        
        :param module: The name of the module to be imported from.
        :type module: str
        :param args: The names of the items to be imported.
        :type args: str
        :param kwargs: The names and aliases of the items to be imported.
        :type kwargs: str
        """
        for arg in args:
            if arg in globals():
                print(f"Warning: '{arg}' is already defined in the global namespace.")
                continue
            globals()[arg] = ImportPromise(module, arg)
            self._imports[arg] = (module, arg)
        for kw, as_ in kwargs.items():
            if as_ in globals():
                print(f"Warning: '{as_}' is already defined in the global namespace.")
                continue
            globals()[as_] = ImportPromise(module, kw)
            self._imports[as_] = (module, kw)


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