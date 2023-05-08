import pydoc
from os import system

import hashlib
import uuid
from typing import Union, Optional, List
from pandas import Series
from pyspark.sql.column import Column

def add_two(value_a: int, value_b: int) -> int:
    """
    Add two integer values together and return the result.

    :param value_a: The first integer value to be added.
    :type value_a: int
    
    :param value_b: The second integer value to be added.
    :type value_b: int
    
    :return: The sum of the two input values.
    :rtype: int
    """
    return value_a + value_b

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