import pydoc
from os import system

from typing import Union, List
from pandas import Series
from pyspark.sql.column import Column

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