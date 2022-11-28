import pydoc
from os import system

def check_library(lib_name: str):
    try:
        try:
            pydoc.render_doc(lib_name, "Help on %s")
        except:
            pydoc.render_doc(lib_name.replace("-", "."), "Help on %s")
        return True
    except:
        return False


def lib_required(lib_name: str):
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