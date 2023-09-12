TokenManager
============

.. autoclass:: hiveadb.functions.TokenManager
    :members: create_token, get_token, delete_token, list_tokens

Example
-------

To use the `TokenManager` class, simply call it as follows:

.. code-block:: python

    from hiveadb.functions import TokenManager

    print(TokenManager('user_token').list_tokens())


Expected Output
---------------

A list of all the tokens that are loaded into the DataBricks Workspace.