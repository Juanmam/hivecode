SecretManager
=============

.. autoclass:: hiveadb.functions.SecretManager
    :members: create_secret, create_scope, read_secret, read_scope, list_secrets, list_scopes, delete_secret, delete_scope

Example
-------
To use the SecretManager you will need a databricks token. Find out more about them here!

Current version of the SecretManager only works with Databricks Secret Scopes and NOT witn Azure Key-Vault.

Declaration
~~~~~~~~~~~

..  code-block:: python

    from hiveadb.functions import SecretManager

    token = dbutils.secrets.get("MyScope", "DatabricksToken")
    
    # Define your secret manager
    secret_manager = SecretManager(token)

Scope Creation
~~~~~~~~~~~~~~

..  code-block:: python

    secret_manager.create_scope('MyNewSecretScope')

Secret Creation
~~~~~~~~~~~~~~~

..  code-block:: python

    secret_manager.create_secret(scope='MyNewSecretScope', key='MySecret', value='www.imgur.com/a/G6rU8')

Secret Reading
~~~~~~~~~~~~~~

..  code-block:: python
    
    my_secret = secret_manager.read_secret(scope='MyNewSecretScope', key='MySecret')

    print('My secret is', my_secret)

Trying to print or log a secret will not reveal its content and keep it a secret but still usable for those who need it.

.. code-block:: text

    My secret is [REDACTED]

Scope Deletion
~~~~~~~~~~~~~~~

..  code-block:: python

    secret_manager.delete_scope('MyNewSecretScope')

Secret Deletion
~~~~~~~~~~~~~~~

..  code-block:: python

    secret_manager.delete_secret(scope = 'MyNewSecretScope', key = 'MySecret')