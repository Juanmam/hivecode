from typing import List, Union, AnyOptional, Mapping
import requests
from databricks_api import DatabricksAPI
from functools import reduce
from itertools import compress, chain
from warnings import warn

from hivecore.patterns import singleton
from hivecore.functions import lib_required, LazyImport

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# DBUtils
try:
    # up to DBR 8.2
    from dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position
except:
    # above DBR 8.3
    from dbruntime.dbutils import DBUtils  # pylint: disable=import-error,wrong-import-position


def get_spark() -> SparkSession:
    """
    Fetches the current instance of Spark. If none exists, creates a new one.

    :return: The current Spark session.
    :rtype: pyspark.sql.SparkSession
    """
    # Spark
    sc: SparkContext = SparkContext.getOrCreate()
    return SparkSession(sc)


def get_dbutils(spark: SparkSession = None) -> DBUtils:
    """
    Fetches the current instance of DBUtils.

    :param spark: The current spark session. Defaults to None.
    :type spark: pyspark.sql.SparkSession

    :return: An instance of DBUtils.
    :rtype: databricks.DBUtils
    """
        
    # We try to create dbutils from spark or by using IPython
    try:
        return DBUtils(spark)
    except:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

spark   = get_spark()
dbutils = get_dbutils()


def mount(
    storage: str,
    key: str,
    mount_point: str = '/mnt/',
    mounts: List[str] = ["raw", "silver", "gold"],
    postfix: str = '-zone',
    include_tqdm: bool = False,
    verbose: bool = False
) -> None:
    """
    Mounts a set of zones into the system.

    :param storage: The name of the storage to mount. This can be found at keys access in your storage account.
    :type storage: str
    
    :param key: The key of the storage to mount. This can be found at keys access in your storage account.
    :type key: str
    
    :param mount_point: The mount point to use, defaults to '/mnt/'.
    :type mount_point: str, optional
    
    :param mounts: A list of all the mounts you want. This doesn't include the prefix. Check example. 
        Defaults to ["raw", "silver", "gold"].
    :type mounts: List[str], optional
    
    :param postfix: The postfix is the ending you want to put to your mount zones. Set it to an empty 
        string if you don't want to apply it, defaults to '-zone'.
    :type postfix: str, optional
    
    :param include_tqdm: A flag to include tqdm bars for mounts, defaults to False.
    :type include_tqdm: bool, optional
    
    :param verbose: A flag to indicate whether to show tqdm progress bar or not, defaults to False.
    :type verbose: bool, optional
    
    :return: None
    :rtype: None
    """
    def __mount(mount_name: str) -> None:
        """
        Mounts a single zone to the system.

        :param mount_name: The name of the zone to mount.
        :type mount_name: str

        :return: None
        :rtype: None
        """
        if not f"{mount_point}{mount_name}" in list(map(lambda mount: mount.mountPoint, dbutils.fs.mounts())):
            dbutils.fs.mount(
                source=f"wasbs://{mount_name}@{storage}.blob.core.windows.net/",
                mount_point=f"{mount_point}{mount_name}",
                extra_configs={f"fs.azure.account.key.{storage}.blob.core.windows.net": key}
            )

    lib_required("tqdm")
    from tqdm import tqdm

    if include_tqdm:
        mount_iterator = tqdm(mounts, desc="Mounts", position=0, leave=True)
    else:
        mount_iterator = mounts

    if verbose:
        mount_iterator = tqdm(mount_iterator)

    for mount_name in mount_iterator:
        __mount(mount_name)


class SecretManager:
    """
    SecretManager class handles the management of secrets in Azure Databricks.

    :methods:
        - create_secret: Store a secret in Azure Databricks.
        - create_scope: Create a new scope in Azure Databricks.
        - read_secret: Read the value of a secret from Azure Databricks.
        - read_scope: List all secrets within a specific scope in Azure Databricks.
        - list_secrets: List all secrets along with their corresponding scopes in Azure Databricks.
        - list_scopes: List all secret scopes in Azure Databricks.
        - delete_secret: Delete a secret from a specific scope in Azure Databricks.
        - delete_scope: Delete a secret scope in Azure Databricks.
    """
    def _init_(self, token: str):
        self._token = token
        self.workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
        self._databricks_api = DatabricksAPI(host=self.workspace_host, token=self._token)

    def create_secret(self, scope: str = None, key: str = None, value: str = None) -> None:
        """
        Store a secret in Azure Databricks.

        :param scope: The scope in which to store the secret.
        :type scope: str
        :param key: The name of the secret/key.
        :type key: str
        :param value: The value of the secret to store.
        :type value: str
        :raises: Exception if an error occurs while storing the secret.
        """
        try:
            # Store the secret
            self._databricks_api.secret.put_secret(scope, key, value)
        except:
            raise
    
    def create_scope(self, scope: str = None, manager: str = 'users', backend: str = "DATABRICKS", backend_azure_keyvault: str = None, headers: Mapping = None) -> None:
        """
        Create a new scope in Azure Databricks.

        :param scope: The name of the scope to create.
        :type scope: str
        :raises: Exception if an error occurs while creating the scope.
        """
        # Replace the following variables with your Databricks workspace URL and token
        if scope:
            try:
                self._databricks_api.secret.create_scope(scope, initial_manage_principal=manager, scope_backend_type=backend, backend_azure_keyvault=backend_azure_keyvault, headers=headers)
            except:
                raise
        else:
            displayHTML(f"""<a href={f'https://{self.workspace_host}#secrets/createScope'}>Databricks Secret Scope UI</a>""")

    def read_secret(self, scope: str, key: str):
        """
        Read the value of a secret from Azure Databricks.

        :param scope: The name of the scope containing the secret.
        :type scope: str
        :param key: The name of the secret/key.
        :type key: str
        :return: The value of the secret.
        :rtype: str
        :raises: Exception if an error occurs while reading the secret.
        """
        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except:
            raise
    
    def read_scope(self, scope: str):
        """
        List all secrets within a specific scope in Azure Databricks.

        :param scope: The name of the scope to list secrets from.
        :type scope: str
        :return: A list of secret key-value pairs in the specified scope.
        :rtype: list
        """
        return dbutils.secrets.list(scope=scope) 

    def list_secrets(self):
        """
        List all secrets along with their corresponding scopes in Azure Databricks.

        :return: A list of dictionaries representing secrets with their associated scopes.
        :rtype: list
        """
        scope_names = list(map(lambda scope: scope.name, dbutils.secrets.listScopes()))

        scope_data = map(lambda scope: self._databricks_api.secret.list_secrets(scope), scope_names)

        secrets_dict = {scope: list(data.values())[0][0] for scope, data in zip(scope_names, scope_data)}

        for scope, data in secrets_dict.items():
            data["scope"] = scope

        return list(secrets_dict.values())

    def list_scopes(self):
        """
        List all secret scopes in Azure Databricks.

        :return: A list of dictionaries representing secret scopes.
        :rtype: list
        """
        return list(self._databricks_api.secret.list_scopes().values())

    def delete_secret(self, scope: str, key: str, headers: Mapping = None):
        """
        Delete a secret from a specific scope in Azure Databricks.

        :param scope: The name of the scope containing the secret.
        :type scope: str
        :param key: The name of the secret/key to delete.
        :type key: str
        :param headers: Additional headers for the API request (optional).
        :type headers: Mapping, optional
        """
        self._databricks_api.secret.delete_secret(scope, key, headers=headers)
    
    def delete_scope(self, scope: str, headers: Mapping = None):
        """
        Delete a secret scope in Azure Databricks.

        :param scope: The name of the scope to delete.
        :type scope: str
        :param headers: Additional headers for the API request (optional).
        :type headers: Mapping, optional
        """
        self._databricks_api.secret.delete_scope(scope, headers=headers)


@singleton
class TokenManager:
    """
    TokenManager class handles the management of tokens for Databricks API.

    :methods:
    - create_token: Creates a new token with the given token_name and stores it in the token list.
    - get_token: Retrieves the dictionary of a token based on its name.
    - delete_token: Deletes a token based on its name.
    - list_tokens: Retrieves a dictionary containing all the tokens organized by scopes.
    """

    def _init_(self, token: str, token_name: str = "temp_token"):
        self._token = token
        self.workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
        self._databricks_api = DatabricksAPI(host=self.workspace_host, token=self._token)
        self._tokens = list(chain.from_iterable(list(self.list_tokens().values())))

        def update_token(tkn):
            """
            Update the token dictionary with necessary information and remove unwanted keys.

            :param tkn: The token dictionary to be updated.
            :type tkn: dict
            :return: The updated token dictionary.
            :rtype: dict
            """
            tkn.update({"token_name": tkn.get("comment")})
            del tkn['creation_time']
            del tkn['expiry_time']
            del tkn['comment']
            tkn_id = tkn.get("token_id")
            del tkn['token_id']
            tkn['token_id'] = tkn_id
            return tkn
        
        list(map(lambda tkn: update_token(tkn), self._tokens))

    def create_token(self, token_name: str = "temp_token"):
        """
        Create a new token and store it in the token list, if a token with the same name does not exist.

        :param token_name: The name for the new token. Default is "temp_token".
        :type token_name: str, optional
        :return: The dictionary containing the new token information.
        :rtype: dict
        """
        existing_token_names = set(token.get("token_name") for token in self._tokens)
        if token_name in existing_token_names:
            warn(f"A token with the name '{token_name}' already exists. Skipping creation.", UserWarning)
            return

        _token = self._databricks_api.token.create_token(comment=token_name)
        _token_dir = { 
            "token_name": token_name,
            "token_id": _token.get('token_info').get('token_id'), 
            "token": _token.get('token_value')
        }
        self._tokens.append(_token_dir)
        return _token_dir

    def get_token(self, name: str):
        """
        Get the dictionary of a token based on its name.

        :param name: The name of the token to retrieve.
        :type name: str
        :return: The dictionary containing the token information.
        :rtype: dict
        :raises IndexError: If the token with the given name is not found.
        """
        tokens_with_name = list(compress(self._tokens, map(lambda token: token.get("token_name") == name, self._tokens)))
        if not tokens_with_name:
            raise IndexError(f"No token found with the name '{name}'.")
        return tokens_with_name[0]

    def delete_token(self, identifier: str):
        """
        Delete a token based on its name or token_id.

        :param identifier: The name or token_id of the token to delete.
        :type identifier: str
        :raises IndexError: If the token with the given name or token_id is not found.
        """
        tokens_with_identifier = list(compress(self._tokens, 
                                            map(lambda token: token.get("token_name") == identifier or 
                                                            token.get("token_id") == identifier, 
                                                self._tokens)))
        if not tokens_with_identifier:
            raise IndexError(f"No token found with the name or token_id '{identifier}'.")
        
        token_to_delete = tokens_with_identifier[0]
        self._databricks_api.token.revoke_token(token_to_delete.get("token_id"))
        
        # Remove the deleted token from the _tokens list
        self._tokens = [token for token in self._tokens if token != token_to_delete]

    def list_tokens(self):
        """
        Get a dictionary containing all the tokens organized by scopes.

        :return: A dictionary containing the tokens organized by scopes.
        :rtype: dict
        """
        return self._databricks_api.token.list_tokens()