import re
import json
import IPython
import inspect
import requests
import traceback
import contextlib
from warnings import warn
from datetime import datetime
from itertools import compress, chain
from databricks_api import DatabricksAPI
from typing import List, Set, Dict, Union, Optional, Mapping

from hivecore.functions import lib_required, LazyImport
import logging

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
    def __init__(self, token: str):
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


class TokenManager:
    """
    TokenManager class handles the management of tokens for Databricks API.

    :methods:
        - create_token: Creates a new token with the given token_name and stores it in the token list.
        - get_token: Retrieves the dictionary of a token based on its name.
        - delete_token: Deletes a token based on its name.
        - list_tokens: Retrieves a dictionary containing all the tokens organized by scopes.
    """

    def __init__(self, token: str):
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


class LibraryManager:
    """
    A class to manage libraries on a Databricks cluster.

    :methods:
        - install: Install a library on the current Databricks cluster.
        - uninstall: Uninstall a library from the current Databricks cluster.
    """

    def __init__(self, token: str):
        """
        Initialize the Libraries_Manager with the given Databricks API token.

        :param token: The Databricks token to authenticate with the Databricks API.
        :type token: str
        """
        self.host = SparkSession.builder.getOrCreate().conf.get("spark.databricks.workspaceUrl")
        self._token = token
        self._databricks_api = DatabricksAPI(host=self.host, token=self._token)
        self.cluster = SparkSession.builder.getOrCreate().conf.get("spark.databricks.clusterUsageTags.clusterId")

    def install(self, library: str = None, version: str = None, index: str = "pypi"):
        """
        Install a library on the current Databricks cluster.

        :param library: The name of the library to install.
        :type library: str
        :param version: The version of the library to install. Default is None.
        :type version: str, optional
        :param index: The index from which to install the library ('pypi' or 'maven'). Default is 'pypi'.
        :type index: str, optional

        :raises ValueError: If the specified index is not supported.
        """
        if index.lower() == "pypi":
            library = library if not version else f"{library}=={version}"
            library_payload = {"package": library}
        elif index.lower() == "maven":
            library = library if not version else f"{library}:{version}"
            library_payload = {"coordinates": library}
        else:
            raise ValueError(f"Index '{index}' is not supported.")

        try:
            self._databricks_api.managed_library.install_libraries(self.cluster, libraries=[{index: library_payload}])
            logging.info(f"Successfully triggered install '{library}' on cluster {self.cluster}.")
        except Exception as e:
            raise Exception(f"Failed to trigger install '{library}' on cluster {self.cluster}. Error: {e}")

    def uninstall(self, library: str = None, version: str = None, index: str = "pypi"):
        """
        Uninstall a library from the current Databricks cluster.

        :param library: The name of the library to uninstall.
        :type library: str
        :param version: The version of the library to uninstall. Default is None.
        :type version: str, optional
        :param index: The index from which to uninstall the library ('pypi' or 'maven'). Default is 'pypi'.
        :type index: str, optional

        :raises ValueError: If the specified index is not supported.
        """
        if index.lower() == "pypi":
            library = library if not version else f"{library}=={version}"
            library_payload = {"package": library}
        elif index.lower() == "maven":
            library = library if not version else f"{library}:{version}"
            library_payload = {"coordinates": library}
        else:
            raise ValueError(f"Index '{index}' is not supported.")

        try:
            self._databricks_api.managed_library.uninstall_libraries(self.cluster, libraries=[{index: library_payload}])
            logging.info(f"Successfully triggered uninstall '{library}' on cluster {self.cluster}.")
        except Exception as e:
            raise Exception(f"Failed to trigger uninstall '{library}' on cluster {self.cluster}. Error: {e}")
        

class DatabricksApi:
    """
        A utility class to interact with Databricks using its API.
        
        :attributes:
            - _token: The authorization token for the Databricks API.
            - _host: The workspace URL for Databricks.
            - _versions: The API versions for different functionalities.
            - _endpoints: The API endpoints for different functionalities.
            - _headers: The headers to be used in API requests.

        :methods:
            - _base: A general-purpose method to make API requests.
            - run_id: Property to retrieve the current run ID.
            - cluster_id: Property to retrieve the current cluster ID.
            - get_cluster: Method to fetch cluster details.
            - list_jobs: Method to list all jobs.
            - list_runs: Method to list all runs.
            - get_run: Method to fetch details of a specific run.
    """
    def __init__(self, token: str):
        """
            Initialize the DatabricksApi with the provided token.
            
            :param token: The authorization token for the Databricks API.
            :type token: str
        """
        self._token = token
        self._host  = spark.conf.get("spark.databricks.workspaceUrl")
        self._versions = {
            "get_job": [2.0, 2.1],
            "list_jobs": [2.0, 2.1],
            "list_runs": [2.0, 2.1],
            "get_run": [2.0, 2.1],
            "get_cluster": [2.0]
        }
        self._endpoints = {
            "get_job": '/jobs/get',
            "list_jobs": '/jobs/list',
            "list_runs": '/jobs/runs/list',
            "get_run": '/jobs/runs/get',
            "get_cluster": '/clusters/get'
        }
        self._headers = {
            "Authorization": f"Bearer {token}"
        }

    def _base(self, func, params, request_type: str = 'get', data=None):
        """
            General-purpose method to make API requests.
            
            :param func: The function or purpose of the API request (e.g., "get_job").
            :type func: str
            :param params: Parameters to pass in the API request.
            :type params: dict
            :param request_type: The type of the API request (e.g., "get", "post"). Default is "get".
            :type request_type: str
            :param data: Data to pass in a POST or PUT request.
            :type data: dict
            :return: The response from the API request.
            :rtype: dict or requests.Response
        """
        url = f"https://{self._host}/api/{self._versions.get(func)[-1]}{self._endpoints.get(func)}"
        
        try:
            if request_type.lower() == "get":
                res = requests.get(url, headers=self._headers, params=params)
            elif request_type.lower() == "post":
                res = requests.post(url, headers=self._headers, params=params, json=data)
            elif request_type.lower() == "put":
                res = requests.put(url, headers=self._headers, params=params, json=data)
            elif request_type.lower() == "delete":
                res = requests.delete(url, headers=self._headers, params=params)
            else:
                print(f"Unsupported request type: {request_type}")
                return None

            res.raise_for_status()  # Raise an exception for HTTP errors
            
        except requests.HTTPError as e:
            print(f"HTTP Error: {e}")
        except Exception as e:
            print(f"General Error: {e}")
        try:
            return json.loads(res.text)
        except:
            return res
    
    @property
    def run_id(self):
        """
            Retrieve the current run ID.
            
            :return: The current run ID.
            :rtype: str
        """
        try:
            return json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()).get("currentRunId").get('id')
        except:
            return
        
    @property
    def cluster_id(self):
        """
            Retrieve the current cluster ID.
            
            :return: The current cluster ID.
            :rtype: str
        """
        try:
            return str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId())[5:-1]
        except:
            return
    
    def get_cluster(self, cluster_id: str = None):
        """
            Fetch cluster details based on the provided cluster ID or the current session's cluster ID if none is provided.
            
            :param cluster_id: The ID of the cluster. If not provided, tries to capture from the session.
            :type cluster_id: str, optional
            :return: Details of the specified cluster.
            :rtype: dict
        """
        if not cluster_id:
            cluster_id = self.cluster_id

            if not cluster_id:
                return

        params = {
            "cluster_id": str(cluster_id)  # Convert run_id to string
        }

        return self._base("get_cluster", params)

    def list_jobs(self):
        """
            List all jobs in the Databricks workspace.
            
            :return: Details of all jobs.
            :rtype: dict
        """
        params = {}

        return self._base("list_jobs", params)
    
    def list_runs(self):
        """
            List all runs in the Databricks workspace.
            
            :return: Details of all runs.
            :rtype: dict
        """
        params = {}

        return self._base("list_runs", params)
    
    def get_run(self, run_id: str = None):
        """
            Fetch details of a specific run based on the provided run ID or the current session's run ID if none is provided.
            
            :param run_id: The ID of the run. If not provided, tries to capture from the session.
            :type run_id: str, optional
            :return: Details of the specified run.
            :rtype: dict
        """
        if not run_id:
            run_id = self.run_id

            if not run_id:
                return

        params = {
            "run_id": str(run_id)  # Convert run_id to string
        }

        return self._base("get_run", params)


class Interaction:
    """
        Represents an interaction with a set of states.
        
        :attributes:
            - VALID_STATES: A set of valid states for interactions.
            - _END_STATE: The default end state for interactions.
            - OPERATIONS: A list of operation types.
            - TRANSITIONS: The default state transition table.
            - _state: The current state of the interaction.
            - _primary_keys: A list of primary keys for the interaction.
            - params: Additional parameters provided for the interaction.
            - creation_time: Timestamp of when the interaction was created.
            - start_time: Timestamp of the interaction's start time.
            
        :methods:
            - can_transition_to: Check if a transition to a given state is valid.
            - set_transition: Define a valid state transition at the class level.
            - add_state: Add a new valid state for interactions at the class level.
            - remove_state: Remove a valid state for interactions at the class level.
            - set_end_state: Set a new end state for interactions at the class level.
            - reset_timer: Reset the start_time for the interaction instance.
            - update: Update the parameters of the interaction and optionally its state.
            - __eq__: Compare two Interaction instances based on their params.
    """
    VALID_STATES = {"Pending", "Failed", "Succeed", "Approved"}
    _END_STATE = "Succeed"

    OPERATIONS = ["rowsRead", "rowsInsert", "rowsUpdate", "rowsDelete"]

    # Initial state transition table
    TRANSITIONS: Dict[str, Set[str]] = {
        "Pending": {"Succeed", "Failed"},
        "Failed": {"Pending"},
        "Succeed": {"Approved"},
        "Approved": {}
    }

    def __init__(self, default_state: str = 'Pending', _primary_keys: List[str] = [], **kwargs):
        """
            Initialize the Interaction instance.
            
            :param default_state: The default state of the interaction, default is "Pending".
            :type default_state: str
            :param _primary_keys: A list of primary keys to determine unique interactions.
            :type _primary_keys: List[str]
            :param kwargs: Additional user-provided parameters.
        """
        self._state = default_state
        self._primary_keys = _primary_keys
        self.params = kwargs
        self.creation_time = datetime.utcnow()
        self.start_time = datetime.utcnow()

    @property
    def state(self) -> str:
        """
            Retrieve the current state of the interaction instance.
            
            :return: Current state of the interaction.
            :rtype: str
        """ 
        return self._state

    @state.setter
    def state(self, value: str):
        """
            Set the state of the interaction, ensuring the transition is valid.
            
            The state transition validity is checked against predefined allowed transitions. 
            An exception is raised if the transition is invalid.
            
            :param value: The target state to which the transition is desired.
            :type value: str
            :raises ValueError: If the state transition is invalid.
        """
        if not self.can_transition_to(value):
            raise ValueError(f"Invalid state transition from {self._state} to {value}.")
        self._state = value

    @property
    def primary_keys(self) -> List[str]:
        """
            Retrieve the primary keys for the interaction.

            :return: List of primary keys.
            :rtype: List[str]
        """
        return self._primary_keys

    def can_transition_to(self, state: str) -> bool:
        """
            Check if a transition to a given state is valid.

            :param state: The target state.
            :type state: str
            :return: True if the transition is valid, False otherwise.
            :rtype: bool
        """
        return state in Interaction.TRANSITIONS.get(self._state, {})

    @classmethod
    def set_transition(cls, from_state: str, to_state: str):
        """
            Define a valid state transition.

            :param from_state: The starting state.
            :type from_state: str
            :param to_state: The target state.
            :type to_state: str
        """
        if from_state not in cls.VALID_STATES or to_state not in cls.VALID_STATES:
            raise ValueError(f"Both states should be in {', '.join(cls.VALID_STATES)}")
        
        if from_state not in cls.TRANSITIONS:
            cls.TRANSITIONS[from_state] = set()
        cls.TRANSITIONS[from_state].add(to_state)

    @classmethod
    def add_state(cls, new_state: str):
        """
            Add a new valid state for interactions.
            
            :param new_state: The new state to add.
            :type new_state: str
        """
        cls.VALID_STATES.add(new_state)

    @classmethod
    def remove_state(cls, state: str):
        """
            Remove a state from the valid states for interactions. Cannot remove the end state.
            
            :param state: The state to remove.
            :type state: str
        """
        if state == cls._END_STATE:
            raise ValueError(f"Cannot remove the end state '{cls._END_STATE}'.")
        cls.VALID_STATES.discard(state)

    @classmethod
    def set_end_state(cls, state: str):
        """
            Set a new end state for interactions.
            
            :param state: The new end state.
            :type state: str
        """
        if state not in cls.VALID_STATES:
            raise ValueError(f"Invalid end state {state}. It should be one of the accepted states: {', '.join(cls.VALID_STATES)}")
        cls._END_STATE = state

    def reset_timer(self):
        """
            Reset the `start_time` attribute of the interaction instance to the current UTC time.
            
            This method updates the `start_time` attribute, which can be utilized to mark the beginning of a new event or to track the elapsed time since the last reset.

            :raises AttributeError: If `start_time` attribute is not present in the instance (though unlikely if the constructor is unaltered).
            :return: None
        """
        self.start_time = datetime.utcnow()

    def update(self, state: str = None, **kwargs):
        """
            Update the parameters of the interaction and set the state to 'Success'.
            
            :param kwargs: The updated parameters.
            :type kwargs: dict
        """
        self.params.update(kwargs)
        if self.state != self._END_STATE:
            self.state = state or self._END_STATE

    def __eq__(self, other: 'Interaction') -> bool:
        """
            Determines if two Interaction instances are equal based on their params.

            :param other: Another Interaction instance.
            :type other: Interaction
            :return: True if equal, False otherwise.
            :rtype: bool
        """
        return self.params == other.params


class AuditManager:
    """
        A utility class to manage and track interactions with Databricks notebooks.
        
        :attributes:
            - _databricks_api: An instance of the Databricks API, initialized using the provided token.
            - _creation_time: A timestamp noting when the instance was created.
            - interactions: A dictionary to store interactions with Interaction instances as values.

        
        :methods:
            - on_error: Decorator for Databricks cells to wrap them with custom error handling.
            - define_interaction: Define a new interaction or update an existing one.
            - add_state: Add a new valid state to all interactions.
            - remove_state: Remove a valid state from all interactions.
            - set_end_state: Set the end state for all interactions.
            - set_transition: Set a valid state transition for all interactions.
            - reset_timer: Reset the timer for specified interactions or all if no arguments.
            - on_exit: Generate an exit message and exit the notebook.
    """
    def __init__(self, token: Optional[str] = None):
        """
        Initialize the AuditManager with an optional token for the Databricks API.
        
        :param token: An optional token for the Databricks API.
        :type token: str, optional
        """
        if token:
            try:
                self._databricks_api = DatabricksApi(token)
            except:
                self._databricks_api = None
        else:
            self._databricks_api = None

        self._creation_time = datetime.utcnow()
        self.interactions: Dict[Union[str, tuple], Interaction] = dict()
        
    @contextlib.contextmanager
    def on_error(self, process: str, status: Optional[str] = "Failed"):
        """
            Decorator for Databricks cells to wrap them with custom error handling.

            :param process: The name of the process for logging purposes.
            :type process: str
            :param status: The status of the process, default is "Failed".
            :type status: Optional[str]
            :yield: Yields control to the wrapped block, allowing it to execute before proceeding with the rest of the function.
            :raises BaseException: Any exception that might occur during the execution of the Databricks cell.
        """
        def get_code_after_function() -> str:
            """
            Get the code after the calling function within the cell content.
            
            :return: Code after the calling function.
            :rtype: str
            """
            def commands_to_dict(commands):
                commands_dict = dict()
                for _, line_number, cmd in commands:
                    commands_dict[line_number] = cmd
                return commands_dict

            def get_current_line() -> int:
                # Fetch the content of the cell from IPython's history
                cell_content = IPython.get_ipython().history_manager.input_hist_parsed[-1]

                # Split cell content into lines
                lines = cell_content.split('\n')
                
                # Find the calling function's name from the stack
                function_name = inspect.stack()[2][3]
                
                # Loop through lines to find function name
                for index, line in enumerate(lines, start=1):
                    if function_name in line:
                        return index
                return -1

            # Obtain global notebook object
            shell = IPython.get_ipython()

            # Obtain command history
            all_commands = list(shell.history_manager.get_range())

            commands_dict = commands_to_dict(all_commands)

            # Get the current cell number (latest entry in the dictionary)
            current_cell = max(commands_dict.keys())
            
            # Get the line number where the function is called
            line_number = get_current_line()
            
            # If the function is not found, return an empty string or appropriate message
            if line_number == -1:
                return "Function not found in the cell!"
            
            # Fetch the content of the current cell from the dictionary
            cell_content = commands_dict[current_cell]

            cell_content = cell_content.split('\n')

            def strip_first_tab(input_string):
                return re.sub(r'^\t', '', input_string, count=1)

            notebook_content = list(map(lambda cell: strip_first_tab(cell), cell_content))

            # Get only the code after the function's line number
            code_after_function = "\n".join(notebook_content[line_number:])
            
            return code_after_function

        # Get the code in the cell after this function call
        # code_after_this_function = get_code_after_function()
        
        try:
            yield
            # exec(code_after_this_function)
        except BaseException as err:
            # Get the traceback as a string
            tb_str = traceback.format_exc()

            # Collect aggregated interactions for the Params section
            aggregated_interactions = []
            for interaction_key, interaction in self.interactions.items():
                interaction_info = {
                    **interaction.params,
                    "state": interaction.state,
                    "time": str(datetime.utcnow() - interaction.start_time)  # Time difference for this interaction
                }
                aggregated_interactions.append(interaction_info)

            # Format the notebook exit string
            exit_message = {
                "Logs": {
                    "Status": {
                        "state": status,
                        "error": tb_str,
                        "process": process
                    },
                    "Params": aggregated_interactions  # New Params section with interactions
                }
            }

            if self._databricks_api:
                exit_message['Logs']['Stats'] = {
                    "active_workers": len(self._databricks_api.get_cluster().get('executors')),
                    "memory_usage": self._databricks_api.get_cluster().get('cluster_memory_mb')
                }

            # Exit the notebook with the formatted message
            dbutils.notebook.exit(str(exit_message))

    def on_exit(self, time: Optional[str] = None):
        """
            Generate an exit message based on aggregated interactions and exit the notebook.

            :param time: The time taken for the operation, default is None.
            :type time: Optional[str]
            :raises Exception: Any exceptions that might occur during time calculation.
            :return: None
            :rtype: None
        """
        aggregated_interactions = {}
        overall_state = "Succeed"

        # Initialize with the current time, will look for the oldest time in the interactions next
        oldest_time = self._creation_time

        for interaction_key, interaction in self.interactions.items():
            aggregation_key = tuple(interaction.params[pk] for pk in interaction.primary_keys)
            
            if interaction.state == "Failed":
                overall_state = "Failed"
            
            if aggregation_key not in aggregated_interactions:
                aggregated_interactions[aggregation_key] = {
                    "state": interaction.state,
                    "time": str(datetime.utcnow() - interaction.start_time),
                    **{op: 0 for op in Interaction.OPERATIONS},
                    **interaction.params
                }
            else:
                for op in Interaction.OPERATIONS:
                    aggregated_interactions[aggregation_key][op] += interaction.params.get(op, 0)
                aggregated_interactions[aggregation_key]["state"] = interaction.state

            # Check for the oldest creation time among interactions
            if interaction.creation_time < oldest_time:
                oldest_time = interaction.creation_time

        # Calculate the global time difference
        global_time_diff = str(datetime.utcnow() - oldest_time)

        # Check for the overall state
        if all(interaction["state"] == "Succeed" for interaction in aggregated_interactions.values()):
            overall_state = "Succeed"

        exit_message = {
            "Logs": {
                "Status": {
                    "state": overall_state,
                    "Time": global_time_diff  # Set the global time difference here
                },
                "Params": list(aggregated_interactions.values())
            }
        }

        # Exit the notebook with the formatted message
        dbutils.notebook.exit(str(exit_message))

    def define_interaction(self, _primary_keys: List = list(), **user_input):
        """
            Define a new SQL interaction or update an existing one based on provided primary keys.
            
            :param _primary_keys: A list of primary keys to define an interaction, default is an empty list.
            :type _primary_keys: List
            :param user_input: Additional parameters to define an interaction.
            :type user_input: dict
        """
        pks_value = tuple(user_input[pk] for pk in _primary_keys) if _primary_keys else str(user_input)

        if pks_value not in self.interactions:
            interaction = Interaction(_primary_keys=_primary_keys, **user_input)
            self.interactions[pks_value] = interaction
        else:
            self.interactions[pks_value].update(**user_input)

    def add_state(self, state: str):
        """
            Add a new valid state to all interactions.
            
            :param state: The state to be added to all interactions.
            :type state: str
            :return: None
            :rtype: None
        """
        for interaction in self.interactions.values():
            interaction.add_state(state)

    def remove_state(self, state: str):
        """
            Remove a valid state from all interactions.
            
            :param state: The state to be removed from all interactions.
            :type state: str
            :return: None
            :rtype: None
        """
        for interaction in self.interactions.values():
            interaction.remove_state(state)

    def set_end_state(self, state: str):
        """
            Set the end state for all interactions.
            
            :param state: The end state to be set for all interactions.
            :type state: str
            :return: None
            :rtype: None
        """
        for interaction in self.interactions.values():
            interaction.set_end_state(state)

    def set_transition(self, from_state: str, to_state: str):
        """
            Set a valid state transition for all interactions.
            
            :param from_state: The initial state of the transition.
            :type from_state: str
            :param to_state: The target state of the transition.
            :type to_state: str
            :return: None
            :rtype: None
        """
        for interaction in self.interactions.values():
            interaction.set_transition(from_state, to_state)

    def reset_timer(self, *args):
        """
            Reset the timer for specified interactions or all if no arguments are provided.
            
            :param args: The arguments representing interactions to have their timers reset. 
                        If no arguments are given, all interactions will have their timers reset.
            :type args: tuple
            :return: None
            :rtype: None
        """
        if args:
            interaction = self.interactions.get((*args,))
            if interaction:
                interaction.reset_timer()
        else:
            for interaction in self.interactions.values():
                interaction.reset_timer()