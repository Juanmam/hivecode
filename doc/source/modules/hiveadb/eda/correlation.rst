Correlation
===========

.. autofunction:: hiveadb.eda.correlation
   :noindex:

Parameters
^^^^^^^^^^
* method: The type of correlation to be applied.
    * Pearson: "pearson"
    * Kendall: "kendall"
    * Spearman: "spearman"

Basic Example
^^^^^^^^^^^^^
..  code-block:: python
    
    from hiveadb.io import read_csv
    from hiveadb.eda import correlation
    from hiveadb.preprocess import encode, replace_nas

    # Reads the data from the sample datasets.
    df = read_csv("us-counties.csv", "/databricks-datasets/COVID/covid-19-data/", as_type = PANDAS)

    # Encodes categorical data into corresponding categories.
    encoded_df = encode(df, ["date", "county", "state"], overwrite = True)

    # Make sure that the data has no null values.
    clean_df = replace = replace_nas(encoded_df)

    # This will return the matrix as a dataframe.
    corr_df = correlation(clean_df, clean_df.columns)
    corr_df

.. image:: /images/corr_example.PNG

Heatmap Example
^^^^^^^^^^^^^^^
..  code-block:: python
    
    from hiveadb.io import read_csv
    from hiveadb.eda import correlation
    from hiveadb.preprocess import encode, replace_nas

    # Reads the data from the sample datasets.
    df = read_csv("us-counties.csv", "/databricks-datasets/COVID/covid-19-data/", as_type = PANDAS)

    # Encodes categorical data into corresponding categories.
    encoded_df = encode(df, ["date", "county", "state"], overwrite = True)

    # Make sure that the data has no null values.
    clean_df = replace = replace_nas(encoded_df)

    # This will return the matrix as a dataframe.
    corr_df = correlation(clean_df, clean_df.columns, as_heatmap = True, full_matrix = False)
    corr_df

.. image:: /images/heat_corr_example.PNG

Method Example
^^^^^^^^^^^^^^
..  code-block:: python
    
    from hiveadb.io import read_csv
    from hiveadb.eda import correlation
    from hiveadb.preprocess import encode, replace_nas

    # Reads the data from the sample datasets.
    df = read_csv("us-counties.csv", "/databricks-datasets/COVID/covid-19-data/", as_type = PANDAS)

    # Encodes categorical data into corresponding categories.
    encoded_df = encode(df, ["date", "county", "state"], overwrite = True)

    # Make sure that the data has no null values.
    clean_df = replace = replace_nas(encoded_df)

    # This will return the matrix as a dataframe. Will be using Kendall instead of Pearson.
    corr_df = correlation(clean_df, clean_df.columns, method="kendall")
    corr_df