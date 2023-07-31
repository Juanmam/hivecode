from hivecore.constant import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PYSPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK
from hiveadb.function import get_spark, get_dbutils, data_convert, to_list, df_type

spark   = get_spark()
dbutils = get_dbutils()

from numpy import median as _median
from scipy.stats import mode as _mode

# Pandas
from pandas import DataFrame, concat, to_numeric

# Pyspark.Pandas
from pyspark.pandas import DataFrame as ps_DataFrame, from_pandas as ps_from_pandas

# Pyspark
from pyspark.sql import Window
from pyspark.sql.types import StringType

# Koalas
from databricks.koalas import from_pandas, DataFrame as KoalasDataFrame

from typing import List, Union
from os import system
from pyspark.sql.functions import abs as sabs, max as smax, min as smin, mean as _mean, stddev as _stddev, count as scount, first, last
from sklearn.metrics.pairwise import cosine_similarity as sk_cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from pandas import DataFrame, concat
from typing import List, Union

import pandas
import pyspark
import databricks.koalas


from textdistance import cosine, jaro, levenshtein, damerau_levenshtein, hamming
from jellyfish import jaro_winkler
from typing import Union, List, Mapping
from pandas import DataFrame as PandasDataFrame, Series as PandasSeries, concat as PandasConcat
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import DoubleType
from polars import DataFrame as PolarsDataFrame, Series as PolarsSeries, Float64 as PolarsFloat64

##### NUMERIC FUNCTIONS #####
def normalize(df: Union[pandas.DataFrame, databricks.koalas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame], 
              columns: List[str] = None, 
              method: str = "max-abs", 
              overwrite: bool = False) -> Union[pandas.DataFrame, databricks.koalas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]:
    """
    Normalize the values in the specified columns of a DataFrame using a given normalization method.

    :param df: A DataFrame object to normalize.
    :type df: Union[pandas.DataFrame, databricks.koalas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]

    :param columns: A list of column names to normalize. If None, normalize all columns.
    :type columns: List[str]

    :param method: The normalization method to use. Default is "max-abs".
    :type method: str

    :param overwrite: If True, overwrite the original values in the DataFrame with the normalized values.
    :type overwrite: bool

    :return: A DataFrame object with the normalized values. If overwrite is True, return the same DataFrame object.
    :rtype: Union[pandas.DataFrame, databricks.koalas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]
    """
    engine = df_type(df)
    method = method.lower()
    if engine == PANDAS or engine == KOALAS or engine == PANDAS_ON_SPARK:
        if not columns:
            s = df.apply(lambda s: to_numeric(s, errors='coerce').notnull().all())
            if engine == KOALAS or engine == PANDAS_ON_SPARK:
                columns = list(s[s].index.to_numpy())
            else:
                columns = list(s[s].index)
            
        if not isinstance(columns, list):
            columns = [columns]
            
        df = df.copy()

        if method in ["max_abs", "max-abs", "max abs", "maximum_absolute", "maximum-absolute","maximum absolute"]:
            for column in columns:
                if overwrite:
                    df[column] = df[column]  / df[column].abs().max()
                else:
                    df[f"{column}_norm"] = df[column]  / df[column].abs().max()
        elif method in ["min_max", "min-max", "min max"]:
            for column in columns:
                if overwrite:
                    df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
                else:
                    df[f"{column}_norm"] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
        elif method in ["z-score"]:
            for column in columns:
                if overwrite:
                    df[column] = (df[column] - df[column].mean()) / df[column].std()
                else:
                    df[f"{column}_norm"] = (df[column] - df[column].mean()) / df[column].std()
    elif engine == PYSPARK:
        if not columns:
            columns = list()
            for column in df.columns:
                if df.select(column, df[column].cast("int").isNotNull().alias("Value")).select("Value").distinct().collect()[0]["Value"] == True and df.select(column, df[column].cast("int").isNotNull().alias("Value")).select("Value").distinct().count() == 1:
                    columns.append(column)
        if not isinstance(columns, list):
            columns = [columns]
        df = df.alias('copy')
        if method in ["max_abs", "max-abs", "max abs", "maximum_absolute", "maximum-absolute","maximum absolute"]:
            for column in columns:
                if overwrite:
                    df = df.withColumn(column, (df[column] / df.select(smax(sabs(df[column])).alias("abs-max")).collect()[0]["abs-max"]).alias(f"{column}_normalized"))
                else:
                    df = df.withColumn(f"{column}_norm", (df[column] / df.select(smax(sabs(df[column])).alias("abs-max")).collect()[0]["abs-max"]).alias(f"{column}_normalized"))
        elif method in ["min_max", "min-max", "min max"]:
            for column in columns:
                if overwrite:
                    df = df.withColumn(column, ( (df[column] - df.select(smin(df[column]).alias("min")).collect()[0]["min"]) / ((df.select(smax(df[column]).alias("max")).collect()[0]["max"]) - (df.select(smin(df[column]).alias("min") )).collect()[0]["min"])).alias(f"{column}_normalized"))
                else:
                    df = df.withColumn(f"{column}_norm", ( (df[column] - df.select(smin(df[column]).alias("min")).collect()[0]["min"]) / ((df.select(smax(df[column]).alias("max")).collect()[0]["max"]) - (df.select(smin(df[column]).alias("min") )).collect()[0]["min"])).alias(f"{column}_normalized"))
        elif method in ["z-score"]:
            for column in columns:
                if overwrite:
                    df = df.withColumn(column, ((df[column] - (df.select(_mean(df[column]).alias("mean")).collect()[0]["mean"])) / (df.select(_stddev(df[column]).alias("std")).collect()[0]["std"])).alias(f"{column}_normalized"))
                else:
                    df = df.withColumn(f"{column}_norm", ((df[column] - (df.select(_mean(df[column]).alias("mean")).collect()[0]["mean"])) / (df.select(_stddev(df[column]).alias("std")).collect()[0]["std"])).alias(f"{column}_normalized"))

    return df


def replace_nas(df: Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame],
                columns: List[str] = None, 
                method: str = "mean") -> Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]:
    """
    Replace missing values (NAs) in the specified columns of a DataFrame with a specified method.

    :param df: The DataFrame to replace NAs in.
    :type df: Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]

    :param columns: The list of columns to replace NAs in. If None, all columns with NAs will be replaced.
    :type columns: List[str]

    :param method: The method to use for replacing NAs. Available methods are "mean", "median", "mode", "ffill", and "bfill".
    :type method: str

    :return: The DataFrame with NAs replaced according to the specified method.
    :rtype: Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]
    """
    engine = df_type(df)
    if not columns:
        if engine == KOALAS or engine == PANDAS_ON_SPARK:
            s = df.apply(lambda s: to_numeric(s, errors='coerce').notnull().all())
            columns = list(set(list(s[s].index.to_numpy())))
            df = df.copy()
        elif engine == PANDAS:
            s = df.apply(lambda s: to_numeric(s, errors='coerce').notnull().all())
            columns = list(s[s].index)
            df = df.copy()
        elif engine == PYSPARK:
            columns = df.columns
            df = df.alias("_copy")
    if method == "mean":
        if engine == KOALAS or engine == PANDAS_ON_SPARK:
            for column in df.columns:
                df[column] = df[column].fillna(df[column].mean())
        elif engine == PANDAS:
            df[columns] = df[columns].fillna(df.mean())
        elif engine == PYSPARK:
            for column in columns:
                mean = df.select(_mean(df[column]).alias("mean")).collect()[0]["mean"]
                df = df.na.fill(value=mean)
    if method == "median":
        if engine == KOALAS or engine == PANDAS_ON_SPARK:
            for column in df.columns:
                median = df[column].median()
                df[column] = df[column].fillna(value=median)
        elif engine == PANDAS:
            df[columns] = df[columns].fillna(df.median())
        elif engine == PYSPARK:
            for column in columns:
                median = _median(edf.select(column).na.drop().rdd.map(lambda r: r[0]).collect())
                df = df.na.fill(value=median)
    if method == "mode":
        if engine == KOALAS or engine == PANDAS_ON_SPARK:
            for column in df.columns:
                df[column] = df[column].fillna(df[column].mode()[0])
        elif engine == PANDAS:
            df[columns] = df[columns].fillna(df.mode())
        elif engine == PYSPARK:
            for column in columns:
                mode = (_mode(edf.select(column).na.drop().rdd.map(lambda r: r[0]).collect())[0][0]).item()
                df   = df.na.fill(value=mode)
    if method == "ffill" or method == "pad":
        if engine in IN_PANDAS_ON_SPARK:
            from pyspark.pandas import config as ps_config
            spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
            ps_config.set_option('compute.ops_on_diff_frames', True)
        elif engine in KOALAS:
            from databricks.koalas import config as koalas_config
            koalas_config.set_option('compute.ops_on_diff_frames', True)
        if engine == KOALAS or engine == PANDAS_ON_SPARK or engine == PANDAS:
            for column in columns:
                df[column] = df[column].ffill()
        if engine == PYSPARK:
            for column in columns:
                w_forward = Window.partitionBy().orderBy(column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
                w_backward = Window.partitionBy().orderBy(column).rowsBetween(Window.currentRow,Window.unboundedFollowing)
                df = df.withColumn(column,last(column,ignorenulls=True).over(w_forward))\
                  .withColumn(column,first(column,ignorenulls=True).over(w_backward))
    if method == "bfill" or method == "backfill":
        if engine in IN_PANDAS_ON_SPARK:
            from pyspark.pandas import config as ps_config
            spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
            ps_config.set_option('compute.ops_on_diff_frames', True)
        elif engine in KOALAS:
            from databricks.koalas import config as koalas_config
            koalas_config.set_option('compute.ops_on_diff_frames', True)
        if engine == KOALAS or engine == PANDAS_ON_SPARK or engine == PANDAS:
            for column in columns:
                df[column] = data_convert(data_convert(df, as_type = PANDAS).bfill(), as_type = engine)[column]
        if engine == PYSPARK:
            for column in columns:
                w_forward = Window.partitionBy().orderBy(column).rowsBetween(Window.unboundedPreceding,Window.currentRow)
                w_backward = Window.partitionBy().orderBy(column).rowsBetween(Window.currentRow,Window.unboundedFollowing)
                df = df.withColumn(column,first(column,ignorenulls=True).over(w_backward))\
                    .withColumn(column,last(column,ignorenulls=True).over(w_forward))
    if method == "interpolate":
        if engine in IN_PANDAS_ON_SPARK:
            from pyspark.pandas import config as ps_config
            ps_config.set_option('compute.ops_on_diff_frames', True)
        elif engine in KOALAS:
            from databricks.koalas import config as koalas_config
            koalas_config.set_option('compute.ops_on_diff_frames', True)
        if engine == KOALAS:
            for column in df.columns:
                df[column] = data_convert(data_convert(df, as_type = PANDAS)[column].interpolate(), as_type = engine)
        if engine == PANDAS_ON_SPARK:
            spark.conf.set('spark.sql.execution.arrow.enabled', 'true')
            for column in df.columns:
                df[column] = data_convert(data_convert(df, as_type = PANDAS).interpolate(), as_type = engine)[column]
        elif engine == PANDAS:
            df[columns] = df[columns].interpolate()
        if engine == PYSPARK:
            # CURRENT IMPLEMENTATION IS JUST TRYING TO BRUTE FORCE A PANDAS IMPLEMENTATION, TOO LAZY TO WORK THIS AROUND.
            try:
                for column in df.columns:
                    df = df.unionByName(data_convert(data_convert(df.select(column), as_type=PANDAS).interpolate(), as_type=PYSPARK), allowMissingColumns=True)
                    df.drop(column)
                    df.withColumnRenamed(f"{column}_t", column)
            except:
                raise
    if method == "drop":
        if engine == KOALAS or engine == PANDAS_ON_SPARK or engine == PANDAS:
            df = df.dropna()
        if engine == PYSPARK:
            df = df.na.drop()
    return df


##### TEXT FUNCTIONS #####
def text_similarity(original_source, comparison_text, engine="auto", threshold=None, dict_attr='values', columns=None, new_column=True) -> Union[float, List, Mapping, PandasDataFrame, PandasSeries, SparkDataFrame, PolarsDataFrame, PolarsSeries]:
    """
    Calculate the similarity between two texts using different algorithms.

    :param original_source: The original text or column-like structure.
    :type original_source: str, list, dict, pandas.DataFrame, pandas.Series, pyspark.sql.DataFrame
    :param comparison_text: The text to compare with the original text.
    :type comparison_text: str
    :param engine: The algorithm/engine to use for similarity calculation. Defaults to "auto".
    :type engine: str, optional
    :param threshold: The similarity threshold. If specified, the result is a boolean indicating whether the similarity
                      is above or equal to the threshold. If not specified, the similarity value is returned.
    :type threshold: float, optional
    :param dict_attr: An option to specify what to use as an iterator for the similarities, "values" or "keys". Defaults to "values".
    :type dict_attr: str
    :param columns: Used to define the columns to apply the similarity to. Defaults to None.
    :type columns: List[str], optional
    :param new_column: Whether to add new columns for similarity scores or overwrite existing columns. Defaults to False.
    :type new_column: bool, optional
    :return: The DataFrame with similarity scores.
    :rtype: Union[float, List, Mapping, pandas.DataFrame, pandas.Series, pyspark.sql.DataFrame, polars.DataFrame, polars.Series]
    """

    def text_similarity_wrapper(original_source, comparison_text, engine, threshold):
        # Validate threshold if specified
        if threshold is not None and (not isinstance(threshold, bool)):
            raise ValueError("Threshold must be a float between 0 and 1.")

        # Validate original_source type
        if not isinstance(original_source, str):
            raise ValueError(f"Original text must be a string, not type {type(original_source)}")
        
        # Validate original_source type
        if not isinstance(comparison_text, str):
            raise ValueError(f"Comparison text must be a string, not type {type(comparison_text)}")

        # Validate engine
        if engine == "auto":
            if len(original_source) > 100 or len(comparison_text) > 100:
                engine = "cosine"
            else:
                engine = "jaro"
        elif engine not in ["cosine", "jaro", "jaro_winkler", "levenshtein", "damerau_levenshtein", "hamming"]:
            raise ValueError(f"Invalid engine: {engine}")



        # Calculate similarity based on the engine
        if engine == "cosine":
            similarity = cosine.normalized_similarity(original_source, comparison_text)
        elif engine == "jaro":
            similarity = jaro.normalized_similarity(original_source, comparison_text)
        elif engine == "jaro_winkler":
            similarity = jaro_winkler(original_source, comparison_text)
        elif engine == "levenshtein":
            similarity = levenshtein.normalized_similarity(original_source, comparison_text)
        elif engine == "damerau_levenshtein":
            similarity = damerau_levenshtein.normalized_similarity(original_source, comparison_text)
        elif engine == "hamming":
            similarity = hamming.normalized_similarity(original_source, comparison_text)

        # Return similarity or boolean result based on threshold
        if threshold is None:
            return similarity
        else:
            return similarity >= threshold

    # Register the text_similarity_wrapper function as a UDF
    spark.udf.register("text_similarity_wrapper", text_similarity_wrapper, DoubleType())

    if isinstance(original_source, str):
        # RETURN THE NORMAL CALL WHEN THE TYPE IS STR
        return text_similarity_wrapper(original_source, comparison_text, engine, threshold)
    elif isinstance(original_source, list):
        # RETURN A LIST MAPPED BY THE LIST ITEMS.
        if any(unexpected_type := list(filter(None,[item if not isinstance(item, str) else None for item in original_source]))):
            raise ValueError(f"Found invalid item in list of type {', '.join(list(map(lambda item: type(item).name, unexpected_type)))}")
        
        return list(map(lambda text: text_similarity_wrapper(text, comparison_text, engine, threshold), original_source))
    elif isinstance(original_source, dict):
        # RETURNS A DICT MAPPED BY KEYS OR VALUES.
        if dict_attr.lower() == "value" or dict_attr.lower() == "values":
            iterator = original_source.items()
            return dict(map(lambda item: (item[0],text_similarity_wrapper(item[1], comparison_text, engine, threshold)), iterator))
        elif dict_attr.lower() == "key" or dict_attr.lower() == "keys":
            iterator = original_source.keys()
            return dict(map(lambda item: (item,text_similarity_wrapper(item, comparison_text, engine, threshold)), iterator))
        else:
            raise ValueError(f"Parameter dict_attr not recognized {dict_attr}")
    elif isinstance(original_source, PandasDataFrame):
        # RETURN A DATAFRAME AFTER APPLYING THE WRAPPER TO SELECTED COLUMNS.
        if columns is None:
            columns = original_source.columns.tolist()

        similarity_df = original_source[columns].applymap(
            lambda item: text_similarity_wrapper(item, comparison_text, engine, threshold)
        )

        if new_column:
            similarity_df = PandasConcat([original_source, similarity_df.add_suffix('_similarity')], axis=1)
        else:
            similarity_df.columns = original_source.columns

        return similarity_df
    elif isinstance(original_source, PandasSeries):
        # RETURN A SERIES AFTER APPLYING THE WRAPPER TO ALL THE ITEMS IN IT.
        return original_source.apply(lambda item: text_similarity_wrapper(item, comparison_text, engine, threshold))
    elif isinstance(original_source, SparkDataFrame):
        if columns is None:
            columns = original_source.columns

        similarity_exprs = []
        for col in columns:
            # Generate a new column name for similarity scores
            if new_column:
                new_col_name = col + "_similarity"
                # Check if the new column name already exists in the DataFrame
                while new_col_name in original_source.columns:
                    new_col_name += "_1"
            else:
                new_col_name = col

            # Create the expression to calculate the similarity score
            expr_col = expr("text_similarity_wrapper({}, '{}', '{}', {})".format(
                col, comparison_text, engine, "CAST({} AS DOUBLE)".format(threshold) if threshold is not None else "NULL"
            )).alias(new_col_name)

            # Append the expression to the list
            similarity_exprs.append(expr_col)

        # Select the similarity expressions along with the original columns
        # Overwrite the existing columns inplace
        if new_column:
            df = original_source.select("*", *similarity_exprs)
        else:
            df = original_source.select(*[expr_col if col in columns else col for col, expr_col in zip(original_source.columns, similarity_exprs)])

        return df
    elif isinstance(original_source, PolarsDataFrame):
        # Multiple columns case
        if columns is None:
            columns = original_source.columns

        def text_similarity_func(item):
            return text_similarity_wrapper(item, comparison_text, engine, threshold)
        
        new_columns = list()

        for column in columns:
            # Apply text_similarity to the selected column
            similarity_col = f"{column}_similarity"
            similarity = original_source[column].apply(lambda x: text_similarity_func(x), return_dtype=PolarsFloat64())
            new_columns.append(similarity.alias(similarity_col))

        if new_column:
            original_source = original_source.with_columns(new_columns)
        else:
            for column in columns:
                original_source = original_source.drop(column)
            original_source = original_source.with_columns(new_columns)

        return original_source
    elif isinstance(original_source, PolarsSeries):
        # RETURN A SERIES AFTER APPLYING THE WRAPPER TO ALL THE ITEMS IN IT.
        if columns is None:
            columns = original_source.columns
        def text_similarity_func(item):
            return text_similarity_wrapper(item, comparison_text, engine, threshold)

        return original_source.select(columns).apply(text_similarity_func)
    else:
        raise ValueError("Unsupported data type: {}".format(type(original_source)._name_))


def encode(df: Union[pandas.DataFrame, KoalasDataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame], 
           columns: List[str], 
           encoder: str = "categorical", 
           overwrite: bool = False,
           as_type: str = None) -> Union[pandas.DataFrame, KoalasDataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]:
    """
    Encodes columns in a dataframe using various encoders.

    :param df: A Pandas, Koalas, PySpark DataFrame or PySpark Koalas DataFrame to encode.
    :type df: Union[pandas.DataFrame, KoalasDataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]

    :param columns: The names of the columns to encode.
    :type columns: List[str]

    :param encoder: The encoding method to use, Possible values are "categorical" or "onehot". defaults to "categorical".
    :type encoder: str, optional

    :param overwrite: If True, overwrites the original columns with the encoded values, defaults to False.
    :type overwrite: bool, optional

    :param as_type: The data type to convert the encoded columns to, defaults to None.
    :type as_type: str, optional

    :return: A Pandas, Koalas, PySpark DataFrame or PySpark Koalas DataFrame with the encoded columns.
    :rtype: Union[pandas.DataFrame, KoalasDataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]
    """
    if not isinstance(columns, list):
        columns = [columns]
    
    engine = df_type(df)
    original_columns = df.columns

    df = data_convert(df, engine)
        
    if encoder in ["categorical", "string"]:
        for column in columns:
            if engine == PANDAS or engine == KOALAS or engine in IN_PANDAS_ON_SPARK:
                df = df.copy()
                if engine in IN_PANDAS_ON_SPARK:
                    from pyspark.pandas import config as ps_config
                    ps_config.set_option("compute.max_rows", None)
                elif engine in KOALAS:
                    from databricks.koalas import config as koalas_config
                    koalas_config.set_option("compute.max_rows", None)
                df[column] = df[column].astype("category")
                if overwrite:
                    df[column] = df[column].cat.codes
                else:
                    df[f"{column}_encoded"] = df[column].cat.codes
            elif engine == PYSPARK:
                df = df.alias("_copy")
                from pyspark.ml.feature import StringIndexer
                from pyspark.sql.functions import col
                if overwrite:
                    df = df.withColumn(column, df[column].cast(StringType()))
                    indexer = StringIndexer(inputCol=column, outputCol=f"{column}_encoded")
                    indexer_fitted = indexer.fit(df)
                    df = indexer_fitted.transform(df).drop(column).withColumnRenamed(f"{column}_encoded", column).withColumn(column,col(column).cast("int")).select(original_columns)
                else:
                    df = df.withColumn(column, df[column].cast(StringType()))
                    indexer = StringIndexer(inputCol=column, outputCol=f'{column}_encoded')
                    indexer_fitted = indexer.fit(df)
                    df = indexer_fitted.transform(df).withColumn(f"{column}_encoded",col(f"{column}_encoded").cast("int"))
    elif encoder in ["onehot", "one-hot", "one hot"]:
        for column in columns:
            if engine == PANDAS or engine == KOALAS or engine in IN_PANDAS_ON_SPARK:
                if engine in IN_PANDAS_ON_SPARK:
                    raise NotImplementedError("Current version doesn't support this opperation for pyspark.pandas.")
                    from pyspark.pandas import config as ps_config
                    ps_config.set_option("compute.max_rows", None)
                elif engine in KOALAS:
                    raise NotImplementedError("Current version doesn't support this opperation for databricks.koalas.")
                    from databricks.koalas import config as koalas_config
                    koalas_config.set_option("compute.max_rows", None)
                df = df.copy()
                df[column] = df[column].astype("category")
                uniques = len(df["firstName"].unique()) - 1
                if overwrite:
                    df[column] = df[column].cat.codes
                    df[column] = df[column].apply(lambda category: (uniques, [category], [1.0]))
                else:
                    df[f"{column}_encoded"] = df[column].cat.codes
                    df[f"{column}_encoded"] = df[f"{column}_encoded"].apply(lambda category: (uniques, [category], [1.0]))
            if engine == PYSPARK:
                from pyspark.ml.feature import OneHotEncoder, StringIndexer
                from pyspark.sql.functions import col
                df = df.alias("_copy")
                df = df.withColumn(column, df[column].cast(StringType()))
                encoder = OneHotEncoder(inputCols=[f'{column}_encoded'], outputCols=[f'{column}_onehot'])
                indexer = StringIndexer(inputCol=column, outputCol=f'{column}_encoded')
                indexer_fitted = indexer.fit(df)
                df = indexer_fitted.transform(df).withColumn(f"{column}_encoded",col(f"{column}_encoded").cast("int"))
                df = encoder.fit(df).transform(df).drop(f'{column}_encoded')
    if as_type:
        return data_convert(df, as_type)
    else:
        return data_convert(df, engine)

##### HIVECODE 2.0 DEV

from hivecore.functions import Context, ConcreteStrategy, pyspark_concat

@Context
class Encoder:
    """
    The Encoder class is responsible for providing a unified interface for categorical encoding in pandas, PySpark, and Polars dataframes. 
    The actual encoding is implemented by the strategy classes: PandasEncoder, SparkEncoder, and PolarsEncoder.

    Attributes:
        data: A DataFrame that can be a pandas, PySpark, or Polars DataFrame.
    """

    def __init__(self, data):
        """
        Constructor of the Encoder class.

        :param data: The DataFrame to be encoded.
        :type data: pandas.DataFrame or pyspark.sql.DataFrame or polars.DataFrame
        """
        self.data = data

    def encode(self, method = 'auto', columns = None, target_col = None, new_col = True):
        """
        Encode the specified columns in the DataFrame using the specified method. 
        The actual encoding is delegated to the strategy classes: PandasEncoder, SparkEncoder, or PolarsEncoder.

        :param method: The encoding method to use. Possible values are: 'auto', 'one_hot', 'label', 'ordinal', 'binary', 'frequency', 'target'.
        :type method: str
        :param columns: The columns to encode. If None, all columns are encoded.
        :type columns: list of str or None
        :param target_col: The target column for target encoding. This parameter is used only when method is 'target'.
        :type target_col: str or None
        :param new_col: If True, new columns are created for the encoded variables. If False, the original columns are replaced.
        :type new_col: bool

        :return: The encoded DataFrame.
        :rtype: pandas.DataFrame or pyspark.sql.DataFrame or polars.DataFrame
        """
        # Setter for the strategy we are going to use.
        if isinstance(self.data, PandasDataFrame):
            self.set_strategy(PandasEncoder())
        elif isinstance(self.data, SparkDataFrame):
            self.set_strategy(SparkEncoder())
        elif isinstance(self.data, PolarsDataFrame):
            self.set_strategy(PolarsEncoder())

        return self.strategy.concrete_encode(data=self.data, method=method, columns=columns, target_col=target_col, new_col=new_col)


@ConcreteStrategy('concrete_encode')
class PandasEncoder:
    """
    A concrete encoding strategy for Pandas dataframes. 

    :methods: 
        - concrete_encode: Main method to perform encoding on the dataframe. Handles the encoding process including 
                           determining the appropriate encoding method, applying the encoding, and managing the 
                           output dataframe.
        - select_method: Determines the appropriate encoding method based on the properties of the data.
        - one_hot: Performs one-hot encoding on a given Pandas Series.
        - label: Performs label encoding on a given Pandas Series.
        - ordinal: Performs ordinal encoding on a given Pandas Series.
        - binary: Performs binary encoding on a given Pandas Series.
        - frequency: Performs frequency encoding on a given Pandas Series.
        - target: Performs target encoding on a given Pandas Series using a specified target column.
    """
    @staticmethod
    def concrete_encode(data, method='auto', columns=None, target_col=None, new_col=True):
        """
        Encode the specified columns in the pandas DataFrame using the specified method.

        :param data: The pandas DataFrame to be encoded.
        :type data: pandas.DataFrame
        :param method: The encoding method to use. Possible values are: 'auto', 'one_hot', 'label', 'ordinal', 'binary', 'frequency', 'target'.
        :type method: str
        :param columns: The columns to encode. If None, all columns are encoded.
        :type columns: list of str or None
        :param target_col: The target column for target encoding. This parameter is used only when method is 'target'.
        :type target_col: str or None
        :param new_col: If True, new columns are created for the encoded variables. If False, the original columns are replaced.
        :type new_col: bool

        :return: The encoded pandas DataFrame.
        :rtype: pandas.DataFrame
        """
        # Validate columns
        if not columns:
            columns = data.columns.to_list()

        # Define automatic selection behavior
        if method == 'auto':
            encoded_columns = dict(map(lambda column: (column, PandasEncoder.select_method(data[column])), columns))
        else:
            encoded_columns = {column: method for column in columns}

        # Apply selected encoding method to each column
        for column, col_method in encoded_columns.items():
            if col_method == 'target':
                # Apply target
                if data[column].dtype == 'object' and data[target_col].dtype in ['int64', 'float64']:
                    encoded = PandasEncoder.target(data, column, target_col)
                else:
                    raise ValueError("Target encoding requires a categorical column and a numeric target column.")
            else:
                encoded = getattr(PandasEncoder, col_method.replace('one-hot', 'one_hot'))(data[column])

            # Handle multiple columns
            if isinstance(encoded, pd.DataFrame):
                for new_col_name, new_col_data in encoded.items():
                    if new_col:
                        data[f'{column}_{new_col_name}_encoded'] = new_col_data
                    else:
                        data[column] = new_col_data
            else:
                if new_col:
                    data[f'{column}_encoded'] = encoded
                else:
                    data[column] = encoded

        return data

    @staticmethod
    def select_method(data):
        """
        Select the encoding method based on the data type and uniqueness of the data.

        :param data: The pandas Series to analyze.
        :type data: pandas.Series

        :return: The selected encoding method. 'one-hot' for categorical data with low cardinality,
                 'label' for categorical data with high cardinality or other data types,
                 'ordinal' for numerical data.
        :rtype: str
        """
        if data.dtype == 'object':
            if data.nunique() / data.count() < 0.05:
                return 'one-hot'
            else:
                return 'label'
        elif data.dtype in ['int64', 'float64']:
            return 'ordinal'
        else:
            return 'label'

    @staticmethod
    def one_hot(data):
        """
        Perform one-hot encoding on the data.

        :param data: The pandas Series to encode.
        :type data: pandas.Series

        :return: The one-hot encoded pandas DataFrame.
        :rtype: pandas.DataFrame
        """
        return pd.get_dummies(data)#, prefix=data.name)

    @staticmethod
    def label(data):
        """
        Perform label encoding on the data.

        :param data: The pandas Series to encode.
        :type data: pandas.Series

        :return: The label encoded pandas Series.
        :rtype: pandas.Series
        """
        _, unique = pd.factorize(data)
        return data.replace({val: i for i, val in enumerate(unique)})

    @staticmethod
    def ordinal(data):
        """
        Perform ordinal encoding on the data.

        :param data: The pandas Series to encode.
        :type data: pandas.Series

        :return: The ordinal encoded pandas Series.
        :rtype: pandas.Series
        """
        return data.rank(method='dense').astype(int)

    @staticmethod
    def binary(data):
        """
        Perform binary encoding on the data.

        :param data: The pandas Series to encode.
        :type data: pandas.Series

        :return: The binary encoded pandas Series.
        :rtype: pandas.Series
        """
        # Convert the data to numerical labels
        _, unique = pd.factorize(data)
        labeled_data = data.replace({val: i for i, val in enumerate(unique)})

        # Convert the labeled data to binary representation and drop the '0b' prefix
        binary_series = labeled_data.apply(lambda x: format(x, '08b'))

        return binary_series


    @staticmethod
    def frequency(data):
        """
        Perform frequency encoding on the data.

        :param data: The pandas Series to encode.
        :type data: pandas.Series

        :return: The frequency encoded pandas Series.
        :rtype: pandas.Series
        """
        # Calculate the frequency of each unique value
        frequency_df = data.value_counts(normalize=True)

        # Map the frequencies back onto the original data
        result_series = data.map(frequency_df)

        return result_series

    @staticmethod
    def target(data, column, target_col):
        """
        Perform target encoding on the data.

        :param data: The pandas DataFrame to encode.
        :type data: pandas.DataFrame
        :param column: The column to target encode.
        :type column: str
        :param target_col: The target column for target encoding.
        :type target_col: str

        :return: The target encoded pandas Series.
        :rtype: pandas.Series
        """
        # Target encode the specified column using the mean of the target column
        if target_col is None:
            raise ValueError("Parameter target_col is missing.")
        target_mean = data.groupby(column)[target_col].transform('mean')
        return target_mean


import polars as pl

@ConcreteStrategy('concrete_encode')
class PolarsEncoder:
    """
    A concrete encoding strategy for Polars dataframes. 

    :methods: 
        - concrete_encode: Main method to perform encoding on the dataframe. Handles the encoding process including 
                           determining the appropriate encoding method, applying the encoding, and managing the 
                           output dataframe.
        - select_method: Determines the appropriate encoding method based on the properties of the data.
        - one_hot: Performs one-hot encoding on a given Polars Series.
        - label: Performs label encoding on a given Polars Series.
        - ordinal: Performs ordinal encoding on a given Polars Series.
        - binary: Performs binary encoding on a given Polars Series.
        - frequency: Performs frequency encoding on a given Polars Series.
        - target: Performs target encoding on a given Polars Series using a specified target column.
    """
    def concrete_encode(self, data, method = 'auto', columns = None, target_col = None, new_col = True):
        """
        Main method to perform encoding on the dataframe. Handles the encoding process including determining the 
        appropriate encoding method, applying the encoding, and managing the output dataframe.

        :param data: The Polars dataframe to encode.
        :type data: polars.DataFrame
        :param method: The encoding method to use. If 'auto', the method is selected based on the data.
        :type method: str
        :param columns: The columns to encode. If None, all columns are encoded.
        :type columns: list
        :param target_col: The target column for target encoding.
        :type target_col: str
        :param new_col: If True, the encoded data is added as new columns. If False, the original columns are replaced.
        :type new_col: bool

        :return: The encoded dataframe.
        :rtype: polars.DataFrame
        """
        # Validate columns
        if not columns:
            columns = data.columns

        # Select columns to encode
        to_encode_data = data.select(columns)

        # Define automatic selection behavior
        if method == 'auto':
            encoded_columns = dict(map(lambda column: (column, self.select_method(to_encode_data[column])), columns))
        else:
            encoded_columns = {column: method for column in columns}

        def apply_series_encoding(encoded_data):
            column, col_method = encoded_data[0], encoded_data[1]

            if col_method == 'target':
                # Apply target
                encoded = self.target(data=data[column], target_col= data[target_col] if  data[target_col] is not None else data[data.columns[-1]])
            else:
                encoded = getattr(self, col_method.replace('one-hot', 'one_hot'))(data=data[column])

            if isinstance(encoded, PolarsDataFrame):
                return encoded
            elif isinstance(encoded, PolarsSeries):
                return encoded.to_frame()

        encoded_data = list(map(apply_series_encoding, encoded_columns.items()))
        encoded_data = PolarsConcat(encoded_data, how='horizontal')

        if new_col:
            # Get the list of columns
            columns = encoded_data.columns

            # Create a dictionary mapping old column names to new column names
            rename_dict = {col: f'{col}_encoded' for col in columns}

            # Rename the columns
            encoded_data = encoded_data.rename(rename_dict)

            encoded_data = PolarsConcat([data, encoded_data], how='horizontal')
            return encoded_data
        else:
            # Get the list of columns
            columns = encoded_data.columns

            # Create a dictionary mapping old column names to new column names
            rename_dict = {col: col.replace('_encoded', '') for col in columns if '_encoded' in col}

            # Rename the columns
            encoded_data = encoded_data.rename(rename_dict)
            
            return encoded_data

    def select_method(self, data, target_col=None):
        """
        Determines the appropriate encoding method based on the properties of the data.

        :param data: The Polars series to analyze.
        :type data: polars.Series
        :param target_col: The target column for target encoding. 
        :type target_col: str

        :return: The selected encoding method. 
        :rtype: str
        """
        def is_categorical(data):
            # Check if the data is of string type and has more than one unique value.
            return data.dtype == pl.Utf8 and data.n_unique() > 1

        def is_ordinal(data):
            # Check if the data is of integer type and has a small number of unique values.
            return data.dtype == pl.Int32 and data.n_unique() <= 10

        def is_binary(data):
            # Check if the data has exactly two unique values (0 and 1).
            return data.n_unique() == 2 and set(data.unique().to_list()) == {0, 1}

        max_unique_one_hot = 100  # Maximum unique categories allowed for one-hot encoding

        # One-Hot Encoding
        if is_categorical(data) and data.n_unique() <= max_unique_one_hot:
            return 'one-hot'
        # Label Encoding
        elif is_ordinal(data) and data.n_unique() <= 10:
            return 'label'
        # Binary Encoding
        elif is_binary(data):
            return 'binary'
        # Frequency Encoding (or Target Encoding if target column is provided)
        elif is_categorical(data) and data.n_unique() <= max_unique_one_hot:
            if target_col in data.columns:
                return 'target'
            else:
                return 'frequency'
        # Default to Ordinal Encoding for other cases (if suitable)
        elif is_ordinal(data) and data.n_unique() <= 10:
            return 'ordinal'
        # Default to Label Encoding for other cases (if suitable)
        elif is_categorical(data) and data.n_unique() <= 10:
            return 'label'
        # Default to One-Hot Encoding for very large unique categories
        elif data.n_unique() <= max_unique_one_hot:
            return 'one-hot'
        else:
            return 'label'

    def one_hot(self, data):
        """
        Performs one-hot encoding on a given Polars Series.

        :param data: The Polars series to encode.
        :type data: polars.Series

        :return: The one-hot encoded series.
        :rtype: polars.Series
        """
        # One-Hot encode the specified columns
        return data.to_dummies()

    def label(self, data):
        """
        Performs label encoding on a given Polars Series.

        :param data: The Polars series to encode.
        :type data: polars.Series

        :return: The label encoded series.
        :rtype: polars.Series
        """
        unique_values = data.unique().to_list()
        mapping = {v: i for i, v in enumerate(unique_values)}

        data = data.apply(lambda x: mapping.get(x, -1), return_dtype=pl.Int32).alias(f"{data.name}")
        
        return data

    def ordinal(self, data):
        """
        Performs ordinal encoding on a given Polars Series.

        :param data: The Polars series to encode.
        :type data: polars.Series

        :return: The ordinal encoded series.
        :rtype: polars.Series
        """
        # Ordinal encode the specified column
        unique_values = data.unique()
        mapping = {value: i for i, value in enumerate(unique_values)}
        return data.apply(lambda x: mapping[x])

    # def binary(self, data):
    #     # Binary encode the specified series
    #     binary_encoded = data.apply(lambda x: format(x, '08b'))
    #     return binary_encoded

    def binary(self, data):
        """
        Performs binary encoding on a given Polars Series.

        :param data: The Polars series to encode.
        :type data: polars.Series

        :return: The binary encoded series.
        :rtype: polars.Series
        """
        # Convert the data to numerical labels
        unique_values = data.unique().to_list()
        mapping = {v: i for i, v in enumerate(unique_values)}
        labeled_data = data.apply(lambda x: mapping.get(x, -1), return_dtype=pl.Int32)

        # Convert the labeled data to binary representation
        binary_data = labeled_data.apply(lambda x: format(x, '08b'))

        return binary_data
        
    def frequency(self, data):
        """
        Performs frequency encoding on a given Polars Series.

        :param data: The Polars series to encode.
        :type data: polars.Series

        :return: The frequency encoded series.
        :rtype: polars.Series
        """
        col_name = data.name

        # Calculate the frequency of each category
        frequency_df = data.value_counts().select(col_name, (pl.col("counts") / len(data)).alias(f"{col_name}_encoded"))

        # Join the frequency data back to the original data
        frequency_encoded = data.to_frame().join(frequency_df, on=col_name, how='left')

        # Extract the frequency encoded column
        frequency_encoded_series = frequency_encoded[f'{col_name}_encoded']

        return frequency_encoded_series.alias(col_name)

    def target(self, data: pl.Series, target_col: pl.Series) -> pl.Series:
        """
        Performs target encoding on a given Polars Series using a specified target column.

        :param data: The Polars series to encode.
        :type data: polars.Series
        :param target_col: The target column for target encoding.
        :type target_col: polars.Series

        :return: The target encoded series.
        :rtype: polars.Series
        """
        if target_col is None:
            raise ValueError("Parameter target_col is missing.")
        
        # Create a DataFrame from the input series and the target series
        data_df = pl.DataFrame({
            "index": np.arange(len(data)),
            "data": data,
            "target": target_col
        })

        # Calculate the mean of the target values for each category in the data
        target_mean = data_df.groupby("data").agg(pl.col("target").mean()).rename({"target": "target_mean"})

        # Join the original DataFrame with the target mean DataFrame
        result_df = data_df.join(target_mean, on="data")

        # Sort the DataFrame by the index column to restore the original order
        result_df = result_df.sort("index")

        return result_df.select("target_mean").rename({"target_mean": f"{data.name}"})


from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.types import StringType
from pyspark.ml.linalg import DenseVector
from pyspark.sql.functions import udf, col, lit, count, mean, monotonically_increasing_id
from pyspark.sql.types import ArrayType, FloatType

class SparkEncoder:
    """
    A concrete encoding strategy for Spark dataframes. 

    :methods: 
        - concrete_encode: Main method to perform encoding on the dataframe. Handles the encoding process including 
                           determining the appropriate encoding method, applying the encoding, and managing the 
                           output dataframe.
        - select_method: Determines the appropriate encoding method based on the properties of the data.
        - one_hot: Performs one-hot encoding on a given Spark DataFrame.
        - label: Performs label encoding on a given Spark DataFrame.
        - ordinal: Performs ordinal encoding on a given Spark DataFrame.
        - binary: Performs binary encoding on a given Spark DataFrame.
        - frequency: Performs frequency encoding on a given Spark DataFrame.
        - target: Performs target encoding on a given Spark DataFrame using a specified target column.
    """
    @staticmethod
    def concrete_encode(data, method='auto', columns=None, target_col=None, new_col=True):
        """
        Main method to perform encoding on the dataframe. Handles the encoding process including determining the 
        appropriate encoding method, applying the encoding, and managing the output dataframe.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame
        :param method: The encoding method to use. If 'auto', the method is selected based on the data.
        :type method: str
        :param columns: The columns to encode. If None, all columns are encoded.
        :type columns: list
        :param target_col: The target column for target encoding.
        :type target_col: str
        :param new_col: If True, the encoded data is added as new columns. If False, the original columns are replaced.
        :type new_col: bool

        :return: The encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        if not columns:
            columns = data.columns

        if method == 'auto':
            encoded_columns = dict(map(lambda column: (column, SparkEncoder.select_method(data)), columns))
        else:
            encoded_columns = {column: method for column in columns}

        for column, col_method in encoded_columns.items():
            if col_method == 'target':
                encoded = SparkEncoder.target(data.select(column), data.select(target_col or data.columns[-1]))
            else:
                encoded = getattr(SparkEncoder, col_method.replace('one-hot', 'one_hot'))(data.select(column))

            if new_col:
                # Perform a union between the dataframes.
                data = pyspark_concat(data, encoded, axis=1)
            else:
                data = data.drop(column).withColumn(column, encoded)

        return data

    @staticmethod
    def select_method(data: DataFrame) -> str:
        """
        Determines the appropriate encoding method based on the properties of the data.

        :param data: The Spark dataframe to analyze.
        :type data: pyspark.sql.DataFrame

        :return: The selected encoding method. 
        :rtype: str
        """
        # Check the data type of the column
        dtype = data.dtypes[0][1]

        if dtype == 'string':
            # Use one-hot encoding for string columns
            return 'one-hot'
        elif dtype in ['int', 'double']:
            # Use binary encoding for numeric columns
            return 'binary'
        else:
            # Use label encoding as a default for other data types
            return 'label'

    @staticmethod
    def one_hot(data: DataFrame, dense_output: bool = False) -> DataFrame:
        """
        Performs one-hot encoding on a given Spark DataFrame.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame

        :return: The one-hot encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        column = data.columns[0]

        # Turn categories into numerical values
        indexer = StringIndexer(inputCol=column, outputCol=f'{column}_indexed')
        model = indexer.fit(data)
        indexed = model.transform(data)

        # Create one-hot encoded values
        encoder = OneHotEncoder(inputCol=f'{column}_indexed', outputCol=f'{column}_one_hot', dropLast=False)
        encoded = encoder.fit(indexed).transform(indexed)

        if dense_output:
            # Convert sparse vectors to dense
            to_dense = udf(lambda vector: DenseVector(vector.toArray()).values.tolist(), ArrayType(FloatType()))
            encoded = encoded.withColumn(f'{column}_one_hot', to_dense(encoded[f'{column}_one_hot']))

        # Create a new column for each value in the one-hot encoded vector
        def extract_from_vector(vector, index):
            try:
                return float(vector[index])
            except ValueError:
                return None

        extract_from_vector_udf = udf(extract_from_vector, FloatType())

        labels = model.labels
        for i in range(len(labels)):
            encoded = encoded.withColumn(f'{column}_{labels[i]}_encoded', extract_from_vector_udf(encoded[f'{column}_one_hot'], lit(i)).cast('int'))

        encoded = encoded.drop(column, f'{column}_one_hot', f'{column}_indexed')

        return encoded

    @staticmethod
    def label(data: DataFrame) -> DataFrame:
        """
        Performs label encoding on a given Spark DataFrame.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame

        :return: The label encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        column = data.columns[0]
        indexer = StringIndexer(inputCol=column, outputCol=f'{column}_encoded').fit(data).transform(data)
        indexer = indexer.withColumn(f'{column}_encoded', indexer[f'{column}_encoded'].cast('int'))
        return indexer.select(f'{column}_encoded')

    @staticmethod
    def ordinal(data: DataFrame) -> DataFrame:
        """
        Performs ordinal encoding on a given Spark DataFrame.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame

        :return: The ordinal encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        column = data.columns[0]
        indexer = StringIndexer(inputCol=column, outputCol=f'{column}_encoded').fit(data).transform(data)
        indexer = indexer.withColumn(f'{column}_encoded', indexer[f'{column}_encoded'].cast('int'))
        return indexer.select(f'{column}_encoded')

    @staticmethod
    def binary(data: DataFrame) -> DataFrame:
        """
        Performs binary encoding on a given Spark DataFrame.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame

        :return: The binary encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        
        # Extract column name
        column = data.columns[0]

        # Use StringIndexer to turn categories into numerical values
        # The numerical values will be saved in a new column '{column}_indexed'
        indexed = StringIndexer(inputCol=column, outputCol=f'{column}_indexed').fit(data).transform(data)

        # Use OneHotEncoder to turn the numerical values into a sparse matrix representation
        # The sparse matrix representation will be saved in a new column '{column}_sparse'
        indexed = OneHotEncoder(inputCol=f'{column}_indexed', outputCol=f'{column}_sparse').fit(indexed).transform(indexed)

        # Define a UDF (User-Defined Function) to turn the sparse matrix into a binary string
        def binary_string(value):
            return ''.join(str(int(x)) for x in value.toArray())

        binary_string_udf = udf(binary_string, StringType())

        # Apply the UDF to the '{column}_sparse' column and save the result in a new column '{column}_encoded'
        indexed = indexed.withColumn(f'{column}_encoded', binary_string_udf(indexed[f'{column}_sparse']))

        # Return the DataFrame with the original column replaced by the binary encoded column
        return indexed.select(f'{column}_encoded')

    @staticmethod
    def frequency(data: DataFrame) -> DataFrame:
        """
        Performs frequency encoding on a given Spark DataFrame.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame

        :return: The frequency encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        column_name = data.columns[0]

        # Group by the specified column and count occurrences of each value
        frequency_df = data.groupBy(column_name).agg((count("*") / data.count()).alias(f"{column_name}_encoded"))

        data = data.withColumn("order", monotonically_increasing_id())

        # Join the original DataFrame with the frequency DataFrame on the specified column
        result_df = data.join(frequency_df, how='left', on=column_name).orderBy("order").select(f"{column_name}_encoded")

        return result_df

    @staticmethod
    def target(data: DataFrame, target: DataFrame) -> DataFrame:
        """
        Performs target encoding on a given Spark DataFrame using a specified target column.

        :param data: The Spark dataframe to encode.
        :type data: pyspark.sql.DataFrame
        :param target_col: The target column for target encoding.
        :type target_col: str

        :return: The target encoded dataframe.
        :rtype: pyspark.sql.DataFrame
        """
        categorical_column = data.columns[0]
        target_column = target.columns[0]
        data_with_target = pyspark_concat(data, target, axis=1)

        # Add an index column to preserve the order of the data
        data_with_target = data_with_target.withColumn("index", F.monotonically_increasing_id())

        # Calculate the mean of the target variable for each category in the categorical column
        mean_df = data_with_target.groupBy(categorical_column).agg(mean(col(target_column)).alias("target_mean"))

        # Join the original DataFrame with the mean DataFrame on the categorical column
        result_df = data_with_target.join(mean_df, on=categorical_column)

        # Replace the categorical column with the target-encoded values
        result_df = result_df.withColumn(f'{categorical_column}_encoded', col("target_mean")).drop("target_mean")

        # Sort the DataFrame by the index column to restore the original order
        result_df = result_df.sort("index").drop("index")

        return result_df.select(f'{categorical_column}_encoded')
