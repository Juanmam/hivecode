from hivecore.constant import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PYSPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK
from hiveadb.function import get_spark, get_dbutils, data_convert, to_list, df_type

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from seaborn import diverging_palette, heatmap
from pandas import DataFrame, to_numeric
from numpy import triu, ones_like
from matplotlib.pyplot import figure
from typing import List, Union
from databricks.koalas import DataFrame as KoalasDataFrame

import pandas
import pyspark

def correlation(df: Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame], 
                columns: List[str], method: str = "pearson", 
                as_heatmap: bool = False, full_matrix: bool = True) -> Union[DataFrame, None]:
    """
    Calculates the correlation matrix between the given columns of a DataFrame.
    
    :param df: A DataFrame object, can be a Koalas, Pandas, PySpark, or PySpark Pandas DataFrame.
    :type df: Union[KoalasDataFrame, pandas.DataFrame, pyspark.sql.DataFrame, pyspark.pandas.DataFrame]
    
    :param columns: A list of column names to compute the correlation between.
    :type columns: List[str]
    
    :param method: The correlation method to use, must be one of "pearson", "kendall", or "spearman". 
                   Defaults to "pearson".
    :type method: str
    
    :param as_heatmap: If True, a heatmap of the correlation matrix will be displayed. Defaults to False.
    :type as_heatmap: bool
    
    :param full_matrix: If True, the full correlation matrix will be displayed. If False, only the 
                        lower triangle of the matrix will be displayed. Defaults to True.
    :type full_matrix: bool
    
    :return: If as_heatmap is True, None is returned and a heatmap is displayed. Otherwise, a Pandas DataFrame
             containing the correlation matrix is returned.
    :rtype: Union[pandas.DataFrame, None]
    """
    engine = df_type(df)
    if engine == PANDAS or engine == KOALAS or engine == PANDAS_ON_SPARK:
        try:
            corr_matrix_df = data_convert(df[columns].corr(method = method), as_type = "pandas")
        except:
            try:
                corr_matrix_df = data_convert(df[columns].fillna(df[columns].median()).corr(method = method), as_type = "pandas")
            except:
                raise
    elif engine == PYSPARK:
        # convert to vector column first
        vector_col = "corr_features"
        assembler = VectorAssembler(inputCols=df.columns, outputCol=vector_col)
        df_vector = assembler.transform(df).select(vector_col)

        # get correlation matrix
        matrix = Correlation.corr(df_vector, vector_col, method = method).collect()[0][0] 
        corr_matrix = matrix.toArray().tolist() 
        corr_matrix_df = DataFrame(data=corr_matrix, columns = columns, index= columns)
    if as_heatmap:
        figure(figsize=(16,5))
        
        mask = triu(ones_like(corr_matrix_df, dtype=bool))
        cmap = diverging_palette(230, 20, as_cmap=True)
        if not full_matrix:
            return heatmap(corr_matrix_df, 
                xticklabels=corr_matrix_df.columns.values,
                yticklabels=corr_matrix_df.columns.values, annot=True, mask = mask, cmap=cmap)
        else:
            return heatmap(corr_matrix_df, 
                xticklabels=corr_matrix_df.columns.values,
                yticklabels=corr_matrix_df.columns.values, annot=True, cmap=cmap)
    else:
        return corr_matrix_df