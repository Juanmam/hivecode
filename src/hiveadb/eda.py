from .constants import PANDAS_TYPES, PYSPARK_TYPES, KOALAS_TYPES, PANDAS_ON_SPARK_TYPES, PANDAS, KOALAS, SPARK, PYSPARK, PANDAS_ON_SPARK, IN_PANDAS_ON_SPARK
from .functions import get_spark, get_dbutils, data_convert, to_list, df_type

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from seaborn import diverging_palette, heatmap
from pandas import DataFrame, to_numeric
from numpy import triu, ones_like
from matplotlib.pyplot import figure
from typing import List

def correlation(df, columns: List[str], method: str = "pearson", heatmap: bool = False, full_matrix: bool = True):
    """
    method {‘pearson’, ‘kendall’, ‘spearman’}
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
    if heatmap:
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