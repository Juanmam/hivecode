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
from databricks.koalas import from_pandas

from typing import List
from os import system
from pyspark.sql.functions import abs as sabs, max as smax, min as smin, mean as _mean, stddev as _stddev, count as scount, first, last
from sklearn.metrics.pairwise import cosine_similarity as sk_cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from pandas import DataFrame, concat
from typing import List

##### NUMERIC FUNCTIONS #####
def normalize(df, columns: List[str] = None, method: str = "max-abs", overwrite: bool = False):
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

        print(columns)
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


def replace_nas(df, columns: List[str] = None, method: str = "mean"):
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
def text_similarity(df, columns: List[str], method: str = "tfid", threshold: float = 0.95, overwrite: bool = False, engine: str = "cosine"):
    def cosine_similarity(documents: List[str], header: bool = True, engine="tfid", i:int = 0):
        if engine == "count":
            count_vect = CountVectorizer()
            trsfm = count_vect.fit_transform(documents)
        elif engine == "tfid":
            vectorizer = TfidfVectorizer()
            trsfm=vectorizer.fit_transform(documents)

        if header:
            docs = [f'Document {i}' for i in range(1, len(documents) + 1)]
        else:
            docs = documents

        vals = sk_cosine_similarity(trsfm[i:i+1], trsfm)[0].tolist()
    #     return vals
        return DataFrame(vals, columns=[documents[i]], index=docs)

    def hamming(s1, s2):
        if len(s1) != len(s2):
            raise ValueError('expected two strings of the same length')
        count = 0
        for i in range(len(s1)):
            if s1[i] != s2[i]:
                count += 1
        return count

    def jaro(s1, s2):

        if s1 == s2:
            return 1.0

        len_s1 = len(s1)
        len_s2 = len(s2)

        if len_s1 == 0 or len_s2 == 0:
            return 0.0
        if len_s1 > len_s2:
            s1, s2 = s2, s1
            len_s1, len_s2 = len_s2, len_s1

        # Maxumum distance upto which matching is allowed
        search_range = (len_s1 + 1) // 2
        # search_range = floor(max(len_s1, len_s2) / 2) - 1

        match = 0
        match_s1 = [0] * len_s1
        match_s2 = [0] * len_s2
        # Check if there is any matches
        for i in range(len_s1):
            start = max(0, i - search_range)
            end = min(len_s2, i + search_range + 1)
            for j in range(start, end):
                if s1[i] == s2[j] and match_s2[j] == 0:
                    match_s1[i] = 1
                    match_s2[j] = 1
                    match += 1
                    break

        if not match:
            return 0.0

        # Number of transpositions
        trans = k = 0
        for i in range(len_s1):
            if match_s1[i]:
                while match_s2[k] == 0:
                    k += 1
                if s1[i] != s2[k]:
                    k += 1
                    trans += 1
                else:
                    k += 1

        trans = trans // 2

        return ((match / len_s1 + match / len_s2 + (match - trans) / match ) / 3.0)

    def damerau_levenshtein(s1, s2):
        len_s1 = len(s1)
        len_s2 = len(s2)
        max_dist = len_s1 + len_s2
        charas = {}
        arr = [[max_dist for _ in range(len_s2 + 2)] for _ in range(len_s1 + 2)]

        for i in range(1, len_s1 + 2):
            arr[i][1] = i - 1
        for j in range(1, len_s2 + 2):
            arr[1][j] = j - 1

        for i in range(2, len_s1 + 2):
            temp = 1
            for j in range(2, len_s2 + 2):
                k = charas.get(s2[j-2], 1)
                l = temp
                cost = 0 if s1[i-2] == s2[j-2] else 1
                if not cost:
                    temp = j
                arr[i][j] = min(
                    arr[i][j-1] + 1,
                    arr[i-1][j] + 1,                      
                    arr[i-1][j-1] + cost,
                    arr[k-1][l-1] + max((i-k-1),(j-l-1)) + 1 # transposition
                    )
            charas[s1[i-2]] = i

        return arr[-1][-1]

    def jaro_winkler(s1, s2):
        jaro_distance = jaro(s1, s2)

        # If the jaro_distance is above a threshold
        if jaro_distance > 0.7:
            prefix = 0
            for i in range(min(len(s1), len(s2))):
                if s1[i] == s2[i]:
                    prefix += 1
                else:
                    break
            prefix = min(4, prefix)
            jaro_distance += 0.1 * prefix * (1 - jaro_distance)

        return jaro_distance

    def levenshtein(s1, s2):
        if s1 == s2:
            return 0

        s1_len = len(s1)
        s2_len = len(s2)

        if s1_len == 0:
            return s2_len
        if s2_len == 0:
            return s1_len

        arr = [[j for j in range(s2_len + 1)] if i == 0 \
            else [i if j == 0 else 0 for j in range(s2_len+1)] for i in range(s1_len+1)]

        for i in range(1, s1_len+1):
            for j in range(1, s2_len+1):
                d1 = arr[i-1][j] + 1
                d2 = arr[i][j-1] + 1
                d3 = arr[i-1][j-1] + (0 if s1[i-1]==s2[j-1] else 1)
                arr[i][j] = min(d1, d2, d3)

        return arr[s1_len][s2_len]
    
    def most_similar(_df, threshold: float = 0.95):
                    similarity_score = _df[_df.columns.to_list()[0]].nlargest(2).tail(1).sum()
                    if similarity_score >= threshold:
                        return list(_df[_df.columns.to_list()[0]].nlargest(2).tail(1).index)[0]
                    else:
                        return list(_df[_df.columns.to_list()[0]].nlargest(2).head(1).index)[0]

    if df_type(df) in [PANDAS, KOALAS, PANDAS_ON_SPARK]:
        df = df.copy()
    elif df_type(df) == PYSPARK:
        df = df.alias("_copy")

    if engine == "cosine_similarity" or engine == "cosine similarity" or engine == "cos" or engine == "cosine":
        for col_name, corpus in zip(columns,to_list(df, columns)):
            for i in range(len(corpus)):
                _df = cosine_similarity(corpus, False, method, i)
                if overwrite:
                    df.loc[df[col_name] == corpus[i], f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus[i], f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df
    elif engine == "jaro":
        for col_name, column in zip(columns, to_list(df, columns)):
            for corpus in column:
                _df = DataFrame(list(map(lambda _corpus: jaro(corpus, _corpus), column)), columns = [corpus], index = column)
                if overwrite:
                    df.loc[df[col_name] == corpus, f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus, f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df
    elif engine == "levenshtein":
        for col_name, column in zip(columns, to_list(df, columns)):
            for corpus in column:
                _df = DataFrame(list(map(lambda _corpus: levenshtein(corpus, _corpus), column)), columns = [corpus], index = column)
                if overwrite:
                    df.loc[df[col_name] == corpus, f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus, f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df
    elif engine == "damerau_levenshtein" or engine == "damerau levenshtein":
        for col_name, column in zip(columns, to_list(df, columns)):
            for corpus in column:
                _df = DataFrame(list(map(lambda _corpus: damerau_levenshtein(corpus, _corpus), column)), columns = [corpus], index = column)
                if overwrite:
                    df.loc[df[col_name] == corpus, f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus, f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df
    elif engine == "jaro_winkler" or engine == "jaro winkler":
        for col_name, column in zip(columns, to_list(df, columns)):
            for corpus in column:
                _df = DataFrame(list(map(lambda _corpus: jaro_winkler(corpus, _corpus), column)), columns = [corpus], index = column)
                if overwrite:
                    df.loc[df[col_name] == corpus, f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus, f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df
    elif engine == "hamming":
        for col_name, column in zip(columns, to_list(df, columns)):
            for corpus in column:
                _df = DataFrame(list(map(lambda _corpus: hamming(corpus, _corpus), column)), columns = [corpus], index = column)
                if overwrite:
                    df.loc[df[col_name] == corpus, f"{col_name}"] = most_similar(_df, threshold=threshold)
                else:
                    df.loc[df[col_name] == corpus, f"{col_name}_similarity"] = most_similar(_df, threshold=threshold)
        if len(df) == 1:
            return df[0]
        else:
            return df


def encode(df, columns, encoder: str = "categorical", overwrite: bool = False, as_type: str = None):
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