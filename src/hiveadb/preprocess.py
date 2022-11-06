from sklearn.metrics.pairwise import cosine_similarity as sk_cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from pandas import DataFrame, concat
from typing import List
from .functions import get_spark, get_dbutils, data_convert, to_list

##### NUMERIC FUNCTIONS #####
def normalize(df, columns: List[str] = None, method: str = "max-abs", overwrite: bool = False):
    engine = df_type(df)
    method = method.lower()
    if engine == PANDAS or engine == KOALAS or engine == PANDAS_ON_SPARK:
        if not columns:
            s = df.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all())
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
    df = df.copy()
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