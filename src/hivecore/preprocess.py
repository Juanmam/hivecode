
##### HIVECODE 2.0 DEV

from hivecore.patterns import Context, ConcreteStrategy
from hivecore.functions import LazyImport, pyspark_concat

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql.types import StringType
from pyspark.ml.linalg import DenseVector
from pyspark.sql.functions import udf, col, lit, count, mean, monotonically_increasing_id
from pyspark.sql.types import ArrayType, FloatType

LazyImport().from_("pyspark.context", "SparkContext")
LazyImport().from_("pyspark.sql.session", "SparkSession")

from pandas import DataFrame

import polars as pl

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
