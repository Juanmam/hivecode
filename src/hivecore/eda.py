from typing import Optional, Union, List, Dict, Any
from pandas import DataFrame, Series
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype

from pyspark.sql.types import NumericType, DoubleType
from pyspark.sql.functions import col, count, when, skewness, kurtosis, lag, countDistinct, stddev
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

from re import sub
from math import ceil
from numpy import isnan, histogram, number as np_number, integer as np_integer, floating as np_floating, ndarray as np_ndarray, issubdtype, unique
from scipy.stats import t, pearsonr
from itertools import chain
from json import dumps, JSONEncoder

from collections import Counter

class PandasDataQuality:
    """
        The PandasDataQuality class implements the data quality checks for pandas dataframes. 

    """
    def _init_(self):
        pass

    @staticmethod
    def concrete_check(data: DataFrame, methods: Optional[List[str]] = None, valid_values_dict: Optional[Dict[str, List]] = None):
        """
        Perform data quality checks on the pandas DataFrame. 

        :param data: The pandas DataFrame to be checked.
        :type data: pandas.DataFrame
        :param methods: The list of methods to perform. If None, all checks are performed.
        :type methods: list of str or None
        :param valid_values_dict: A dictionary where the keys are column names and the values are lists of valid values.
        :type valid_values_dict: dict of lists
        :return: Results of the data quality checks.
        """
        results = {}

        # If no methods are specified, perform all checks
        if not methods:
            methods = ['missing_values', 'duplicates', 'constant_column', 'data_type', 'distribution', 
                       'statistical_summary', 'valid_values', 'correlation', 'uniqueness', 'regularity', 'cardinality']

        # Perform each check
        for method in methods:
            if method == "valid_values":
                if hasattr(PandasDataQuality, method):
                    results[method] = getattr(PandasDataQuality, method)(data, valid_values_dict)
                else:
                    raise ValueError(f"Invalid method: {method}")
            else:
                if hasattr(PandasDataQuality, method):
                    results[method] = getattr(PandasDataQuality, method)(data)
                else:
                    raise ValueError(f"Invalid method: {method}")

        return results

    @staticmethod
    def missing_values(data: DataFrame):
        return data.isnull().sum().to_dict()

    @staticmethod
    def duplicates(data: DataFrame):
        return data.duplicated().sum()

    @staticmethod
    def constant_column(data: DataFrame):
        return [col for col in data.columns if data[col].nunique() <= 1]

    @staticmethod
    def data_type(data: DataFrame):
        return dict(map(lambda item: (item[0], sub(r'[0-9]', '', item[1].name.capitalize())), data.dtypes.to_dict().items()))



    @staticmethod
    def distribution(df: DataFrame, n: int = 10000) -> dict:
        """
            Calculate distribution of each column in the dataframe. For numerical columns,
            it divides the data into bins and counts the number of data points in each bin.
            For non-numerical columns, it counts the frequency of each unique value.
            
            :param df: Input dataframe.
            :type df: pd.DataFrame
            :param n: Number of bins for numerical data. Default is 10.
            :type n: int
            :return: Dictionary with distribution data for each column.
            :rtype: dict
        """
        distribution = {}
        for column in df.columns:
            if is_numeric_dtype(df[column]):
                # Calculate distribution for numerical columns
                hist, bin_edges = histogram(df[column].dropna(), bins=n)
                distribution[column] = {"histogram": hist.tolist(), "bin_edges": bin_edges.tolist()}
            elif is_datetime64_any_dtype(df[column]):
                # Ignore datetime columns as the concept of distribution isn't applicable
                continue
            elif df[column].dropna().nunique() == 0:
                # Skip columns with all NaN values or no unique values
                continue
            else:
                # Calculate distribution for non-numerical columns
                distribution[column] = df[column].value_counts().to_dict()
        return distribution

    @staticmethod
    def statistical_summary(data: DataFrame):
        return data.describe().to_dict()

    @staticmethod
    def valid_values(data: DataFrame, valid_values_dict: Optional[Dict[str, List]] = None):
        if valid_values_dict is None:
            valid_values_dict = {col: data[col].unique().tolist() for col in data.columns}
        return data.apply(lambda col: (~col.isin(valid_values_dict[col.name])).sum()).to_dict()

    @staticmethod
    def correlation(data: DataFrame):
        numeric_cols = data.select_dtypes(include=[np_number]).columns
        return data[numeric_cols].corr().to_dict()

    @staticmethod
    def uniqueness(data: DataFrame):
        return data.nunique().to_dict()

    @staticmethod
    def regularity(data: DataFrame):
        datetime_cols = data.select_dtypes(include=['datetime64']).columns
        regularity_check = {}
        for col in datetime_cols:
            diff = data[col].sort_values().diff().value_counts()
            if len(diff) == 1:
                regularity_check[col] = True  # data is regularly spaced
            else:
                regularity_check[col] = False  # data is not regularly spaced
        return regularity_check

    @staticmethod
    def cardinality(data: DataFrame):
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns
        return data[categorical_cols].nunique().to_dict()
    
    @staticmethod
    def median(data: DataFrame) -> Series:
        """
            Calculate the median for numeric columns in a DataFrame.

            :param data: The input DataFrame.
            :type data: pd.DataFrame
            :return: A Series containing the median of each numeric column.
            :rtype: pd.Series
        """
        # Select only numeric columns from the DataFrame
        numeric_data = data.select_dtypes(include=['number'])

        # Calculate the median for each numeric column
        median_values = numeric_data.median()

        return median_values.to_dict()
    
    @staticmethod
    def mode(data: DataFrame) -> dict:
        """
            Calculate the mode for each column in a Pandas DataFrame.

            :param data: The input DataFrame.
            :type data: pd.DataFrame
            :return: A dictionary containing the mode value for each column.
            :rtype: dict
        """
        mode_values = {}
        for col_name in data.columns:
            mode_val = data[col_name].mode().iloc[0]
            mode_values[col_name] = mode_val

        return mode_values
    
    @staticmethod
    def variance(df: DataFrame) -> dict:
        """
            Calculate the variance for each column in a Pandas DataFrame.

            :param df: The input DataFrame.
            :type df: pd.DataFrame
            :return: A dictionary containing the variance value for each column.
            :rtype: dict
        """
        variance_values = {}
        for col_name in df.columns:
            if issubdtype(df[col_name].dtype, np_number):
                variance_val = df[col_name].var()
                variance_values[col_name] = variance_val

        return variance_values
    
    @staticmethod
    def skew(data: DataFrame) -> dict:
        """
            Calculate the skewness for each numeric column in a Pandas DataFrame.

            :param data: The input DataFrame.
            :type data: pd.DataFrame
            :return: A dictionary containing the skewness value for each numeric column.
            :rtype: dict
        """
        skew_values = {}
        for col_name in data.columns:
            if issubdtype(data[col_name].dtype, np_number):
                skew_val = data[col_name].skew()
                skew_values[col_name] = skew_val

        return skew_values

    @staticmethod
    def kurtosis(data: DataFrame):
        """
            Calculate the kurtosis for each column in a pandas DataFrame.

            :param data: The input DataFrame.
            :return: A dictionary containing the kurtosis value for each column.
        """
        kurtosis_values = {}
        for col_name in data.columns:
            if is_numeric_dtype(data[col_name]):
                kurtosis_val = data[col_name].kurtosis()
                kurtosis_values[col_name] = kurtosis_val

        return kurtosis_values

    @staticmethod
    def columns(data: DataFrame):
        return data.columns.tolist()
    
    @staticmethod
    def row_count(data: DataFrame) -> int:
        """
        Get the number of rows in the given pandas DataFrame.

        :param data: Input pandas DataFrame.
        :type data: pandas.DataFrame
        :return: Number of rows in the DataFrame.
        :rtype: int
        """
        return len(data)

    @staticmethod
    def sample(data: DataFrame, num_rows: int = 10) -> Union[List[str], List[List[Any]]]:
        """
            Get a random sample of a pandas DataFrame.

            :param data: The input DataFrame.
            :param num_rows: The number of rows to return as a sample.
            :return: A tuple containing a list of columns and a list of lists with the sample data.
        """
        sample_data = data.sample(n=num_rows, random_state=42)  # Randomly sample 'num_rows' rows
        columns = sample_data.columns.tolist()
        sample_list = sample_data.values.tolist()

        return columns, sample_list
    
    @staticmethod
    def representative_sample(data: DataFrame, desired_sample_size: float = 0.01, fixed_sample_size = 10000):
        def compute_sample_size(population_size: int, std_dev: float, margin_error: float = 5, confidence_level: float = 95) -> int:
            """
                Compute the sample size for a given population size, margin error, confidence level, and standard deviation.
                
                :param population_size: The total number of items in the population.
                :type population_size: int
                :param margin_error: The acceptable margin of error for the sample mean as a percentage.
                :type margin_error: float
                :param confidence_level: The desired confidence level as a percentage.
                :type confidence_level: float
                :param std_dev: The standard deviation of the population. 
                :type std_dev: float
                :return: The required sample size.
                :rtype: int
            """

            # Lookup the z-score for the given confidence level
            z_score_dict = {80: 1.28, 85: 1.44, 90: 1.645, 95: 1.96, 99: 2.57, 99.5: 2.807, 99.9: 3.291}
            z_score = z_score_dict.get(confidence_level, 1.96)  # Default to 95% confidence level

            # Convert margin of error from percentage to decimal
            margin_error_decimal = margin_error / 100

            # Calculate the sample size
            sample_size = ceil((z_score*2 * std_dev) / (margin_error_decimal*2))

            # Adjust sample size for finite population
            if population_size is not None:
                sample_size = ceil((sample_size / (1 + ((sample_size - 1) / population_size))))

            return sample_size
        
        samples = {}
    
        for column in data.columns:
            if data[column].dtype == 'object':  # or another condition identifying non-numeric data
                # Get the counts for each category
                counts = data[column].value_counts()
                # Determine the proportions for each category
                proportions = counts / len(data)
                
                samples_for_category = list()

                for category in data[column].unique():
                    # Get the number of samples for this category
                    num_samples = round(proportions[category] * desired_sample_size)
                    # Ensure we take at least one sample, if there are enough available
                    num_samples = max(num_samples, 1) if counts[category] >= 1 else 0
                    # Randomly sample from this category

                    if num_samples < 250:
                        num_samples = len(data[data[column] == category]) if len(data[data[column] == category]) < 500 else 500

                    samples_for_category.append(data[data[column] == category].sample(n=num_samples)[column].tolist())
                    
                # # Add these samples to your overall samples
                # if column not in samples:
                #     samples[column] = []
                samples[column] = list(chain.from_iterable(samples_for_category))
            elif issubdtype(data[column].dtype, np_number):
                data_ = data[column]
                population_size = len(data_)
                std_dev = data_.std()
                representative_size = compute_sample_size(population_size, std_dev)

                sample_data = data_.sample(n=representative_size, random_state=42)  # Randomly sample 'num_rows' rows

                samples[column] = sample_data.values.tolist()
            else:
                sample_data = data[column].sample(n=min(fixed_sample_size, len(data[column])), random_state=42)
                samples[column] = sample_data.values.tolist()

        return samples
    
    def correlation_p_values(self, data: DataFrame) -> dict:
        p_values = {}
        for col1 in data.columns:
            p_values[col1] = {}
            for col2 in data.columns:
                if data[col1].dtype in ('int64', 'float64') and data[col2].dtype in ('int64', 'float64'):
                    _, p = pearsonr(data[col1], data[col2])
                    p_values[col1][col2] = p if not isnan(p) else None
                else:
                    p_values[col1][col2] = None
        return p_values

    @staticmethod
    def calculate_memory_usage(data: DataFrame):
        """
            Estimate the memory usage of a pandas DataFrame.

            :param data: The pandas DataFrame to estimate memory usage for.
            :type data: pd.DataFrame
            :return: A tuple containing total memory usage in bytes and memory usage by column in bytes.
            :rtype: Tuple[int, Dict[str, int]]
        """
        # Get the number of rows in the DataFrame
        num_rows = data.shape[0]

        # Calculate the memory usage in bytes
        total_memory_usage_bytes = data.memory_usage(deep=True).sum()

        # Calculate memory usage by column
        memory_usage_by_column = {}
        for column in data.columns:
            column_memory_usage = data[column].memory_usage(deep=True)
            memory_usage_by_column[column] = column_memory_usage

        return total_memory_usage_bytes, memory_usage_by_column


class SparkDataQuality:
    """
    The SparkDataQuality class implements the data quality checks for pyspark dataframes. 

    """
    def _init_(self):
        pass

    @staticmethod
    def orchestrator_check(data: DataFrame, methods: Optional[List[str]] = None, valid_values_dict: Optional[Dict[str, List]] = None):
        """
        Perform data quality checks on the pyspark DataFrame. 

        :param data: The pyspark DataFrame to be checked.
        :type data: pyspark.sql.DataFrame
        :param methods: The list of methods to perform. If None, all checks are performed.
        :type methods: list of str or None
        :param valid_values_dict: A dictionary where the keys are column names and the values are lists of valid values.
        :type valid_values_dict: dict of lists
        :return: Results of the data quality checks.
        """
        results = {}

        # If no methods are specified, perform all checks
        if not methods:
            methods = ['missing_values', 'duplicates', 'constant_column', 'data_type', 'distribution', 
                       'statistical_summary', 'valid_values', 'correlation', 'uniqueness', 'regularity', 'cardinality']

        # Perform each check
        for method in methods:
            if method == "valid_values":
                if hasattr(SparkDataQuality, method):
                    results[method] = getattr(SparkDataQuality, method)(data, valid_values_dict)
                else:
                    raise ValueError(f"Invalid method: {method}")
            else:
                if hasattr(SparkDataQuality, method):
                    results[method] = getattr(SparkDataQuality, method)(data)
                else:
                    raise ValueError(f"Invalid method: {method}")

        return results

    @staticmethod
    def missing_values(data):
        # Use dictionary comprehension to calculate missing values count for each column
        missing_values_count = {
            column: data.where(col(column).isNull() | (col(column) == "")).agg(count(col(column))).collect()[0][0]
            for column in data.columns
        }
        return missing_values_count

    def duplicates(data: DataFrame) -> int:
        # Use groupBy and count functions to identify duplicate rows
        duplicate_count = data.groupBy(data.columns).count().filter("count > 1").count()
        return duplicate_count

    @staticmethod
    def constant_column(data: DataFrame) -> list:
        constant_columns = []
        for col_name in data.columns:
            distinct_values = data.select(col_name).distinct().collect()
            if len(distinct_values) == 1:
                constant_columns.append(col_name)
        return constant_columns

    @staticmethod
    def data_type(data: DataFrame) -> dict:
        data_types = {}
        for col_name, col_type in data.dtypes:
            data_types[col_name] = col_type
        return data_types

    @staticmethod
    def distribution(df: DataFrame, n: int = 100) -> Dict[str, DataFrame]:
        """
        Calculate distribution of each column in the dataframe. For numerical columns,
        it divides the data into bins and counts the number of data points in each bin.
        For non-numerical columns, it counts the frequency of each unique value.
        
        :param df: Input dataframe.
        :type df: DataFrame
        :param n: Number of bins for numerical data. Default is 10.
        :type n: int
        :return: Dictionary with distribution data for each column.
        :rtype: Dict[str, DataFrame]
        """
        distribution = {}
        for column in df.columns:
            if isinstance(df.schema[column].dataType, DoubleType):
                # Calculate distribution for numerical columns
                min_val, max_val = df.selectExpr("min({})".format(column), "max({})".format(column)).first()
                bin_width = (max_val - min_val) / n
                bins = [min_val + i*bin_width for i in range(n+1)]
                bin_labels = [i for i in range(n)]
                bin_expr = when(col(column) < bins[0], bin_labels[0])
                for i in range(1, len(bins)):
                    bin_expr = bin_expr.when(col(column) < bins[i], bin_labels[i-1])
                df_bin = df.withColumn(column + '_bin', bin_expr)
                distribution[column] = df_bin.groupBy(column + '_bin').count()
            else:
                # Calculate distribution for non-numerical columns
                distribution[column] = df.groupBy(column).count()
        return distribution

    @staticmethod
    def statistical_summary(data: DataFrame):
        """
        Calculate the statistical summary for each numeric column in the given DataFrame.

        Parameters:
            data (DataFrame): Input DataFrame.

        Returns:
            dict: A nested dictionary containing the statistical summary for each numeric column.
                The keys of the outer dictionary are column names, and the values are dictionaries
                containing 'mean', 'stddev', 'min', '25%', '50%', '75%', and 'max' statistics for each column.
        """
        # Initialize an empty dictionary to store the statistical summary results
        summary_dict = {}

        # Get the numeric columns from the DataFrame
        numeric_columns = [col_name for col_name, col_type in data.dtypes if col_type in ('double', 'float', 'int')]

        # Calculate the summary statistics for each numeric column
        for col_name in numeric_columns:
            summary = data.select(col_name).summary("mean", "stddev", "min", "25%", "50%", "75%", "max")
            summary_dict[col_name] = {
                "mean": float(summary.filter(col("summary") == "mean").select(col(col_name)).collect()[0][0]),
                "stddev": float(summary.filter(col("summary") == "stddev").select(col(col_name)).collect()[0][0]),
                "min": float(summary.filter(col("summary") == "min").select(col(col_name)).collect()[0][0]),
                "25%": float(summary.filter(col("summary") == "25%").select(col(col_name)).collect()[0][0]),
                "50%": float(summary.filter(col("summary") == "50%").select(col(col_name)).collect()[0][0]),
                "75%": float(summary.filter(col("summary") == "75%").select(col(col_name)).collect()[0][0]),
                "max": float(summary.filter(col("summary") == "max").select(col(col_name)).collect()[0][0]),
            }

        return summary_dict

    @staticmethod
    def valid_values(data: DataFrame, valid_values_dict: Optional[Dict[str, List]] = None) -> Dict[str, int]:
        """
        Check the validity of values in each column of the given DataFrame.

        Parameters:
            data (DataFrame): Input DataFrame.
            valid_values_dict (Optional[Dict[str, List]]): A dictionary containing valid values for each column.
                If not provided, it will be automatically generated from the unique values in each column.

        Returns:
            dict: A dictionary containing the count of invalid values in each column.
                The keys of the dictionary are column names, and the values are integers representing the count of
                invalid values in each column.
        """
        if valid_values_dict is None:
            # Generate valid_values_dict from unique values in each column
            valid_values_dict = {col_name: data.select(col_name).distinct().rdd.flatMap(lambda x: x).collect()
                                for col_name in data.columns}

        # Calculate the count of invalid values in each column
        invalid_values_count = {}
        for col_name in data.columns:
            invalid_values_count[col_name] = data.filter(~col(col_name).isin(valid_values_dict[col_name])).count()

        return invalid_values_count

    @staticmethod
    def distribution(data: DataFrame) -> dict:
        value_dist = {}
        for col_name in data.columns:
            # Count the occurrences of each unique value in the column
            value_counts = data.groupBy(col_name).agg(count("*").alias("count"))

            # Convert the DataFrame to a dictionary of values and their counts
            value_counts_dict = {
                row[col_name]: row["count"]
                for row in value_counts.collect()
            }

            value_dist[col_name] = value_counts_dict

        return value_dist

    @staticmethod
    def correlation(data: DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Calculate the correlation matrix for the given DataFrame.

        Parameters:
            data (DataFrame): Input DataFrame containing numerical columns.

        Returns:
            dict: A dictionary of dictionaries representing the correlation matrix.
                The keys of the outer dictionary are column names, and the values are
                dictionaries representing the correlation of each column with other columns.
                The keys of the inner dictionary are column names, and the values are the
                corresponding correlation coefficients.
        """
        # Select only numerical columns for correlation calculation
        numerical_columns = [col for col, dtype in data.dtypes if dtype in ['int', 'bigint', 'float', 'double']]
        numerical_data = data.select(*numerical_columns)

        # Assemble all numerical columns into a single vector column
        assembler = VectorAssembler(inputCols=numerical_columns, outputCol="features")
        assembled_data = assembler.transform(numerical_data)

        # Calculate the correlation matrix using the Correlation.corr function
        correlation_matrix = Correlation.corr(assembled_data, "features")

        # Get the correlation values from the matrix and convert it to a list
        corr_values = correlation_matrix.collect()[0][0].toArray().tolist()

        # Build the result dictionary
        result = {}
        for i, col in enumerate(numerical_columns):
            # For each column, create a dictionary with correlation values for other columns
            result[col] = {}
            for j, other_col in enumerate(numerical_columns):
                if i != j:
                    result[col][other_col] = corr_values[i][j]

        return result

    @staticmethod
    def uniqueness(data: DataFrame) -> Dict[str, float]:
        """
        Calculate the uniqueness of each column in the given DataFrame.

        Parameters:
            data (DataFrame): Input DataFrame.

        Returns:
            dict: A dictionary containing the uniqueness value for each column.
                The keys of the dictionary are column names, and the values are
                the corresponding uniqueness values.
        """
        # Calculate the number of rows in the DataFrame
        total_rows = data.count()

        # Initialize an empty dictionary to store uniqueness values
        uniqueness_values = {}

        # Iterate over each column in the DataFrame
        for col in data.columns:
            # Calculate the number of distinct values in the column
            distinct_values = data.select(col).distinct().count()

            # Calculate the uniqueness ratio for the column
            uniqueness_ratio = distinct_values / total_rows

            # Store the uniqueness value in the dictionary
            uniqueness_values[col] = uniqueness_ratio

        return uniqueness_values

    @staticmethod
    def regularity(data: DataFrame):
        """
        Check the regularity of datetime columns in the given DataFrame.

        Parameters:
            data (DataFrame): Input DataFrame.

        Returns:
            dict: A dictionary containing the regularity check result for each datetime column.
                The keys of the dictionary are column names, and the values are booleans
                indicating whether the data is regularly spaced (True) or not (False).
        """
        # Get the datetime columns from the DataFrame
        datetime_cols = [col_name for col_name, col_type in data.dtypes if col_type == 'timestamp']

        # Initialize an empty dictionary to store regularity check results
        regularity_check = {}

        # Iterate over each datetime column in the DataFrame
        for col in datetime_cols:
            # Create a Window specification to order the DataFrame by the datetime column
            window_spec = Window.orderBy(col)

            # Calculate the difference between consecutive values in the ordered DataFrame
            diff_col = lag(data[col]).over(window_spec)

            # Calculate the frequency counts of the differences
            diff_counts = data.select(diff_col).groupBy(diff_col).count()

            # If there is only one non-null difference, the data is regularly spaced
            regularity_check[col] = diff_counts.filter(diff_col.isNotNull()).count() == 1

        return regularity_check

    @staticmethod
    def cardinality(data: DataFrame) -> Dict[str, int]:
        """
            Calculate the cardinality (distinct count) of each column in the given DataFrame.

            Parameters:
                data (DataFrame): Input DataFrame.

            Returns:
                dict: A dictionary containing the cardinality of each column.
                    The keys of the dictionary are column names, and the values are integers
                    representing the distinct count of each column.
        """
        # Initialize an empty dictionary to store cardinality results
        cardinality_dict = {}

        # Iterate over each column in the DataFrame
        for col in data.columns:
            # Calculate the distinct count for the current column
            distinct_count = data.select(countDistinct(col)).collect()[0][0]
            cardinality_dict[col] = distinct_count

        return cardinality_dict
    
    @staticmethod
    def median(data: DataFrame):
        """
        Calculate the median for numeric columns in a PySpark DataFrame.

        :param data: The input DataFrame.
        :return: A dictionary containing the median of each numeric column.
        """
        numeric_columns = [
            field.name for field in data.schema.fields
            if isinstance(field.dataType, NumericType)
        ]

        # Handle non-numeric columns gracefully
        data_numeric = data.select(*numeric_columns)

        # Calculate the median for each numeric column
        median_values = []
        for col_name in numeric_columns:
            median_val = data_numeric.approxQuantile(col_name, [0.5], 0.001)[0]
            median_values.append((col_name, median_val))

        # Convert the list of tuples to a dictionary
        median_dict = dict(median_values)

        return median_dict
    
    @staticmethod
    def mode(data: DataFrame):
        """
            Calculate the mode for each column in a PySpark DataFrame.

            :param data: The input DataFrame.
            :return: A dictionary containing the mode value for each column.
        """
        mode_values = {}
        for col_name in data.columns:
            mode_val = data.groupBy(col_name).count().orderBy(col("count").desc()).select(col_name).limit(1).collect()[0][0]
            mode_values[col_name] = mode_val

        return mode_values
    
    @staticmethod
    def variance(data: DataFrame):
        """
            Calculate the variance for each column in a PySpark DataFrame.

            :param data: The input DataFrame.
            :return: A dictionary containing the variance value for each column.
        """
        variance_values = {}
        for col_name in data.columns:
            variance_val = data.agg({col_name: 'variance'}).collect()[0][0]
            variance_values[col_name] = variance_val

        return variance_values
    
    @staticmethod
    def skew(data: DataFrame):
        """
            Calculate the skewness for each numeric column in a PySpark DataFrame.

            :param data: The input DataFrame.
            :return: A dictionary containing the skewness value for each numeric column.
        """
        skew_values = {}
        for col_name in data.columns:
            if isinstance(data.schema[col_name].dataType, NumericType):
                skew_val = data.agg(skewness(col_name)).collect()[0][0]
                skew_values[col_name] = skew_val

        return skew_values

    @staticmethod
    def kurtosis(data: DataFrame):
        """
        Calculate the kurtosis for each numeric column in a PySpark DataFrame.

        :param data: The input DataFrame.
        :return: A dictionary containing the kurtosis value for each column.
        """
        kurtosis_values = {}
        for col_name in data.columns:
            if isinstance(data.schema[col_name].dataType, NumericType):
                kurtosis_val = data.select(kurtosis(col_name)).na.drop().collect()[0][0]
                kurtosis_values[col_name] = kurtosis_val

        return kurtosis_values

    @staticmethod
    def columns(data: DataFrame):
        return data.columns

    @staticmethod
    def row_count(data: Union[DataFrame, None]) -> int:
        """
            Get the number of rows in the given PySpark DataFrame.

            :param data: Input PySpark DataFrame.
            :type data: pyspark.sql.DataFrame
            :return: Number of rows in the DataFrame.
            :rtype: int
        """
        if data is not None:
            return data.count()
        return 0

    @staticmethod
    def sample(data: DataFrame, num_rows: int = 10):
        """
            Get a random sample of a PySpark DataFrame.

            :param data: The input DataFrame.
            :param num_rows: The number of rows to return as a sample.
            :return: A tuple containing a list of columns and a list of lists with the sample data.
        """
        total_rows = data.count()
        if num_rows >= total_rows:
            return data.columns, [list(row.asDict().values()) for row in data.collect()]
        
        fraction = num_rows / total_rows
        sample_data = data.sample(False, fraction, seed=42)  # Randomly sample 'num_rows' rows
        columns = sample_data.columns
        sample_list = [list(row) for row in sample_data.collect()]

        return columns, sample_list
    
    @staticmethod
    def representative_sample(data: DataFrame, threshold: int = 100, desired_sample_size: float = 0.01):
        def compute_sample_size(population_size: int, std_dev: float, margin_error: float = 5, confidence_level: float = 95) -> int:
            """
                Compute the sample size for a given population size, margin error, confidence level, and standard deviation.
                
                :param population_size: The total number of items in the population.
                :type population_size: int
                :param margin_error: The acceptable margin of error for the sample mean as a percentage.
                :type margin_error: float
                :param confidence_level: The desired confidence level as a percentage.
                :type confidence_level: float
                :param std_dev: The standard deviation of the population. 
                :type std_dev: float
                :return: The required sample size.
                :rtype: int
            """

            # Lookup the z-score for the given confidence level
            z_score_dict = {80: 1.28, 85: 1.44, 90: 1.645, 95: 1.96, 99: 2.57, 99.5: 2.807, 99.9: 3.291}
            z_score = z_score_dict.get(confidence_level, 1.96)  # Default to 95% confidence level

            # Convert margin of error from percentage to decimal
            margin_error_decimal = margin_error / 100

            # Calculate the sample size
            sample_size = ceil((z_score*2 * std_dev) / (margin_error_decimal*2))

            # Adjust sample size for finite population
            if population_size is not None:
                sample_size = ceil((sample_size / (1 + ((sample_size - 1) / population_size))))

            return sample_size

        samples = {}
    
        for column in data.columns:
            # Check if the column data type is string (categorical)
            if str(data.schema[column].dataType) in ('StringType', 'BooleanType'):
                # Get the count of unique categories in the column
                num_categories = data.select(column).distinct().count()
                
                # If the number of categories is small enough, use the stratified sampling method
                if num_categories <= threshold:  # Set 'threshold' as appropriate for your use case
                    fractions = data.groupBy(column).count().rdd.collectAsMap()
                    total = sum(fractions.values())
                    fractions = {k: desired_sample_size / total for k, v in fractions.items()}
                    sample_data = data.sampleBy(column, fractions)
                else:
                    # If the number of categories is too large, use simple random sampling
                    sample_data = data.sample(False, desired_sample_size / data.count())
                
                samples[column] = sample_data.select(column).rdd.flatMap(lambda x: x).collect()
            else:
                data_col = data.select(column)
                population_size = data_col.count()
                std_dev = data_col.select(stddev(column)).first()[0]
                representative_size = compute_sample_size(population_size, std_dev)

                fraction = representative_size / population_size
                sample_data = data_col.sample(False, fraction, seed=42)  # Randomly sample a fraction of rows

                # Convert the sampled DataFrame to a list of values
                samples[column] = [row[column] for row in sample_data.collect()]

        return samples

    def correlation_p_values(self, data: DataFrame) -> dict:
        """
        Calculate the p-values of correlations between each pair of numerical columns in a PySpark DataFrame.

        Note: This method can be inefficient for dataframes with a large number of columns, as it requires iterating 
        over each pair of columns and calculating the correlation between them.

        :param data: A PySpark DataFrame.
        :type data: pyspark.sql.DataFrame
        :return: A dictionary with column pairs as keys and their corresponding correlation p-value as values.
        :rtype: dict
        """
        p_values = {}
        numerical_cols = [f.name for f in data.schema.fields if isinstance(f.dataType, NumericType)]
        
        for col1 in numerical_cols:
            p_values[col1] = {}
            for col2 in numerical_cols:
                n = data.select(col1, col2).na.drop().count()
                if n > 2:  # Need at least 3 data points to calculate correlation
                    r = data.stat.corr(col1, col2)
                    t_statistic = r * ((n-2)**0.5) / ((1 - r**2)**0.5)
                    p = 2 * (1 - t.cdf(abs(t_statistic), n-2))
                    p_values[col1][col2] = float(p) if not isnan(p) else None
                else:
                    p_values[col1][col2] = None

        return p_values

    @staticmethod
    def calculate_memory_usage(data: DataFrame):
        """
        Estimate the memory usage of a pandas DataFrame.

        :param df: The pandas DataFrame to estimate memory usage for.
        :type df: pd.DataFrame
        :return: A tuple containing total memory usage in bytes and memory usage by column in bytes.
        :rtype: Tuple[int, Dict[str, int]]
        """
        # Get the number of rows in the DataFrame
        num_rows = data.shape[0]

        # Calculate the memory usage in bytes
        total_memory_usage_bytes = data.memory_usage(deep=True).sum()

        # Calculate memory usage by column
        memory_usage_by_column = {}
        for column in data.columns:
            column_memory_usage = data[column].memory_usage(deep=True)
            memory_usage_by_column[column] = column_memory_usage

        return total_memory_usage_bytes, memory_usage_by_column
    

class NumPyEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np_integer):
            return int(obj)
        elif isinstance(obj, np_floating):
            return float(obj)
        elif isinstance(obj, np_ndarray):
            return obj.tolist()
        else:
            return super(NumPyEncoder, self).default(obj)

@Context
class DataQuality:
    """
        The DataQuality class is responsible for providing a unified interface for performing data quality checks 
        on pandas, PySpark, and Polars dataframes. 
        The actual checks are implemented by the strategy classes: PandasDataQuality, SparkDataQuality, and PolarsDataQuality.

        Attributes:
            data: A DataFrame that can be a pandas, PySpark, or Polars DataFrame.
    """

    def _init_(self, data):
        """
        Constructor of the DataQuality class.

        :param data: The DataFrame to be checked.
        :type data: pandas.DataFrame or pyspark.sql.DataFrame
        """
        self.data = data
        
        if isinstance(self.data, PandasDataFrame):
            self.set_strategy(PandasDataQuality())
        # elif isinstance(self.data, SparkDataFrame):
        #     self.set_strategy(SparkDataQuality())
        # elif isinstance(self.data, PolarsDataFrame):
        #     self.set_strategy(PolarsDataQuality())
        # Create a Jinja2 environment
        self.env = Environment(loader=BaseLoader)

    def check(self, *args, **kwargs):
        """
        Perform data quality checks on the DataFrame. 
        The actual checks are delegated to the strategy classes: PandasDataQuality, SparkDataQuality, PolarsDataQuality.

        :return: Results of the data quality checks.
        """
        # Setter for the strategy we are going to use.
        if isinstance(self.data, PandasDataFrame):
            self.set_strategy(PandasDataQuality())
        # elif isinstance(self.data, SparkDataFrame):
        #     self.set_strategy(SparkDataQuality())
        # elif isinstance(self.data, PolarsDataFrame):
        #     self.set_strategy(PolarsDataQuality())

        return self.strategy.concrete_check(data=self.data, *args, **kwargs)

    def generate_insights(self, profiling_results: dict) -> List[str]:
        insights = []
        total_rows = self.strategy.row_count(self.data)
        
        # Insight: Most common data type
        data_type_counts = Counter(profiling_results['data_type'].values())
        common_type, count = data_type_counts.most_common(1)[0]
        insights.append(f"<b><i>dataset:</i></b> The most common data type is '{common_type}' ({count} variables).")
        
        # Insight: Percentage of missing values
        total_missing = sum(profiling_results['missing_values'].values())
        missing_percent = total_missing / (total_rows * len(profiling_results['data_type'])) * 100
        if missing_percent > 10:
            insights.append(f"<b><i>dataset:</i></b> High number of missing values - {missing_percent:.2f}%.")
        
        # Insight: Variables with highest and lowest missing values
        variable_with_most_missing = max(profiling_results['missing_values'], key=profiling_results['missing_values'].get)
        variable_with_least_missing = min(profiling_results['missing_values'], key=profiling_results['missing_values'].get)
        insights.append(f"<b><i>dataset:</i></b> The variable '{variable_with_most_missing}' has the most missing values.")
        insights.append(f"<b><i>dataset:</i></b> The variable '{variable_with_least_missing}' has the least missing values.")
        
        # Insight: High cardinality variables
        high_cardinality_variables = [var for var, unique in profiling_results['uniqueness'].items() if unique > 0.8 * total_rows]
        if high_cardinality_variables:
            insights.append(f"<b><i>dataset:</i></b> High cardinality: {', '.join(high_cardinality_variables)}")
        
        # Insight: Duplicate rows
        duplicate_percent = profiling_results['duplicates'] / total_rows * 100
        if duplicate_percent > 10:
            insights.append(f"<b><i>dataset:</i></b> High number of duplicates - {duplicate_percent:.2f}%.")

        skewness_ = self.strategy.skew(self.data)
        kurtosis_ = self.strategy.kurtosis(self.data)
        variance_ = self.strategy.variance(self.data)

        # Insights: Skewness, Kurtosis, Variance, and Standard Deviation
        for variable, stats in profiling_results['statistical_summary'].items():
            if 'std' in stats and stats['std'] > 0:
                skewness = skewness_.get(variable)
                kurtosis = kurtosis_.get(variable)
                variance = variance_.get(variable)
                std_dev = stats['std']
                # insights.append(f"<b>{variable}:</b> The variable has a skewness of {skewness:.2f}, a kurtosis of {kurtosis:.2f}, a variance of {variance:.2f}, and a standard deviation of {std_dev:.2f}.")
                if skewness > 0.5 or skewness < -0.5:
                    insights.append(f"<b>{variable}:</b> The variable is skewed.")
                if kurtosis > 0.5 or kurtosis < -0.5:
                    insights.append(f"<b>{variable}:</b> High kurtosis.")
                if variance > std_dev:
                    insights.append(f"<b>{variable}:</b> Variance greater than standard deviation. ")
                
                # Check if variable follows a normal distribution
                if abs(skewness) < 0.5 and abs(kurtosis - 3) < 0.5:
                    insights.append(f"<b>{variable}:</b> Normal distribution.")
                else:
                    insights.append(f"<b>{variable}:</b> Not normal distribution.")
        
        # More insights can be added as needed
        return insights
    
    def _generate_overview_section(self, profiling_results: dict) -> str:
        # Define the HTML templates for Data Statistics and Data Insights
        data_statistics_template = """
        <div class="overview-section">
            <h3>Data Statistics</h3>
            <table class="data-statistics-table">
                <tr><td>Number of Variables</td><td>{{ num_variables }}</td></tr>
                <tr><td>Number of Rows</td><td>{{ num_rows }}</td></tr>
                <tr><td>Missing Cells</td><td>{{ missing_cells }}</td></tr>
                <tr><td>Missing Cells (%)</td><td>{{ missing_cells_pct }}</td></tr>
                <tr><td>Duplicate Rows</td><td>{{ duplicate_rows }}</td></tr>
                <tr><td>Duplicate Rows (%)</td><td>{{ duplicate_rows_pct }}</td></tr>
                <tr><td>Total Size in Memory</td><td>{{ size_memory }}</td></tr>
                <tr><td>Average Row Size in Memory</td><td>{{ avg_row_memory }}</td></tr>
                <tr><td>Variable Types</td><td>
                    <table class="variable-types-table">
                    {% for type, count in variable_types.items() %}
                        <tr><td>{{ type }}</td><td>{{ count }}</td></tr>
                    {% endfor %}
                    </table>
                </td></tr>
            </table>
        </div>
        """

        data_insights_template = """
        <div class="overview-section">
            <h3>Data Insights</h3>
            <div class="data-insights-table-wrapper">
                <table class="data-insights-table">
                {% for insight in insights %}
                    <tr><td>{{ insight }}</td></tr>
                {% endfor %}
                </table>
            </div>
        </div>
        """

        # Define a CSS template for styling
        css_template = """
        <style>
        body {
            font-family: Arial, sans-serif;
        }
        h1, h2 {
            color: #333;
        }
        .overview-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        .overview-section {
            display: flex;
            flex-direction: column;
            align-items: start;
        }
        .data-statistics-table,
        .variable-types-table {
            width: 100%;
            border-collapse: collapse;
        }
        .data-statistics-table td,
        .variable-types-table td {
            border: 1px solid #ccc;
            padding: 10px;
        }
        .data-statistics-table tr td:first-child,
        .variable-types-table tr td:first-child {
            font-weight: bold;
        }
        .variable-types-table {
            border: 0;
        }
        .variable-types-table td {
            border: 0;
        }
        .data-insights-table-wrapper {
            width: 100%;
            max-height: 400px; /* You may adjust this value as needed */
            overflow-y: auto;
        }
        .data-insights-table {
            width: 100%;
            border-collapse: collapse;
        }
        .data-insights-table td {
            border: 1px solid #ccc;
            padding: 10px;
        }

        .overview_title {
            padding-left: 2.5%;
        }
        </style>
        """

        # Call the functions to get the total memory usage and memory usage by column
        total_memory_usage_bytes, memory_usage_by_column = self.strategy.calculate_memory_usage(self.data)

        # Calculate the average row memory usage
        avg_row_memory = total_memory_usage_bytes / profiling_results['statistical_summary']['sepal_length']['count']

        # Calculate the percentage of missing cells
        total_cells = profiling_results['statistical_summary']['sepal_length']['count'] * len(profiling_results['data_type'])
        missing_cells_pct = sum(profiling_results['missing_values'].values()) / total_cells * 100

        # Calculate the percentage of duplicate rows
        duplicate_rows_pct = profiling_results['duplicates'] / profiling_results['statistical_summary']['sepal_length']['count'] * 100

        # Count the number of columns for each data type
        variable_types_count = Counter(profiling_results['data_type'].values())

        # Render the Data Statistics template
        data_statistics_html = self.env.from_string(data_statistics_template).render(
            num_variables=len(profiling_results['data_type']),
            num_rows=profiling_results['statistical_summary']['sepal_length']['count'],  # Assuming all columns have the same number of rows
            missing_cells=sum(profiling_results['missing_values'].values()),
            missing_cells_pct=missing_cells_pct,
            duplicate_rows=profiling_results['duplicates'],
            duplicate_rows_pct=duplicate_rows_pct,
            size_memory=self._format_bytes(total_memory_usage_bytes),
            avg_row_memory=self._format_bytes(avg_row_memory),
            variable_types=variable_types_count
        )

        # Render the Data Insights template
        data_insights_html = self.env.from_string(data_insights_template).render(
            insights=self.generate_insights(profiling_results=profiling_results)
        )

        # Combine the two sections into an overview grid
        overview_html = f"""
        <h2 class="overview_title">Overview</h2>
        <div class="overview-grid">
            {data_statistics_html}
            {data_insights_html}
        </div>
        """

        return css_template + overview_html

    def _generate_variables_section(self, profiling_results: dict) -> str:
        selected_variable = self.strategy.columns(self.data)[0]
        histograms = self.histograms_to_json(profiling_results.get("distribution"))

        # Add additional attributes to the profiling_results
        profiling_results['memory_usage'] = self.strategy.calculate_memory_usage(self.data)[1] if self.strategy.calculate_memory_usage(self.data) else "Undefined"
        profiling_results['median'] = self.strategy.median(self.data) if self.strategy.median(self.data) else "Undefined"
        profiling_results['mode'] = self.strategy.mode(self.data) if self.strategy.mode(self.data) else "Undefined"
        profiling_results['variance'] = self.strategy.variance(self.data) if self.strategy.variance(self.data) else "Undefined"
        profiling_results['skew'] = self.strategy.skew(self.data) if self.strategy.skew(self.data) else "Undefined"
        profiling_results['kurtosis'] = self.strategy.kurtosis(self.data) if self.strategy.kurtosis(self.data) else "Undefined"
        profiling_results['row_count'] = self.strategy.row_count(self.data) if self.strategy.row_count(self.data) else "Undefined"

        css_template = """
        <style>
        body {
            font-family: Arial, sans-serif;
        }
        h2, h3 {
            color: #333;
        }
        .overview-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        .overview-section {
            display: flex;
            flex-direction: column;
            align-items: start;
        }
        .variable-section-table {
            width: 100%;
            border-collapse: collapse;
        }
        .variable-section-table td {
            border: 1px solid #ccc;
            padding: 10px;
        }
        .variable-section-table tr td:first-child {
            font-weight: bold;
        }

        .overview-section {
            width: 95%;
            padding-left: 2.5%
        }
        .overview-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            width: 95%;
            padding-left: 2.5%
        }

        .overview-section, .overview-grid {
            width: 95%;  /* This sets the width of the div to 95% of the parent div's width */
            margin: 0 auto;  /* This centers the div */
            padding: 0 2.5%;  /* This sets the left and right padding to 2.5% of the div's width */
        }

        .title-and-dropdown {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            width: 100%;
        }

        #dataInfo {
            width: 100%;
        }
        </style>
        """

        variables_template = """
        <div class="overview-header">
            <h2 style="margin-bottom: 0;">Variables</h2>
            <select id="variablesDropdown" onchange="updateVariableInfo()">
                {% for variable in variables %}
                <option value="{{ variable }}">{{ variable }}</option>
                {% endfor %}
            </select>
        </div>

        <div class="overview-grid">
            <div class="overview-section">
                <div class="title-and-dropdown">
                    <h3>Data Info</h3>
                    <select id="dataInfoDropdown" onchange="updateDataInfo()">
                        <option value="info">Info</option>
                        <option value="statistics">Statistics</option>
                        <option value="sample">Sample</option>
                    </select>
                </div>

                <div id="dataInfo">
                    <table class="variable-section-table">
                        <tr><td>Distinct Count</td><td id="distinctCount"></td></tr>
                        <tr><td>Uniques %</td><td id="uniquePercentage"></td></tr>
                        <tr><td>Missing %</td><td id="missingPercentage"></td></tr>
                        <tr><td>Memory Size</td><td id="memorySize"></td></tr>
                    </table>
                </div>

                <div id="dataStatistics" style="display: none;">
                    <table class="variable-section-table">
                        <tr><td>Mean</td><td id="mean"></td></tr>
                        <tr><td>Median</td><td id="median"></td></tr>
                        <tr><td>Mode</td><td id="mode"></td></tr>
                        <tr><td>Standard Deviation</td><td id="stdDev"></td></tr>
                        <tr><td>Variance</td><td id="variance"></td></tr>
                        <tr><td>Min</td><td id="min"></td></tr>
                        <tr><td>Max</td><td id="max"></td></tr>
                        <tr><td>Skew</td><td id="skew"></td></tr>
                        <tr><td>Kurtosis</td><td id="kurtosis"></td></tr>
                    </table>
                </div>

                <div id="dataSample" style="display: none;">
                    <table class="variable-section-table" id="sampleTable">
                    </table>
                </div>
            </div>

            <div class="overview-section">
                <div style="flex-grow: 1;"></div>
                <div class="title-and-dropdown">
                    <h3>Graphs</h3>
                    <select id="graphsDropdown" onchange="updateGraph()">
                        <option value="histogram">Histogram</option>
                        <option value="boxWhiskers">Box and Whiskers</option>
                        <option value="violin">Violin</option>
                        <option value="textCloud">Text Cloud</option>
                    </select>
                </div>
                <div id="graph"></div>
            </div>
        </div>
        """

        variables_html = self.env.from_string(variables_template).render(variables=profiling_results['data_type'].keys())
        
        # Assuming self.data is the data you're working with
        representative_sample_data = self.strategy.representative_sample(self.data)

        variables_js = f"""
        <script>
        var representative_sample = {dumps(representative_sample_data)};
        var profiling_results = {dumps(profiling_results, cls=NumPyEncoder)};
        var histograms = {dumps(histograms)};

        function updateVariableInfo() {{
            var variable = document.getElementById('variablesDropdown').value;
            var distinctCount = document.getElementById('distinctCount');
            var uniquePercentage = document.getElementById('uniquePercentage');
            var missingPercentage = document.getElementById('missingPercentage');
            var memorySize = document.getElementById('memorySize');

            distinctCount.innerHTML = profiling_results['uniqueness'][variable];
            uniquePercentage.innerHTML = profiling_results['uniqueness'][variable] / profiling_results['row_count'];
            missingPercentage.innerHTML = profiling_results['missing_values'][variable] / profiling_results['row_count'];
            memorySize.innerHTML = profiling_results['memory_usage'][variable];

            var mean = document.getElementById('mean');
            var median = document.getElementById('median');
            var mode = document.getElementById('mode');
            var stdDev = document.getElementById('stdDev');
            var variance = document.getElementById('variance');
            var min = document.getElementById('min');
            var max = document.getElementById('max');
            var skew = document.getElementById('skew');
            var kurtosis = document.getElementById('kurtosis');

            try {{
                mean.innerHTML = profiling_results['statistical_summary'][variable]['mean'];
                stdDev.innerHTML = profiling_results['statistical_summary'][variable]['std'];
                min.innerHTML = profiling_results['statistical_summary'][variable]['min'];
                max.innerHTML = profiling_results['statistical_summary'][variable]['max'];
            }} catch {{
                mean.innerHTML = 'Undefined';
                stdDev.innerHTML = 'Undefined';
                min.innerHTML = 'Undefined';
                max.innerHTML = 'Undefined';
            }}
            median.innerHTML = profiling_results['median'][variable];
            mode.innerHTML = profiling_results['mode'][variable];
            variance.innerHTML = profiling_results['variance'][variable];
            skew.innerHTML = profiling_results['skew'][variable];
            kurtosis.innerHTML = profiling_results['kurtosis'][variable];

            updateGraphOptions();
            updateGraph();
        }}

        function updateDataInfo() {{
            var selection = document.getElementById('dataInfoDropdown').value;
            var dataInfo = document.getElementById('dataInfo');
            var dataStatistics = document.getElementById('dataStatistics');
            var dataSample = document.getElementById('dataSample');

            if (selection == 'info') {{
                dataInfo.style.display = 'block';
                dataStatistics.style.display = 'none';
                dataSample.style.display = 'none';
            }} else if (selection == 'statistics') {{
                dataInfo.style.display = 'none';
                dataStatistics.style.display = 'block';
                dataSample.style.display = 'none';
            }} else if (selection == 'sample') {{
                dataInfo.style.display = 'none';
                dataStatistics.style.display = 'none';
                dataSample.style.display = 'block';
            }}
        }}
        
        function updateGraph() {{
            var variable = document.getElementById('variablesDropdown').value;
            var graphType = document.getElementById('graphsDropdown').value;
            var graphDiv = document.getElementById('graph');
            var isNumeric = typeof histograms[variable] !== 'undefined';

            // Clear the graph div before drawing a new plot
            graphDiv.innerHTML = '';

            if (graphType == 'histogram' && isNumeric) {{
                var plotData = histograms[variable]['data'];
                var layout = histograms[variable]['layout'];
                Plotly.newPlot(graphDiv, plotData, layout);
            }} else if (graphType == 'boxWhiskers' && isNumeric) {{
                var sampleData = representative_sample[variable];
                var trace = {{
                    x: sampleData,
                    type: 'box'
                }};
                var layout = {{ title: variable + ' Box and Whiskers' }};
                Plotly.newPlot(graphDiv, [trace], layout);
            }} else if (graphType == 'violin' && isNumeric) {{
                var sampleData = representative_sample[variable];
                var trace = {{
                    y: sampleData,
                    type: 'violin'
                }};
                var layout = {{ title: variable + ' Violin Plot' }};
                Plotly.newPlot(graphDiv, [trace], layout);
            }} else if (graphType == 'textCloud') {{
                var sampleData = representative_sample[variable].map(function(d) {{
                    return {{text: d[0], size: d[1]}};
                }});
                drawWordCloud();
            }}
        }}

        function convertToWordCloudFormat(words) {{
            var wordCounts = {{}};
            for (var i = 0; i < words.length; i++) {{
                wordCounts[words[i]] = (wordCounts[words[i]] || 0) + 1;
            }}
            var wordCloudFormat = [];
            for (var word in wordCounts) {{
                wordCloudFormat.push([word, wordCounts[word]]);
            }}
            return wordCloudFormat;
        }}

        function drawWordCloud() {{
            var variable = document.getElementById('variablesDropdown').value;
            var sampleData = representative_sample[variable];

            var wordCounts = {{}};
            for (var i = 0; i < sampleData.length; i++) {{
                wordCounts[sampleData[i]] = (wordCounts[sampleData[i]] || 0) + 1;
            }}

            var words = [];
            var scaleFactor = 5;  // Adjust this value to get the desired font sizes
            for (var word in wordCounts) {{
                words.push({{text: word, size: wordCounts[word] + scaleFactor}});
            }}

            var layout = d3.layout.cloud()
                .size([800, 600])
                .words(words)
                .padding(5)
                .rotate(function() {{ return ~~(Math.random() * 2) * 90; }})
                .font('Impact')
                .fontSize(function(d) {{ return d.size; }})
                .on('end', draw);

            layout.start();

            function draw(words) {{
                d3.select('#graph').html('');  // Clear the canvas
                d3.select('#graph').append('svg')
                    .attr('width', layout.size()[0])
                    .attr('height', layout.size()[1])
                    .append('g')
                    .attr('transform', 'translate(' + layout.size()[0] / 2 + ',' + layout.size()[1] / 2 + ')')
                    .selectAll('text')
                    .data(words)
                    .enter().append('text')
                    .style('fill', 'black')
                    .style('font-size', function(d) {{ return d.size + 'px'; }})
                    .style('font-family', 'Impact')
                    .attr('text-anchor', 'middle')
                    .attr('transform', function(d) {{
                        return 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')';
                    }})
                    .text(function(d) {{ return d.text; }});
            }}
        }}

        function updateGraphOptions() {{
            var variable = document.getElementById('variablesDropdown').value;
            var graphDropdown = document.getElementById('graphsDropdown');
            var dataType = profiling_results['data_type'][variable];

            // Clear the dropdown
            graphDropdown.innerHTML = '';

            // Check if the variable is numeric or non-numeric based on data type
            if (['Number', 'Bigint', 'int', 'Float'].includes(dataType)) {{
                // Add the options for numeric variables
                graphDropdown.innerHTML += '<option value="histogram">Histogram</option>';
                graphDropdown.innerHTML += '<option value="boxWhiskers">Box and Whiskers</option>';
                graphDropdown.innerHTML += '<option value="violin">Violin</option>';
            }} else {{
                // Add the options for non-numeric variables
                graphDropdown.innerHTML += '<option value="histogram">Histogram</option>';
                graphDropdown.innerHTML += '<option value="textCloud">Text Cloud</option>';
            }}
        }}

        
        updateVariableInfo();
        updateDataInfo();
        updateGraphOptions()
        updateGraph();
        </script>
        """

        final_html = css_template + variables_html + variables_js
        return final_html

    def _generate_interactions_section(self, profiling_results: dict) -> str:
        # Get a representative sample
        representative_sample = self.strategy.representative_sample(self.data)

        # Create the dropdown options outside of the f-string
        dropdown_options = "\n".join(f'<option value="{var}">{var}</option>' for var in representative_sample.keys())

        interactions_template = f"""
        <div class="interactions-section">
            <div class="interactions-header">
                <h2>Interactions</h2>
                <div class="dropdowns">
                    <select id="independentVarDropdown" onchange="updateScatterPlot()">
                        {dropdown_options}
                    </select>
                    <select id="dependentVarDropdown" onchange="updateScatterPlot()">
                        {dropdown_options}
                    </select>
                </div>
            </div>
            <div id="scatterPlotDiv"></div>
        </div>

        <script>
        var representative_sample = {dumps(representative_sample, cls=NumPyEncoder)};
        var profiling_results = {dumps(profiling_results, cls=NumPyEncoder)};

        function updateScatterPlot() {{
            var independentVar = document.getElementById('independentVarDropdown').value;
            var dependentVar = document.getElementById('dependentVarDropdown').value;
            var xData = representative_sample[independentVar];
            var yData = representative_sample[dependentVar];

            var trace = {{
                x: xData,
                y: yData,
                mode: 'markers',
                type: 'scatter'
            }};

            var layout = {{
                title: dependentVar + ' vs ' + independentVar,
                xaxis: {{
                    title: independentVar
                }},
                yaxis: {{
                    title: dependentVar
                }},
                showlegend: false,
                autosize: false,
                width: document.getElementById('scatterPlotDiv').offsetWidth,
                height: 600
            }};

            Plotly.newPlot('scatterPlotDiv', [trace], layout);
        }}

        updateScatterPlot();
        </script>
        """

        # Define a CSS template for styling
        css_template = """
        <style>
        body {
            font-family: Arial, sans-serif;
        }
        h1, h2 {
            color: #333;
        }
        .interactions-section {
            width: 95%;
            padding-left: 2.5%
        }
        .interactions-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .dropdowns {
            display: flex;
            gap: 10px;
        }
        .interactions-controls {
            display: flex;
            gap: 20px;
            align-items: center;
        }
        #scatterPlotDiv {
            width: 100%;
            height: 600px;
        }
        </style>
        """

        # Render the Interactions section template
        interactions_html = self.env.from_string(interactions_template).render(
            representative_sample=dumps(representative_sample, cls=NumPyEncoder),
            profiling_results=dumps(profiling_results, cls=NumPyEncoder),
            variables=list(representative_sample.keys())
        )

        return css_template + interactions_html

    def _generate_correlations_section(self, profiling_results: dict) -> str:
        correlations = profiling_results['correlation']
        p_values = self.strategy.correlation_p_values(self.data)

        # Create the dropdown options outside of the f-string
        dropdown_options = "\n".join(f'<option value="{var}">{var}</option>' for var in correlations.keys())

        correlations_template = f"""
        <div class="correlations-section">
            <div class="correlations-toolbar">
                <div style="flex-grow: 1;"><h2 style="margin-bottom: 0;">Correlations</h2></div>
                <div>
                    <select id="correlationTypeDropdown" onchange="updateHeatmap()">
                        <option value="correlation">Correlation Coefficients</option>
                        <option value="p-value">P-values</option>
                    </select>
                </div>
            </div>
            <div id="correlationHeatmapDiv"></div>
        </div>

        <style>
        #correlationHeatmapDiv {{
            display: flex;
            justify-content: center;
        }}

        .correlations-section {{
            width: 95%;
            padding-left: 2.5%;
        }}

        .correlations-toolbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }}
        </style>

        <script>
        var correlations = {dumps(correlations, cls=NumPyEncoder)};
        var p_values = {dumps(p_values, cls=NumPyEncoder)};
        var variables = {dumps(list(correlations.keys()), cls=NumPyEncoder)};

        function updateHeatmap() {{
            var type = document.getElementById('correlationTypeDropdown').value;
            var z_data = (type == 'correlation') ? correlations : p_values;
            var colorscale = (type == 'correlation') ? 'Viridis' : 'Reds';
            var title = (type == 'correlation') ? 'Correlation Coefficients' : 'P-values';

            var data = [{{
                x: variables,
                y: variables,
                z: Object.values(z_data).map(row => Object.values(row)),
                type: 'heatmap',
                colorscale: colorscale,
                showscale: true,
            }}];

            var layout = {{
                title: title,
                xaxis: {{
                    side: 'top'
                }},
                width: 600,
                height: 600,
            }};

            Plotly.newPlot('correlationHeatmapDiv', data, layout);
        }}

        updateHeatmap();
        </script>
        """

        return correlations_template

    def _generate_missing_values_section(self, profiling_results: dict) -> str:
        total_rows = self.strategy.row_count(self.data)

        # Create the Missing Values section template
        missing_values_template = f"""
        <div class="missing-values-section">
            <div class="missing-toolbar">
                <h2>Missing Values</h2>
            </div>
            <div id="missingValuesBarDiv"></div>
        </div>

        <style>
            .missing-values-section {{
                width: 95%;
                padding-left: 2.5%;
            }}

            .missing-values-toolbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }}
        </style>
        
        <script>
        var total_rows = {total_rows};
        var profiling_results = {dumps(profiling_results, cls=NumPyEncoder)};

        function updateMissingValuesBarPlot() {{
            var missing_values = profiling_results['missing_values'];
            var variables = Object.keys(missing_values);
            var missing_counts = Object.values(missing_values);
            var not_missing_counts = variables.map(function(variable) {{
                return total_rows - missing_values[variable];
            }});
            var missing_percents = missing_counts.map(function(count) {{
                return count / total_rows * 100;
            }});
            var not_missing_percents = not_missing_counts.map(function(count) {{
                return count / total_rows * 100;
            }});

            var trace1 = {{
                x: variables,
                y: not_missing_percents,
                name: 'Not Missing',
                type: 'bar',
                marker: {{
                    color: 'blue'
                }}
            }};

            var trace2 = {{
                x: variables,
                y: missing_percents,
                name: 'Missing',
                type: 'bar',
                marker: {{
                    color: 'orange'
                }}
            }};

            var layout = {{
                title: 'Missing Values by Variable',
                barmode: 'stack',
                yaxis: {{
                    title: '% of Total Values'
                }}
            }};

            Plotly.newPlot('missingValuesBarDiv', [trace1, trace2], layout);
        }}

        updateMissingValuesBarPlot();
        </script>
        """

        return missing_values_template

    @staticmethod
    def _format_bytes(size: int) -> str:
        """Format a size in bytes into a human-readable format."""
        # Adapted from https://stackoverflow.com/a/1094933/1064325
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
            if abs(size) < 1024.0:
                return f"{size:3.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} YB"

    def histograms_to_json(self, distribution: dict) -> dict:
        plots_dict = {}

        for column, data in distribution.items():
            if 'bin_edges' in data:
                # Numeric variable (histogram)
                filtered_bins = [(data['histogram'][i], (data['bin_edges'][i], data['bin_edges'][i + 1])) for i in range(len(data['histogram'])) if data['histogram'][i] != 0]
                counts, bin_edges = zip(*filtered_bins)
                bin_centers = [(edges[0] + edges[1]) / 2 for edges in bin_edges]

                # Determine the bar width based on the range of bin centers
                bar_width = min([bin_centers[i + 1] - bin_centers[i] for i in range(len(bin_centers) - 1)]) * 0.8

                # Create histogram
                trace = {
                    "type": "bar",
                    "x": bin_centers,
                    "y": counts,
                    "name": column,
                    "marker": {
                        "color": 'rgba(100, 149, 237, 0.6)',  # CornflowerBlue
                        "line": {"width": 1, "color": 'DarkSlateGrey'}
                    },
                    "hoverinfo": 'x+y',
                    "hoverlabel": {"namelength": -1},
                    "width": bar_width  # Setting the bar width
                }
            else:
                # Categorical variable (bar chart)
                categories, counts = unique(data, return_counts=True)
                trace = {
                    "type": "bar",
                    "x": categories if isinstance(categories, list) else list(categories[0].keys()),
                    "y": counts if isinstance(counts, list) else list(categories[0].values()),
                    "name": column,
                }

            layout = {
                "height": 400,
                "width": 900,
                "title": {"text": f"Distribution of {column}", "x": 0.5},
                "xaxis_title": column,  # X-axis label
                "yaxis_title": 'Count',  # Y-axis label
                "autosize": True,
                "hovermode": 'x',
                "bargap": 0.05,  # Reduced gap between bars
                "showlegend": False  # Hide the legend
            }

            plots_dict[column] = {
                "data": [trace],
                "layout": layout
            }

        return plots_dict

    def report(self, filename=None, title: str = "Exploratory Data Analysis"):
        ### Profile the data
        profiler_results = self.check()

        separator_section = '<hr style="width:95%;text-align:center;margin-left:2.5%; margin-top: 40px;">'

        ### Generate the sections of the report
        # Generate Overview
        overview_section = self._generate_overview_section(profiling_results=profiler_results)
        variables_section = self._generate_variables_section(profiling_results=profiler_results)
        interactions_section = self._generate_interactions_section(profiling_results=profiler_results)
        correlations_section = self._generate_correlations_section(profiling_results=profiler_results)
        missing_section = self._generate_missing_values_section(profiling_results=profiler_results)

        # TODO: Generate the other sections...

        # Combine all the sections into the final report, including the Plotly script tag
        report_html = separator_section.join([overview_section, variables_section, interactions_section, correlations_section, missing_section]) # + other sections...
        # report_html = missing_section # + other sections...

        ### Include the Plotly script tag
        report_html = f"""
        <!DOCTYPE html>
        <html lang='en'>
            <head>
                <meta charset='UTF-8'>
                <meta name='viewport' content='width=device-width, initial-scale=1.0'>
                <title>{title}</title>
                <script src='https://cdn.plot.ly/plotly-latest.min.js'></script>
                <script src='https://d3js.org/d3.v5.min.js'></script>
                <script src='https://cdnjs.cloudflare.com/ajax/libs/d3-cloud/1.2.5/d3.layout.cloud.min.js'></script>

                <style>
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                td {{
                    border: 1px solid #ccc;
                    padding: 10px;
                }}
                tr td:first-child {{
                    font-weight: bold;
                }}
                </style>

            </head>
            <body>
                {report_html}
            </body>
        </html>
        """

        # If a filename is provided, write the report to an HTML file
        if filename:
            with open(filename, 'w') as f:
                f.write(report_html)

        # Otherwise, display the report in the notebook
        else:
            from IPython.core.display import display, HTML
            display(HTML(report_html))