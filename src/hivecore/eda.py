"""
    The hivecore.eda module includes multiple functions to make data exploration quick and insightful.
"""

import pandas as pd
from collections import defaultdict
from typing import Tuple, Union
from functools import reduce

def confusion_matrix(y_true: list[int], y_pred: list[int], include_precision: bool = False, include_recall: bool = False) -> Union[pd.DataFrame, Tuple[pd.DataFrame, float], Tuple[pd.DataFrame, float, float]]:
    """
    Compute the confusion matrix between y_true and y_pred.
    
    :param y_true: The true labels. Must be a list of integers, where 1 represents positive and 0 represents negative.
    :type y_true: list[int]

    :param y_pred: The predicted labels. Must be a list of integers, where 1 represents positive and 0 represents negative.
    :type y_pred: list[int]

    :param include_precision: If True, also compute and return the precision. Defaults to False.
    :type include_precision: bool

    :param include_recall: If True, also compute and return the recall. Defaults to False.
    :type include_recall: bool

    :return: The confusion matrix as a DataFrame, and optionally the precision and/or recall as a float(s).
    :rtype: Union[pandas.DataFrame, Tuple[pandas.DataFrame, float], Tuple[pandas.DataFrame, float, float]]
    """
    if len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred must have the same length.")
        
    d = defaultdict(lambda: defaultdict(int))
    
    # Loop through the true and predicted labels, and increment the counts in the defaultdict d.
    reduce(lambda acc, xy: (acc[0]+1, d[xy[0]].__setitem__(xy[1], d[xy[0]][xy[1]]+1)), zip(y_true, y_pred), (0, None))
        
    # Compute the TP, FP, FN, and TN from the counts in d.
    TP = d[1][1] if 1 in d and 1 in d[1] else 0
    FP = d[0][1] if 0 in d and 1 in d[0] else 0
    FN = d[1][0] if 1 in d and 0 in d[1] else 0
    TN = d[0][0] if 0 in d and 0 in d[0] else 0
    
    # Create the confusion matrix DataFrame.
    confusion_df = pd.DataFrame({'Positive': [TP, FP], 'Negative': [FN, TN]}, index=['True', 'False'])

    # Compute and return the precision and/or recall, if requested.
    if include_precision and not include_recall:
        precision = TP / (TP + FP) if TP + FP > 0 else 0
        return confusion_df, precision
    elif include_recall and not include_precision:
        recall = TP / (TP + FN) if TP + FN > 0 else 0
        return confusion_df, recall
    elif include_precision and include_recall:
        precision = TP / (TP + FP) if TP + FP > 0 else 0
        recall = TP / (TP + FN) if TP + FN > 0 else 0
        return confusion_df, precision, recall
    else:
        return confusion_df
