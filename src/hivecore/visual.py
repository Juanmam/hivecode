"""
    The hivecore.visual module includes multiple functions that complement the already existing EDA functions by improving visualization on the data displayed.
"""

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def visualize_confusion_matrix(df: pd.DataFrame,
                          group_names: list[str] = None,
                          categories: list[str] = 'auto',
                          count: bool = True,
                          percent: bool = True,
                          cbar: bool = True,
                          xyticks: bool = True,
                          xyplotlabels: bool = True,
                          sum_stats: bool = True,
                          figsize: tuple[float, float] = None,
                          cmap: str = 'Blues',
                          title: str = None) -> None:
    '''
    This function will make a nicer visual out of the standard confusion matrix.

    :param cf: Confusion matrix to be passed in.
    :type cf: pandas.DataFrame
    
    :param group_names: List of strings that represent the labels row by row to be shown in each square. Default is None.
    :type group_names: list[str], optional
    
    :param categories: List of strings containing the categories to be displayed on the x,y axis. Default is 'auto'.
    :type categories: list[str], optional
    
    :param count: If True, show the raw number in the confusion matrix. Default is True.
    :type count: bool, optional
    
    :param percent: If True, show the proportions for each category. Default is True.
    :type percent: bool, optional
    
    :param cbar: If True, show the color bar. The cbar values are based off the values in the confusion matrix. Default is True.
    :type cbar: bool, optional
    
    :param xyticks: If True, show x and y ticks. Default is True.
    :type xyticks: bool, optional
    
    :param xyplotlabels: If True, show 'True Label' and 'Predicted Label' on the figure. Default is True.
    :type xyplotlabels: bool, optional
    
    :param sum_stats: If True, display summary statistics below the figure. Default is True.
    :type sum_stats: bool, optional
    
    :param figsize: Tuple representing the figure size. Default will be the matplotlib rcParams value.
    :type figsize: tuple[float, float], optional
    
    :param cmap: Colormap of the values displayed from matplotlib.pyplot.cm. Default is 'Blues'.
    :type cmap: str, optional
    
    :param title: Title for the heatmap. Default is None.
    :type title: str, optional
    '''

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pd.DataFrame but got {type(df)} instead.")

    if group_names and len(group_names) != df.size:
        raise ValueError(f"Length of group_names should be equal to the size of the confusion matrix.")

    if not isinstance(categories, list) and categories != 'auto':
        raise TypeError(f"Expected list or 'auto' but got {type(categories)} instead.")

    if not isinstance(count, bool):
        raise TypeError(f"Expected bool for count but got {type(count)} instead.")

    if not isinstance(percent, bool):
        raise TypeError(f"Expected bool for percent but got {type(percent)} instead.")

    if not isinstance(cbar, bool):
        raise TypeError(f"Expected bool for cbar but got {type(cbar)} instead.")

    if not isinstance(xyticks, bool):
        raise TypeError(f"Expected bool for xyticks but got {type(xyticks)} instead.")

    if not isinstance(xyplotlabels, bool):
        raise TypeError("Parameter 'xyplotlabels' must be a boolean.")

    # Turn df into matrix
    df = np.array(df)

    # Prepare the group labels based on the given group names
    blanks = ['' for i in range(df.size)]
    if group_names and len(group_names) == df.size:
        group_labels = ["{}\n".format(value) for value in group_names]
    else:
        group_labels = ["{}\n".format(value) for value in ['True Neg','False Pos','False Neg','True Pos']]

    # Prepare the group counts based on the given count flag
    if count:
        group_counts = ["{0:0.0f}\n".format(value) for value in df.flatten()]
    else:
        group_counts = blanks

    # Prepare the group percentages based on the given percent flag
    if percent:
        group_percentages = ["{0:.2%}".format(value) for value in df.flatten() / np.sum(df)]
    else:
        group_percentages = blanks

    # Combine the group labels, counts, and percentages into box labels
    box_labels = [f"{v1}{v2}{v3}".strip() for v1, v2, v3 in zip(group_labels, group_counts, group_percentages)]
    box_labels = np.asarray(box_labels).reshape(df.shape[0], df.shape[1])

    # Calculate and format the summary statistics if requested
    if sum_stats:
        accuracy = np.trace(df) / float(np.sum(df))
        if len(df) == 2:
            precision = df[1, 1] / sum(df[:, 1])
            recall = df[1, 1] / sum(df[1, :])
            f1_score = 2 * precision * recall / (precision + recall)
            stats_text = "\n\nAccuracy={:0.3f}\nPrecision={:0.3f}\nRecall={:0.3f}\nF1 Score={:0.3f}".format(
                accuracy, precision, recall, f1_score)
        else:
            stats_text = "\n\nAccuracy={:0.3f}".format(accuracy)
    else:
        stats_text = ""

    # Set figure parameters according to other arguments
    if figsize is None:
        # Get default figure size if not set
        figsize = plt.rcParams.get('figure.figsize')

    if not xyticks:
        # Do not show categories if xyticks is False
        categories = False
 
    if categories == 'auto':
        # Define categories as a boolean variable instead of a numeric one.
        categories = ['True', 'False']
        
    # Make the heatmap visualization
    plt.figure(figsize=figsize)
    sns.heatmap(df, annot=box_labels, fmt="", cmap=cmap, cbar=cbar, xticklabels=categories, yticklabels=categories)

    # Add plot labels and title
    if xyplotlabels:
        plt.ylabel('True label')
        plt.xlabel('Predicted label' + stats_text)
    else:
        plt.xlabel(stats_text)

    if title:
        plt.title(title)