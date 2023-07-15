Confusion Matrix
================

.. autofunction:: hivecore.visual.visualize_confusion_matrix
   :noindex:

Example
^^^^^^^
..  code-block:: python
    
    from hivecore.eda import confusion_matrix
    from hivecore.visual import visualize_confusion_matrix

    y_true = [1, 1, 0, 1, 0, 1, 0, 0]
    y_pred = [1, 0, 0, 1, 1, 1, 0, 0]

    matrix = confusion_matrix(y_true, y_pred)

    visualize_confusion_matrix(matrix, cmap='binary')