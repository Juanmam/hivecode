Write CSV
========

.. role:: method
.. role:: param

hivecode.hiveadb.io. :method:`write_csv` (:param:`df, file_name: str, path: str, sep: str, nas: str, header: bool, mode: str, num_files: int`)

    Writes a CSV file from a DataFrame into the file system.

Example
^^^^^^^
..  code-block:: python

    df = write_csv('titanic.csv', '/mnt/raw-zone/')

Example
^^^^^^^
..  code-block:: python

    df = write_csv('diabetes.csv', '/mnt/silver-zone/', as_type="spark")

