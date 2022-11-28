Normal Area Under Curve Analysis (NAUCA)
========================================

.. role:: method
.. role:: param

hivesignal.analysis. :method:`nauca` ( :param:`df, NORMAL_FREQ_MIN: float, NORMAL_FREQ_MAX: float, amplitud_col: str, frequency_col: str, alpha: float, method: str, return_mode: str`)

Performs an analysis given a dataframe with an "amplitude" and a "frequency" column. The analysis takes into account an interval of normal frequencies and returns a relation between the AUC of the abnormal frequencies and the total AUC.

Alpha is a parameter to filter the amplitudes. By default, its a greater than comparison. This can be changed using the method parameter (By default 'amplitude') into quantile and filter by the alpha quantile of the amplitudes.

NORMAL_FREQ_MAX and NORMAL_FREQ_MIN are range parameters. They refer to the normal ranges that you expect to have.

.. math::

    frequencies_{low} = frequencies > NORMALFREQMIN

.. math::

   frequencies_{high} = frequencies < NORMALFREQMAX

.. math::

    NAUCA = \frac{\int frequencies_{low} + \int frequencies_{high}}{\int frequencies}

Parameters
^^^^^^^^^^
* alpha: A filter parameter to include more or less data. It is recommended to go for 0.1 alpha with method ampliutde.
* NORMAL_FREQ_MIN: Min frequency that the analysis should take into account.
* NORMAL_FREQ_MAX: Max frequency that the analysis should take into account.

Example
^^^^^^^
..  code-block:: python
    
    from hivesignal.analysis import nauca
    from hivesignal.transform import fourier_transform
    from hivesignal.io import read_comtrade # Can also use from hiveadb.io import read_comtrade for azure databricks.

    ft = fourier_transform(read_comtrade("2020-01-01.CFG", "/mnt/raw-zone/", "2020-01-01.dat", "/mnt/raw-zone/", return_mode = "analog"))
    first_chanel_fft = ft[0]
    nauca(first_chanel_fft)