from pandas import DataFrame
from numpy import array as nparray, abs
from scipy.fft import fft, fftfreq
from hivecore.function import lib_required

def fourier_transform(df, columns = None, time_col: str = None, normalized_amplitudes: bool = True):
    lib_required("scipy")
    # We pack columns to meet the criteria we need.
    if isinstance(columns, str):
        columns = [columns]
    elif not isinstance(columns, list) and not type(columns) == type(None):
        raise Exception(f"Parameter type {type(columns)} not supported for columns. Only supports str or list types.")
    
    if not time_col:
        timestep  = df.index.to_series().reset_index(drop=True)
    else:
        timestep  = df[time_col]

    # We filter df to only take into account the given columns
    
    if columns:
        df_analog = df[columns]
    else:
        df_analog = df
    
    # We create a dictionary with channel : phases. Channel is an str and phases is a list of complex numbers.
    fft_dict = dict(map(lambda column: (column, fft(nparray(df_analog[column]))), df_analog.columns))

    # Generamos el espectro de frequencias. Este va a ser nuestro eje X
    fftfreq_dict = dict(map(lambda column: (column, fftfreq(len(df_analog[column]), d=timestep.diff().mode()[0])), df_analog.columns))

    # Creamos una mascara para eliminar frecuencias negativas.
    masks        = dict(map(lambda item: (item[0], item[1] > 0),fftfreq_dict.items()))

    # Calculamos el fft teorico que normaliza los valores de corriente y voltaje.
    fft_theory   = dict(map(lambda item: (item[0], 2*abs(item[1]/len(item[1]))), fft_dict.items()))
    
    if normalized_amplitudes:
        return list(map(lambda amplitud, frequency, mask: DataFrame({"amplitude": amplitud[mask].tolist(), "frequency": frequency[mask].tolist()}), fft_theory.values(), fftfreq_dict.values(), masks.values()))
    else:
        return list(map(lambda amplitud, frequency, mask: DataFrame({"amplitude": amplitud[mask].tolist(), "frequency": frequency[mask].tolist()}), fft_dict.values(), fftfreq_dict.values(), masks.values()))