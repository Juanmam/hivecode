from pandas import DataFrame


def nauca(df, NORMAL_FREQ_MIN: float, NORMAL_FREQ_MAX: float, amplitude_col: str = "amplitud", frequency_col: str = "frequency", alpha: float = 0.1, method: str = "amp", return_mode: str = "nauca"):
    # AOC
    df = DataFrame({"frecuencia": df[frequency_col], "indicador": df[amplitude_col]})
    df["indicador"] = (df.indicador - df.indicador.min())/(df.indicador.max() - df.indicador.min())

    AUC = df.indicador.sum()
    MODE = df.loc[df.indicador == df.indicador.max(), "frecuencia"].to_list()[0]

    ### Aplicamos algun filtro para reducir el ruido.
    # En este caso escogimos el uso de quantiles para seleccionar el 1% superior.
    if method == "amp" or "amplitude":
        df_th = df[df.indicador > alpha]
    elif method == "quantile":
        df_th = df[df.indicador > df.indicador.quantile(alpha)]
    else:
        raise Exception(f"Parameter method got {method} but only accepted values are amplitud or quantile.")

    # Sacamos los min y los max del 1% mas afectado
    MIN = df_th.frecuencia.min()
    MAX = df_th.frecuencia.max()

    # Sacamos valores de auc min y max
    AUC_MIN = df_th[df_th.frecuencia < NORMAL_FREQ_MIN].indicador.sum()
    AUC_MAX = df_th[df_th.frecuencia > NORMAL_FREQ_MAX].indicador.sum()

    DIFF_AUC = (AUC_MIN + AUC_MAX) / AUC
    if return_mode.lower() == "all":
        return {"AUC":AUC, "MIN":MIN, "MAX":MAX, "MODE":MODE, "AUC_MIN":AUC_MIN, "AUC_MAX":AUC_MAX, "DIFF_AUC":DIFF_AUC}
    elif return_mode.lower() == "nauca":
        return DIFF_AUC
    else:
        raise Exception(f"Parameter return_mode got {return_mode} but can only accept all or nauca.")
