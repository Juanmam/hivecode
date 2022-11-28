from hivecore.function import lib_required
from pandas import DataFrame, concat

def read_comtrade(file_name_cfg: str, path_cfg: str, file_name_dat: str, path_dat: str, return_mode: str = 'all'):
    # We make sure comtrade is installed, if not we will force install it.
    lib_required('comtrade')
    
    # We read the comtrade with all posible combinations of .cfg and .dat
    rec = Comtrade()
    try:
        rec.load(f'/{path_cfg}/{file_name_cfg.split(".")[0]}.cfg', f'/{path_dat}/{file_name_dat.split(".")[0]}.dat')
    except:
        try:
            rec.load(f'/{path_cfg}/{file_name_cfg.split(".")[0]}.cfg', f'/{path_dat}/{file_name_dat.split(".")[0]}.DAT')
        except:
            try:
                rec.load(f'/{path_cfg}/{file_name_cfg.split(".")[0]}.CFG', f'/{path_dat}/{file_name_dat.split(".")[0]}.dat')
            except:
                try:
                    rec.load(f'/{path_cfg}/{file_name_cfg.split(".")[0]}.CFG', f'/{path_dat}/{file_name_dat.split(".")[0]}.DAT')
                except:
                    raise

    # We capture analog variables
    df_analog = [DataFrame({"time": rec.time.tolist()})]
    df_analog = [*df_analog, *list(map(lambda i: DataFrame({rec.analog_channel_ids[i]: rec.analog[i].tolist()}), range(len(rec.analog))))]
    df_analog = concat(df_analog, axis = 1)
    time = df_analog.time.copy()
    timestep = df_analog.mode().time
    del(time)
    df_analog.set_index("time", inplace=True)

    # We capture digital variables
    df_digital = [DataFrame({"time": rec.time.tolist()})]
    df_digital = [*df_digital, *list(map(lambda i: DataFrame({rec.status_channel_ids[i]: rec.status[i].tolist()}), range(len(rec.status))))]
    df_digital = concat(df_digital, axis = 1)
    df_digital.set_index("time", inplace=True)
    
    if return_mode == "analog":
        return df_analog
    elif return_mode == "digital":
        return df_digital
    elif return_mode == "signals":
        return df_analog, df_digital
    elif return_mode == "all":
        return df_analog, df_digital, {"station_name": rec.station_name, "frequency": rec.frequency, "start_timestamp": rec.start_timestamp, "trigger_timestamp": rec.trigger_timestamp, "timestep": timestep}
    else:
        raise Exception(f"Parameter return_mode only accepts 'analog', 'digital' or 'all', but '' was given.")