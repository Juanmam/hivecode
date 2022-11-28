from matplotlib.pyplot import figure, plot, legend, show

def plot_fourier(dfs):
    if not isinstance(dfs, list):
        dfs = [dfs]
        
    for df in dfs:
        figure(figsize = (50,5))
        plot(df.frequency, df.amplitud, label = "Waves")
        legend()

    show()