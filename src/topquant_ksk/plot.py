import seaborn as sns
import matplotlib.pyplot as plt

def heatmap(dataframe ,size=(12,6), annot = True, vmax=None, vmin=None, title=None, rotation=0, rotation_y=0,show_colorbar=False,fontsize=25):
    plt.figure(figsize=size)
    ax = sns.heatmap(dataframe.round(3)*100, cmap = 'RdYlBu_r', annot = annot , vmax=vmax, vmin=vmin, annot_kws={"size": fontsize},cbar=show_colorbar)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=rotation)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=rotation_y)
    plt.title(title, fontsize=20)
    plt.show()
    return None