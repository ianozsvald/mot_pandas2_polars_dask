import math
import matplotlib as mpl
import matplotlib.pyplot as plt

# https://github.com/ianozsvald/notes_to_self/blob/master/simpler_mpl.py

def set_common_mpl_styles(
    ax,
    legend=True,
    grid_axis="y",
    ylabel=None,
    xlabel=None,
    title=None,
    ymin=None,
    xmin=None,
):
    """Nice common plot configuration
    We might use it via `fig, ax = plt.subplots(constrained_layout=True, figsize=(8, 6))`
    """
    if grid_axis is not None:
        # depending on major/minor grid frequency we might
        # need the simpler form
        # ax.grid(axis=grid_axis)
        ax.grid(visible=True, which="both", axis=grid_axis)
    #if legend is False: # CHANGED
    #    ax.legend_.remove()
    #else:
    #    ax.legend()
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    if xlabel is not None:
        ax.set_xlabel(xlabel)
    if title is not None:
        ax.set_title(title)
    if ymin is not None:
        ax.set_ylim(ymin=ymin)
    if xmin is not None:
        ax.set_xlim(xmin=xmin)


def rotate_labels(x_axis=False, y_axis=False, rotation=-90):
    if x_axis:
        plt.xticks(rotation=rotation)
    if y_axis:
        plt.yticks(rotation=rotation)


def set_commas(ax, x_axis=False, y_axis=False):
    # NOTE this may not work well e.g. on bar plots
    # in which case make a df_to_plot where index has been
    # reset, turned with string formatting into good result,
    # then index has been set again
    texts = []
    if x_axis:
        ticks = ax.get_xticks()
        tick_labels = ax.get_xticklabels()
        for label in tick_labels:
            text = label.get_text()
            texts.append(f"{int(text):,}")
        plt.xticks(ticks=ticks, labels=texts)
    if y_axis:
        ticks = ax.get_yticks()
        tick_labels = ax.get_yticklabels()
        for label in tick_labels:
            text = label.get_text()
            text = text.replace('−', '-') # CHANGED
            texts.append(f"{int(text):,}")
        plt.yticks(ticks=ticks, labels=texts)