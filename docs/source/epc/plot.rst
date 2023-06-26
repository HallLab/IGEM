Plot
====

Functions that generate plots

`histogram`
-----------

Plot a histogram of the values in the given column.

Parameters
----------
- `data`: pd.DataFrame
  The DataFrame containing data to be plotted
- `column`: str
  The name of the column that will be plotted
- `figsize`: Tuple[int, int], default (12, 5)
  The figure size of the resulting plot
- `title`: str or None, default None
  The title used for the plot
- `figure`: matplotlib Figure or None, default None
  Pass in an existing figure to plot to that instead of creating a new one (ignoring figsize)
- `**kwargs`: Other keyword arguments to pass to the histplot or catplot function of Seaborn

Examples
--------
``` python
import igem
igem.epc.plot.histogram(
    nhanes_discovery_cont,
    column="BMXBMI",
    title=x,
    bins=100
)
```

`distributions`
---------------

Create a pdf containing histograms for each binary or categorical variable and one of several types of plots for each continuous variable.

Parameters
----------
- `data`: pd.DataFrame
  The DataFrame containing data to be plotted
- `filename`: str
  Name of the saved pdf file. The extension will be added automatically if it was not included.
- `continuous_kind`: str, default "count"
  What kind of plots to use for continuous data. Binary and Categorical variables will always be shown with histograms. One of {'count', 'box', 'violin', 'qq'}
- `nrows`: int, default 4
  Number of rows per page
- `ncols`: int, default 3
  Number of columns per page
- `quality`: str, default "medium"
  Adjusts the DPI of the plots (150, 300, or 1200)
- `variables`: List[str] or None
  Which variables to plot. If None, all variables are plotted.
- `sort`: bool, default True
  Whether or not to sort variable names

Examples
--------
``` python
import igem
igem.epc.plot.distributions(
    df[['female', 'occupation', 'LBX074']], filename="test"
)
```

`manhattan`
-----------

Create a Manhattan-like plot for a list of EWAS Results.

Parameters
----------
- `dfs`: Dict[str, pd.DataFrame]
  Dictionary of dataset names to pandas dataframes of ewas results (requires certain columns)
- `categories`: Dict[str, str] or None, default None
  A dictionary mapping each variable name to a category name for optional grouping
- `bonferroni`: float or None, default 0.05
  Show a cutoff line at the p-value corresponding to a given bonferroni-corrected p-value
- `fdr`: float or None, default None
  Show a cutoff line at the p-value corresponding to a given false discovery rate (FDR)
- `num_labeled`: int, default 3
  Label the top <num_labeled> results with the variable name
- `label_vars`: List[str] or None, default None
  Label the named variables (or pass None to skip labeling this way)
- `figsize`: Tuple[int, int], default (12, 6)
  The figure size of the resulting plot in inches
- `dpi`: int, default 300
  The figure dots-per-inch
- `title`: str or None, default None
  The title used for the plot
- `figure`: matplotlib Figure or None, default

