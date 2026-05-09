"""
Plot
====

Visual layer of the IGEM client.

Two surfaces:

* **Primitives** (``igem.modules.plot.{manhattan, qq_plot, dotplot,
  distribution, heatmap, miami_plot, ...}``) — pure matplotlib functions
  taking ``pd.DataFrame`` / ``pd.Series`` / arrays. Useful for callers
  who already have data in the right shape, or who want to customise
  styling beyond what the bridges expose.
* **Bridges** (``from_results``, ``from_describe``, ``from_modify``,
  ``from_interaction``) — accept the typed IGEM objects directly
  (``RegressionResults``, ``Phenotypes``, ``Genotypes``) and dispatch
  to the right primitive with sensible defaults.

The high-level facade ``igem.plot.X(...)`` mirrors this surface.
``suggest_plots(obj)`` returns the plot kinds that make sense for any
IGEM result type — also reachable as ``results.suggested_plots()`` for
the regression family.
"""
from igem.modules.plot.bridges.from_describe import from_describe
from igem.modules.plot.bridges.from_interaction import from_interaction
from igem.modules.plot.bridges.from_modify import from_modify
from igem.modules.plot.bridges.from_results import from_results
from igem.modules.plot.primitives.distribution import distribution
from igem.modules.plot.primitives.dotplot import dotplot
from igem.modules.plot.primitives.heatmap import heatmap
from igem.modules.plot.primitives.manhattan import (
    manhattan,
    manhattan_bonferroni,
    manhattan_fdr,
    miami_plot,
)
from igem.modules.plot.primitives.qq import qq_plot
from igem.modules.plot.suggest import suggest_plots

__all__ = [
    "from_results",
    "from_describe",
    "from_modify",
    "from_interaction",
    "manhattan",
    "manhattan_fdr",
    "manhattan_bonferroni",
    "miami_plot",
    "qq_plot",
    "dotplot",
    "distribution",
    "heatmap",
    "suggest_plots",
]
