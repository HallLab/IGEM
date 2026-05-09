"""Pure plotting primitives — operate on DataFrames / arrays, no IGEM types."""
from igem.modules.plot.primitives.distribution import detect_kind, distribution
from igem.modules.plot.primitives.dotplot import dotplot
from igem.modules.plot.primitives.heatmap import heatmap
from igem.modules.plot.primitives.manhattan import (
    manhattan,
    manhattan_bonferroni,
    manhattan_fdr,
    miami_plot,
)
from igem.modules.plot.primitives.qq import qq_plot

__all__ = [
    "detect_kind",
    "distribution",
    "dotplot",
    "heatmap",
    "manhattan",
    "manhattan_bonferroni",
    "manhattan_fdr",
    "miami_plot",
    "qq_plot",
]
