"""
Static rule engine that recommends plot kinds given an IGEM result type.

Discovery is intentionally dict-driven — there is no ML and no
auto-detection of "the right" plot. Rules either trigger or they
don't, based on the type of the input and (for ``RegressionResults``)
the schema of the underlying DataFrame: the presence of
``term1`` / ``term2`` discriminates interaction-study output from
regular association/EWAS/GWAS results, and ``p_corrected`` enables the
FDR-corrected Manhattan suggestion.
"""
from __future__ import annotations

from typing import Any

from igem.modules.analyze.results import RegressionResults
from igem.modules.data import Genotypes, Phenotypes


_INTERACTION_COLS = ("term1", "term2")


def _is_interaction(results: RegressionResults) -> bool:
    return all(c in results.df.columns for c in _INTERACTION_COLS)


def suggest_plots(obj: Any) -> list[str]:
    """
    Return a list of plot kinds appropriate for ``obj``.

    The return values are bridge / primitive names callable from
    :mod:`igem.modules.plot` — for example ``"manhattan"`` maps to
    :func:`igem.plot.from_results(..., kind="manhattan")`. Pass the
    suggestions back to the bridge of the matching ``from_*`` family.

    Returns ``[]`` for unsupported types so callers can use the result
    as a falsy guard (``if not igem.plot.suggest_plots(x): ...``).
    """
    if isinstance(obj, RegressionResults):
        if _is_interaction(obj):
            suggestions = ["heatmap", "top_pairs"]
        else:
            suggestions = ["manhattan", "qq", "top"]
            if "p_corrected" in obj.df.columns:
                suggestions.insert(1, "manhattan_fdr")
        return suggestions
    if isinstance(obj, Phenotypes):
        return ["distributions"]
    if isinstance(obj, Genotypes):
        return ["maf_distribution", "call_rate_distribution"]
    return []
