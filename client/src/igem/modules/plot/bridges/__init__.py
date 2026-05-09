"""Bridges from typed IGEM objects to pure plotting primitives."""
from igem.modules.plot.bridges.from_describe import from_describe
from igem.modules.plot.bridges.from_interaction import from_interaction
from igem.modules.plot.bridges.from_modify import from_modify
from igem.modules.plot.bridges.from_results import from_results

__all__ = ["from_describe", "from_interaction", "from_modify", "from_results"]
