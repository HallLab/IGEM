from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from igem.core.core import IGEMCore


@dataclass
class BaseComponent:
    """
    Base for all IGEM client components.

    Components are thin wrappers that:
      - hold a reference to the shared ``IGEMCore`` (logger, http, config)
      - expose a stable public interface (``igem.<component>.<method>``)
      - delegate logic to domain managers in ``igem.modules.<name>``.
    """

    core: "IGEMCore"
