from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from igem_backend.ge import GECore


@dataclass
class BaseComponent:
    """
    Base for all GE components.

    Components are thin wrappers that:
    - Validate preconditions (DB connected, etc.)
    - Expose a stable public interface (ge.<component>.<method>)
    - Delegate logic to domain managers (ETLManager, etc.)
    """

    core: "GECore"

    def require_db(self):
        return self.core.require_db()
