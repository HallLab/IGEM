from typing import Optional

from sqlalchemy.exc import IntegrityError

from igem_backend.modules.db.models.model_entities import (
    Entity,
    EntityAlias,
    EntityRelationship,
)


class EntityQueryMixin:
    """
    Reusable ORM helpers for DTPs to create/resolve entities and relationships.

    Canonical edge ordering (enforced in get_or_create_relationship):
    - Gene always as entity_1 when paired with any non-Gene entity.
    - Gene vs Gene: lower entity_id as entity_1.
    - All other pairs: inserted as provided; use UNION at query time.
    """

    # ------------------------------------------------------------------
    # Canonical ordering
    # ------------------------------------------------------------------

    def _gene_type_id(self) -> int:
        """Cached lookup for the 'Genes' EntityType id."""
        if not hasattr(self, "_cached_gene_type_id"):
            self._cached_gene_type_id = self.get_entity_type_id("Genes")
        return self._cached_gene_type_id

    def _canonical_order(
        self,
        e1_id: int,
        e1_type_id: Optional[int],
        e2_id: int,
        e2_type_id: Optional[int],
    ) -> tuple[int, Optional[int], int, Optional[int]]:
        """
        Return (entity_1_id, entity_1_type_id, entity_2_id, entity_2_type_id)
        with Gene always in the entity_1 slot.
        """
        gene_tid = self._gene_type_id()
        e1_gene = e1_type_id == gene_tid
        e2_gene = e2_type_id == gene_tid

        # entity_2 is Gene, entity_1 is not → swap
        if e2_gene and not e1_gene:
            return e2_id, e2_type_id, e1_id, e1_type_id

        # both Gene → lower id first (deterministic, avoids duplicate pairs)
        if e1_gene and e2_gene and e1_id > e2_id:
            return e2_id, e2_type_id, e1_id, e1_type_id

        return e1_id, e1_type_id, e2_id, e2_type_id

    # ------------------------------------------------------------------
    # Entity helpers
    # ------------------------------------------------------------------

    def get_or_create_entity(
        self,
        name: str,
        type_id: int,
        data_source_id: int,
        package_id: Optional[int] = None,
        alias_type: str = "preferred",
        xref_source: Optional[str] = None,
        alias_norm: Optional[str] = None,
        is_active: bool = True,
        auto_commit: bool = True,
    ) -> tuple[Optional[int], bool]:
        """
        Get or create an Entity by its primary alias.
        Returns (entity_id, is_new).
        """
        try:
            clean_name = str(name).strip()
            if not clean_name:
                raise ValueError("Entity name must not be empty.")

            existing = (
                self.session.query(EntityAlias)
                .filter_by(
                    alias_value=clean_name,
                    alias_type=alias_type,
                    xref_source=xref_source,
                    is_primary=True,
                )
                .first()
            )
            if existing:
                return existing.entity_id, False

            entity = Entity(
                type_id=type_id,
                is_active=is_active,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(entity)
            self.session.flush()

            primary = EntityAlias(
                entity_id=entity.id,
                type_id=type_id,
                alias_value=self.guard_alias(clean_name),
                alias_type=alias_type,
                xref_source=xref_source,
                alias_norm=self.guard_alias_norm(alias_norm or clean_name),
                is_primary=True,
                is_active=is_active,
                locale="en",
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(primary)

            if auto_commit:
                self.session.commit()

            return entity.id, True

        except Exception as e:
            self.session.rollback()
            self.logger.log(
                f"get_or_create_entity failed for '{name}': {e}", "WARNING"
            )
            return None, False

    def add_aliases(
        self,
        entity_id: int,
        type_id: int,
        aliases: list[dict],
        is_active: bool = True,
        data_source_id: int = 0,
        package_id: Optional[int] = None,
        auto_commit: bool = True,
    ) -> int:
        """
        Add a list of alias dicts to an entity, skipping existing ones.
        Returns the number of aliases added.
        """
        existing = {
            (a.alias_value, a.alias_type, a.xref_source)
            for a in self.session.query(EntityAlias)
            .filter_by(entity_id=entity_id)
            .all()
        }

        seen_norms: set[str] = set()
        count = 0
        for alias in aliases:
            key = (
                alias["alias_value"].strip(),
                alias["alias_type"],
                alias["xref_source"],
            )
            if key in existing:
                continue
            norm = (alias.get("alias_norm") or "").strip()
            if norm and norm in seen_norms:
                continue
            if norm:
                seen_norms.add(norm)

            self.session.add(EntityAlias(
                entity_id=entity_id,
                type_id=type_id,
                alias_value=self.guard_alias(key[0]),
                alias_type=key[1],
                xref_source=key[2],
                alias_norm=self.guard_alias_norm(alias.get("alias_norm")),
                locale=alias.get("locale", "en"),
                is_primary=False,
                is_active=is_active,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            ))
            count += 1

        if not auto_commit:
            return count

        try:
            self.session.commit()
            return count
        except IntegrityError:
            self.session.rollback()
            self.logger.log(
                f"Rollback while adding aliases to entity {entity_id}", "WARNING"
            )
            return 0

    # ------------------------------------------------------------------
    # Relationship helpers
    # ------------------------------------------------------------------

    def get_or_create_relationship(
        self,
        entity_1_id: int,
        entity_2_id: int,
        relationship_type_id: int,
        data_source_id: int,
        entity_1_type_id: Optional[int] = None,
        entity_2_type_id: Optional[int] = None,
        package_id: Optional[int] = None,
        discovery_method: str = "structured",
        confidence_score: Optional[float] = None,
        evidence_count: Optional[int] = None,
        source_ref: Optional[str] = None,
        auto_commit: bool = True,
    ) -> bool:
        """
        Get or create an EntityRelationship.

        Applies canonical ordering (Gene always entity_1) before insert.
        Returns True if created, False if already exists.
        """
        e1_id, e1_tid, e2_id, e2_tid = self._canonical_order(
            entity_1_id, entity_1_type_id, entity_2_id, entity_2_type_id
        )

        existing = (
            self.session.query(EntityRelationship)
            .filter_by(
                entity_1_id=e1_id,
                entity_2_id=e2_id,
                relationship_type_id=relationship_type_id,
                data_source_id=data_source_id,
            )
            .first()
        )
        if existing:
            return False

        self.session.add(EntityRelationship(
            entity_1_id=e1_id,
            entity_2_id=e2_id,
            entity_1_type_id=e1_tid,
            entity_2_type_id=e2_tid,
            relationship_type_id=relationship_type_id,
            data_source_id=data_source_id,
            etl_package_id=package_id,
            discovery_method=discovery_method,
            confidence_score=confidence_score,
            evidence_count=evidence_count,
            source_ref=source_ref,
        ))

        if auto_commit:
            try:
                self.session.commit()
            except IntegrityError:
                self.session.rollback()
                return False

        return True
