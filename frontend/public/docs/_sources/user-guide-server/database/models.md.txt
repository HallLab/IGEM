# Models

🚧 *In progress.*

Will cover:

- The shared `Base` and how to register a model so
  `bootstrap_models` picks it up.
- The entity model: `EntityType`, `EntityAlias`, the master tables
  per domain (`GeneMaster`, `ChemicalMaster`, `PathwayMaster`, …).
- Relationships: `EntityRelationshipType`, `EntityRelationship`,
  and how membership tables (e.g. `DiseaseGroupMembership`) tie
  back.
- The metadata tables: `IgemMetadata`, `SystemConfig`,
  `ETLSourceSystem`, `ETLDataSource`.
- Working with sessions: `ge.db.get_session()`, transactional
  patterns, when to use raw `engine.connect()`.
- Where the `pgvector` `embedding` column lives and why it is
  hand-managed outside Alembic.
