from sqlalchemy.types import BigInteger, Integer, TypeDecorator


class PKBigIntOrInt(TypeDecorator):
    """
    Primary-key type: INTEGER on SQLite (rowid-compatible), BIGINT on Postgres.
    """

    impl = BigInteger
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "sqlite":
            return dialect.type_descriptor(Integer())
        return dialect.type_descriptor(BigInteger())
