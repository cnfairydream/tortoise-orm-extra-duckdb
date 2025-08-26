# -*- coding: utf-8 -*-

import uuid
from copy import copy
from enum import Enum
from typing import Any


def patch_pypika_enums():
    from pypika_tortoise import enums

    class Dialects(Enum):
        VERTICA = "vertica"
        CLICKHOUSE = "clickhouse"
        DUCKDB = "duckdb"
        ORACLE = "oracle"
        MSSQL = "mssql"
        MYSQL = "mysql"
        POSTGRESQL = "postgresql"
        REDSHIFT = "redshift"
        SQLITE = "sqlite"
        SNOWFLAKE = "snowflake"

    enums.Dialects = Dialects


def patch_pypika_terms():
    from pypika_tortoise.enums import Dialects
    from pypika_tortoise.terms import Parameter, Interval

    Parameter.IDX_PLACEHOLDERS[Dialects.DUCKDB] = lambda idx: f"${idx}"
    Interval.templates[Dialects.DUCKDB] = "INTERVAL '{expr} {unit}'"


def patch_tortoise_fields():
    from tortoise.fields.data import (
        IntField, BigIntField, SmallIntField, DatetimeField, FloatField, UUIDField, TimeField,
    )

    class _db_duckdb:
        GENERATED_SQL = "INT NOT NULL PRIMARY KEY"

    IntField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        GENERATED_SQL = "BIGINT NOT NULL PRIMARY KEY"

    BigIntField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        GENERATED_SQL = "SMALLINT NOT NULL PRIMARY KEY"

    SmallIntField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        SQL_TYPE = "TIMESTAMPTZ"

    DatetimeField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        SQL_TYPE = "DOUBLE"

    FloatField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        SQL_TYPE = "UUID"

    UUIDField._db_duckdb = _db_duckdb
    del _db_duckdb

    class _db_duckdb:
        SQL_TYPE = "TIMETZ"

    TimeField._db_duckdb = _db_duckdb
    del _db_duckdb


def patch_tortoise_queryset():
    from pypika_tortoise.functions import Cast
    from pypika_tortoise.terms import Case, Field
    from tortoise.queryset import BulkUpdateQuery
    from tortoise.utils import chunk

    def _make_queries(self) -> list[tuple[str, list[Any]]]:
        table = self.model._meta.basetable
        self.query = self._db.query_class.update(table)
        if self.capabilities.support_update_limit_order_by and self._limit:
            self.query._limit = self.query._wrapper_cls(self._limit)
            self.resolve_ordering(
                model=self.model,
                table=table,
                orderings=self._orderings,
                annotations=self._annotations,
            )

        self.resolve_filters()
        pk_attr = self.model._meta.pk_attr
        source_pk_attr = self.model._meta.fields_map[pk_attr].source_field or pk_attr
        pk = Field(source_pk_attr)
        for objects_item in chunk(self._objects, self._batch_size):
            query = copy(self.query)
            for field in self.fields:
                case = Case()
                pk_list = []
                for obj in objects_item:
                    pk_value = self.model._meta.fields_map[pk_attr].to_db_value(obj.pk, None)
                    field_obj = obj._meta.fields_map[field]
                    field_value = field_obj.to_db_value(getattr(obj, field), obj)
                    case.when(
                        pk == pk_value,
                        (
                            Cast(
                                self.query._wrapper_cls(field_value),
                                field_obj.get_for_dialect(
                                    self._db.schema_generator.DIALECT, "SQL_TYPE"
                                ),
                            )
                            if self._db.schema_generator.DIALECT in ("postgres", "duckdb")
                            else self.query._wrapper_cls(field_value)
                        ),
                    )
                    pk_list.append(pk_value)
                query = query.set(field, case)
                query = query.where(pk.isin(pk_list))
            self._queries.append(query)
        return [query.get_parameterized_sql() for query in self._queries]

    BulkUpdateQuery._make_queries = _make_queries
    del _make_queries


def patch_tortoise_backends():
    import urllib.parse as urlparse
    from tortoise.exceptions import ConfigurationError
    from tortoise.backends.base import config_generator
    from tortoise.backends.base.config_generator import DB_LOOKUP

    urlparse.uses_netloc.append("duckdb")
    DB_LOOKUP["duckdb"] = {
        "engine": "tortoise.backends.duckdb",
        "skip_first_char": False,
        "vmap": {"path": "file_path"},
        "defaults": {},
        "cast": {},
    }

    def expand_db_url(db_url: str, testing: bool = False) -> dict:
        url = urlparse.urlparse(db_url)
        if url.scheme not in DB_LOOKUP:
            raise ConfigurationError(f"Unknown DB scheme: {url.scheme}")

        db_backend = url.scheme
        db = DB_LOOKUP[db_backend]
        if db.get("skip_first_char", True):
            path: str | None = url.path[1:]
        else:
            path = url.netloc + url.path

        if not path:
            if db_backend in ("sqlite", "duckdb"):
                raise ConfigurationError("No path specified for DB_URL")
            # Other database backend accepts database name being None (but not empty string).
            path = None

        params: dict = {}
        for key, val in db["defaults"].items():
            params[key] = val
        for key, val in urlparse.parse_qs(url.query).items():
            cast = db["cast"].get(key, str)
            params[key] = cast(val[-1])

        if testing and path:
            path = path.replace("\\{", "{").replace("\\}", "}")
            path = path.format(uuid.uuid4().hex)

        vmap: dict = {}
        vmap.update(db["vmap"])
        params[vmap["path"]] = path
        if vmap.get("hostname"):
            params[vmap["hostname"]] = url.hostname or None
        try:
            if vmap.get("port") and url.port:
                params[vmap["port"]] = int(url.port)
        except ValueError:
            raise ConfigurationError("Port is not an integer")
        if vmap.get("username"):
            # Pass username as None, instead of empty string,
            # to let asyncpg retrieve username from environment variable or OS user
            params[vmap["username"]] = url.username or None
        if vmap.get("password"):
            # asyncpg accepts None for password, but aiomysql not
            params[vmap["password"]] = (
                None
                if (not url.password and db_backend in {"postgres", "asyncpg", "psycopg"})
                else urlparse.unquote_plus(url.password or "")
            )

        return {"engine": db["engine"], "credentials": params}

    config_generator.expand_db_url = expand_db_url
    del expand_db_url


def monkey_patch():
    patch_pypika_enums()
    patch_pypika_terms()
    patch_tortoise_fields()
    patch_tortoise_queryset()
    patch_tortoise_backends()
