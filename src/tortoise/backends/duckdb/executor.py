# -*- coding: utf-8 -*-

import uuid

from tortoise import Model
from tortoise.backends.base.executor import BaseExecutor
from tortoise.contrib.duckdb.regex import duckdb_posix_regex
from tortoise.fields import BigIntField, IntField, SmallIntField
from tortoise.filters import posix_regex


class DuckDBExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN QUERY PLAN"
    DB_NATIVE = BaseExecutor.DB_NATIVE | {bool, uuid.UUID}
    FILTER_FUNC_OVERRIDE = {
        posix_regex: duckdb_posix_regex,
    }

    async def _process_insert_result(self, instance: Model, results: int) -> None:
        pk_field_object = self.model._meta.pk
        if (
                isinstance(pk_field_object, (SmallIntField, IntField, BigIntField))
                and pk_field_object.generated
        ):
            instance.pk = results
