# -*- coding: utf-8 -*-

import uuid

from tortoise import Model
from tortoise.backends.base.executor import BaseExecutor
from tortoise.contrib.duckdb.regex import duckdb_posix_regex
from tortoise.filters import posix_regex


class DuckDBExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN QUERY PLAN"
    DB_NATIVE = BaseExecutor.DB_NATIVE | {bool, uuid.UUID}
    FILTER_FUNC_OVERRIDE = {
        posix_regex: duckdb_posix_regex,
    }

    async def _process_insert_result(self, instance: Model, results: dict | None) -> None:
        if results:
            generated_fields = self.model._meta.generated_db_fields
            db_projection = instance._meta.fields_db_projection_reverse
            for key, val in zip(generated_fields, results):
                setattr(instance, db_projection[key], val)
