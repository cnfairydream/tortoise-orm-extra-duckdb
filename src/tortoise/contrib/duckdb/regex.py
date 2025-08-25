# -*- coding: utf-8 -*-

import enum
from typing import cast

from pypika_tortoise.enums import SqlTypes
from pypika_tortoise.functions import Cast, Coalesce
from pypika_tortoise.terms import BasicCriterion, Term


class DuckDBRegexMatching(enum.Enum):
    POSIX_REGEX = " ~ "


def duckdb_posix_regex(field: Term, value: str):
    term = cast(Term, field.wrap_constant(value))
    return BasicCriterion(
        DuckDBRegexMatching.POSIX_REGEX, Coalesce(Cast(field, SqlTypes.VARCHAR), ""), term
    )
