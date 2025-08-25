# -*- coding: utf-8 -*-

from tortoise.indexes import PartialIndex


class DuckDBIndex(PartialIndex):
    pass


class ZonemapsIndex(DuckDBIndex):
    INDEX_TYPE = "Zonemaps"


class ARTIndex(DuckDBIndex):
    INDEX_TYPE = "ART"
