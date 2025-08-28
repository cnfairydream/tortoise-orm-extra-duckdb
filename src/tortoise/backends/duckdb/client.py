# -*- coding: utf-8 -*-

import asyncio
import os
from collections.abc import Callable, Coroutine, Sequence
from contextlib import suppress
from functools import wraps
from typing import Any, TypeVar, cast

import duckdb
from aioduck import AsyncConnection
from pypika_tortoise.dialects.duckdb import DuckDBQuery
from tortoise.backends.base.client import (
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    NestedTransactionContext,
    T_conn,
    TransactionalDBClient,
    TransactionContext,
)
from tortoise.backends.duckdb.executor import DuckDBExecutor
from tortoise.backends.duckdb.schema_generator import DuckDBSchemaGenerator
from tortoise.connection import connections
from tortoise.exceptions import (
    IntegrityError,
    OperationalError,
    TransactionManagementError,
)

T = TypeVar("T")
FuncType = Callable[..., Coroutine[None, None, T]]


def translate_exceptions(func: FuncType) -> FuncType:
    @wraps(func)
    async def wrapper(self, query, *args) -> T:
        try:
            return await func(self, query, *args)
        except duckdb.OperationalError as exc:
            raise OperationalError(exc)
        except duckdb.IntegrityError as exc:
            raise IntegrityError(exc)

    return wrapper


class DuckDBClient(BaseDBAsyncClient):
    executor_class = DuckDBExecutor
    query_class = DuckDBQuery
    schema_generator = DuckDBSchemaGenerator
    capabilities = Capabilities(
        "duckdb",
        daemon=False,
        support_for_update=False,
        support_update_limit_order_by=False,
    )

    def __init__(self, file_path: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.filename = file_path
        self.pragmas = kwargs.copy()
        self._connection: AsyncConnection | None = None
        self._lock = asyncio.Lock()

    async def _load_config(self):
        async with self._connection.cursor() as cursor:
            await cursor.execute("SELECT * FROM duckdb_settings()")
            rows = await cursor.fetchall()
            self.config = {row[0]: row[3] for row in rows}

    async def _apply_config(self):
        valid_pragmas = {}
        for pragma, val in self.pragmas.items():
            if pragma in self.config:
                match self.config[pragma]:
                    case "BIGINT" | "UBIGINT":
                        val = int(val)
                    case "BOOLEAN":
                        val = bool(val)
                    case "DOUBLE":
                        val = float(val)
                    case "VARCHAR":
                        val = "'" + str(val) + "'"
                    case "VARCHAR[]":
                        val = [str(v) for v in val]
                valid_pragmas[pragma] = val
                await self._connection.execute(f"SET {pragma} = {val}")
        self.pragmas = valid_pragmas

    async def create_connection(self, with_db: bool) -> None:
        if not self._connection:  # pragma: no branch
            self._connection = AsyncConnection(self.filename)
            await self._connection.agent.start()
            await self._load_config()
            await self._apply_config()
            self.log.debug(
                "Created connection %s with params: filename=%s %s",
                self._connection,
                self.filename,
                " ".join(f"{k}={v}" for k, v in self.pragmas.items()),
            )

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()
            await self._connection.agent.stop()
            self.log.debug(
                "Closed connection %s with params: filename=%s %s",
                self._connection,
                self.filename,
                " ".join(f"{k}={v}" for k, v in self.pragmas.items()),
            )

            self._connection = None

    async def db_create(self) -> None:
        # DB's are automatically created once accessed
        pass

    async def db_delete(self) -> None:
        await self.close()
        with suppress(FileNotFoundError):
            os.remove(self.filename)

    def acquire_connection(self) -> ConnectionWrapper:
        return ConnectionWrapper(self._lock, self)

    def _in_transaction(self) -> TransactionContext:
        return DuckDBTransactionContext(DuckDBTransactionWrapper(self), self._lock)

    async def execute_insert(self, query: str, values: list) -> int:
        inserted, _ = await self.execute_query(query, values)
        return inserted

    @translate_exceptions
    async def execute_many(self, query: str, values: list[list]) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # This code is only ever called in AUTOCOMMIT mode
            await connection.begin()
            try:
                await connection.executemany(query, values)
            except BaseException:
                await connection.rollback()
                raise
            else:
                await connection.commit()

    @translate_exceptions
    async def execute_query(
            self, query: str, values: list | None = None
    ) -> tuple[int, Sequence[dict]]:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            async with connection.cursor() as cursor:
                await cursor.execute(query, values)
                pl = await cursor.pl()
                return pl.shape[0], pl.to_dicts()

    async def execute_query_dict(self, query: str, values: list | None = None) -> list[dict]:
        _, rows = await self.execute_query(query, values)
        return rows

    @translate_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug(query)
            await connection.execute(query)


class DuckDBTransactionContext(TransactionContext):
    """A DuckDB-specific transaction context.

    DuckDB uses a single connection, meaning that transactions need to
    acquire an exclusive lock on the connection to prevent other operations
    from interfering with the transaction. This is done by acquiring a lock
    on the connection object itself.
    """

    __slots__ = ("connection", "connection_name", "token", "_trxlock")

    def __init__(self, connection: Any, trxlock: asyncio.Lock) -> None:
        self.connection = connection
        self.connection_name = connection.connection_name
        self._trxlock = trxlock

    async def ensure_connection(self) -> None:
        if not self.connection._connection:
            await self.connection._parent.create_connection(with_db=True)
            self.connection._connection = self.connection._parent._connection

    async def __aenter__(self) -> T_conn:
        await self._trxlock.acquire()
        await self.ensure_connection()
        self.token = connections.set(self.connection_name, self.connection)
        await self.connection.begin()
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if not self.connection._finalized:
                if exc_type:
                    # Can't rollback a transaction that already failed.
                    if exc_type is not TransactionManagementError:
                        await self.connection.rollback()
                else:
                    await self.connection.commit()
        finally:
            connections.reset(self.token)
            self._trxlock.release()


class DuckDBTransactionWrapper(DuckDBClient, TransactionalDBClient):

    def __init__(self, connection: DuckDBClient) -> None:
        self.capabilities = connection.capabilities
        self.connection_name = connection.connection_name
        self._connection: AsyncConnection = cast(AsyncConnection, connection._connection)
        self.log = connection.log
        self._finalized = False
        self._parent = connection

    def _in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(DuckDBTransactionWrapper(self))

    @translate_exceptions
    async def execute_many(self, query: str, values: list[list]) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # Already within transaction, so ideal for performance
            await connection.executemany(query, values)

    async def begin(self) -> None:
        try:
            await self._connection.begin()
        except duckdb.OperationalError as exc:
            raise TransactionManagementError(exc)

    async def commit(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await self._connection.commit()
        self._finalized = True

    async def rollback(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await self._connection.rollback()
        self._finalized = True

    async def savepoint(self) -> None:
        await self._connection.checkpoint()

    async def savepoint_rollback(self) -> None:
        pass

    async def release_savepoint(self) -> None:
        pass
