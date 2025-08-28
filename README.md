# Tortoise ORM backend for DuckDB

This integration allows you to use asyncio duckdb APIs with Tortoise ORM.

## Install

tortoise-orm-extra-duckdb is compatible with Python 3.11 and newer. You can install it from PyPI:

``` console
$ pip install tortoise-orm-extra-duckdb
```

## Usage

It is very simple to use. You only need to add two lines of code to the original project and switch the database to
duckdb.

``` python
import asyncio

from tortoise import Tortoise, fields, run_async
from tortoise.models import Model
+ from tortoise.patch import monkey_patch


class Event(Model):
    id = fields.IntField(primary_key=True)
    name = fields.TextField(description="Name of the event that corresponds to an action")
    datetime = fields.DatetimeField(
        null=True, description="Datetime of when the event was generated"
    )

    class Meta:
        table = "event"
        table_description = "This table contains a list of all the example events"

    def __str__(self):
        return self.name


async def main():
~   await Tortoise.init(db_url="duckdb://:memory:", modules={"models": ["__main__"]})
    await Tortoise.generate_schemas()

    event = await Event.create(id=1, name="Test")
    await Event.filter(id=event.id).update(name="Updated name")

    print(await Event.filter(name="Updated name").first())

    await Event(id=2, name="Test 2").save()
    print(await Event.all().values_list("id", flat=True))
    print(await Event.all().values("id", "name"))


if __name__ == "__main__":
+   monkey_patch()
    asyncio.run(main())
```

## License

tortoise-orm-extra-duckdb is licensed under the MIT license.
I am providing code in this repository to you under an open source license.
This is my personal repository; the license you receive to my code is from me and not from my employer.
See the [LICENSE](https://github.com/cnfairydream/tortoise-orm-extra-duckdb/blob/main/LICENSE) file for details.
