import json

from sqlalchemy import event

from database.db import engine, units, metadata, entity_or_function


def prepopulate_units(target, connection, **kwargs):
    print("Populating units...")
    with open("database/initial_data/units.json", "r", encoding="UTF-8") as file:
        values = json.loads(json.load(file))
        connection.execute(target.insert(), *values)


def prepopulate_functions(target, connection, **kwargs):
    print("Populating functions...")
    with open("database/initial_data/functions.json", "r", encoding="UTF-8") as file:
        values = json.load(file)
        connection.execute(target.insert(), *values)


def init_db():
    event.listen(units, "after_create", prepopulate_units)
    event.listen(entity_or_function, "after_create", prepopulate_functions)

    metadata.create_all(engine)
