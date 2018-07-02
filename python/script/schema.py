from datetime import datetime
from os import environ as env
from pony.orm import *

db = Database()
db.bind(provider='postgres', user=env['DATABASE_USER'], password=env['DATABASE_PASSWORD'], host=env['DATABASE_HOST'], database=env['DATABASE_DB'])

class Document(db.Entity):
    record_id = Required(str, unique=True)
    source = Required(str)
    nds_index = Optional(str)
    record = Optional(Json)
    hash = Optional(str, unique=True)
    scientific_name_group = Optional(str)
    status = Optional(str)
    datum = Optional(datetime, default=lambda: datetime.now())
    documenttype = Optional(str)
    PrimaryKey(record_id,source)
    

db.generate_mapping(create_tables=True)
