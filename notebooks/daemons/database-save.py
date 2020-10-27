#!/usr/bin/env python3
import datetime
import pika
import json
import pandas as pd
from rabbitmq import Subscriber
from config import app_config
from db import mk_schema

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

# adds the word IGNORE after INSERT in sqlalchemy
@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kwords):
    return compiler.visit_insert(insert.prefix_with('IGNORE'), **kwords)

meta = MetaData()
mk_schema(meta)
engine = create_engine('{db.driver}://{db.username}:{db.password}@{db.host}/{db.database}'.format(db = app_config.db))
meta.create_all(engine)

class DbSubscriber(Subscriber):
    def on_message_callback(self, basic_delivery, properties, body):
        body_object = json.loads(body)
        if 'table_name' not in body_object:
            return
        table_name = body_object['table_name']
        if 'table_desc' not in body_object:
            return
        table_desc = body_object['table_desc']
        
        try:
            df = pd.DataFrame.from_dict(table_desc)
        except:
            return
        
        if 'stamp' in df.columns and 'time' not in df.columns:
            time_col = df['stamp'].apply(lambda stamp : datetime.datetime.utcfromtimestamp(stamp // 1000))
            time_pos = df.columns.get_loc('stamp') + 1
            df.insert(time_pos, column = 'time', value = time_col)
        
        try:
            df.to_sql(
                name = table_name,
                con = engine,
                if_exists = 'append',
                index = False,
                method = 'multi'
            )
        except:
            pass

params = pika.ConnectionParameters(host='localhost')
subscriber = DbSubscriber(params)
subscriber['queue'] = 'database'
subscriber['routing_key'] = 'database.save'
        
if __name__ == '__main__':
    subscriber.run()
