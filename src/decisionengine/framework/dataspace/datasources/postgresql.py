import platform
import time
import logging

try:
    import dbutils.pooled_db as pooled_db
except ModuleNotFoundError:
    import DBUtils.PooledDB as pooled_db

if platform.python_implementation() == 'CPython':
    import psycopg2
    import psycopg2.extras
else:
    # try to load psycopg2cffi dynamically and use psycopg2 namespace
    import importlib
    importlib.import_module('psycopg2cffi')
    importlib.import_module('psycopg2cffi.compat')
    __import__('psycopg2cffi').compat.register()
    import psycopg2.extras

import decisionengine.framework.dataspace.datasource as ds

MAX_NUMBER_OF_RETRIES = 10
TIME_TO_SLEEP = 2


def generate_insert_query(table_name, keys):
    """
    Generate insert query given table name
    and list of fields

    :type table_name: :obj:`str`
    :arg table_name: Name of the table to insert into

    :keys: :obj:`list`
    :arg keys: List of column names

    :rtype: :obj:`str` - insert query

    """
    query = """
    INSERT INTO {} ({}) VALUES ({})
    """
    query = query.format(table_name, ",".join(keys), ("%s," * len(keys))[:-1])
    return query


SELECT_QUERY = """
SELECT tm.channel_manager_id, foo.* FROM {} foo, channel_manager tm
WHERE tm.sequence_id = foo.channel_manager_id
and foo.channel_manager_id=%s AND foo.generation_id=%s AND foo.key=%s
"""

SELECT_PRODUCTS = """
SELECT tm.channel_manager_id, foo.* FROM {} foo, channel_manager tm
WHERE tm.sequence_id = foo.channel_manager_id
and foo.channel_manager_id=%s
"""

SELECT_LAST_GENERATION_ID_BY_NAME = """
SELECT max(generation_id)
FROM dataproduct
WHERE channel_manager_id = (select  max(sequence_id) from channel_manager where name = %s)
"""

SELECT_LAST_GENERATION_ID_BY_NAME_AND_ID = """
SELECT max(dp.generation_id)
FROM dataproduct dp
JOIN channel_manager tm ON dp.channel_manager_id=tm.sequence_id
WHERE tm.name=%s
AND tm.channel_manager_id=%s
"""

SELECT_CHANNEL_MANAGER_BY_NAME = """
SELECT tm.name, tm.sequence_id, tm.channel_manager_id, tm.datestamp
FROM channel_manager tm where tm.sequence_id =
(SELECT max(sequence_id) from channel_manager where name = %s);
"""

SELECT_CHANNEL_MANAGER_BY_NAME_AND_ID = """
SELECT tm.name, tm.sequence_id, tm.channel_manager_id, tm.datestamp
FROM channel_manager tm where tm.name = %s and tm.channel_manager_id = %s
"""

DELETE_OLD_DATA_QUERY = """
DELETE FROM channel_manager where datestamp < current_date - interval '%s days'
"""


class Postgresql(ds.DataSource):
    """
    Implementation of postgresql data source
    """

    tables = {
        'header': [
            'channel_manager_id TEXT',
            'generation_id INT',
            'key TEXT',
            'create_time REAL',
            'expiration_time REAL',
            'scheduled_create_time REAL',
            'creator TEXT',
            'schema_id INT',
        ],
        'schema': [
            'schema_id INT',  # Auto generated
            'schema BLOB',    # keys in the value dict of the dataproduct table
        ],
        'metadata': [
            'channel_manager_id TEXT',
            'generation_id INT',
            'key TEXT',
            'state TEXT',
            'generation_time REAL',
            'missed_update_count INT',
        ],
        'dataproduct': [
            'channel_manager_id TEXT',
            'generation_id INT',
            'key TEXT',
            'value BLOB'
        ]
    }

    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.connection_pool = pooled_db.PooledDB(psycopg2,
                                                  **config_dict)
        self.retries = MAX_NUMBER_OF_RETRIES
        self.timeout = TIME_TO_SLEEP
        self.logger = logging.getLogger()
        self.logger.debug('Initializing a Postgresql datasource')

    def create_tables(self):
        return True

    def store_channel_manager(self, name, channel_manager_id):
        return self._update_returning_result("INSERT INTO channel_manager (name, channel_manager_id) values (%s, %s)",
                                             (name, channel_manager_id)).get('sequence_id')

    def get_channel_manager(self, channel_manager_name, channel_manager_id=None):
        if channel_manager_id:
            try:
                return self._select_dictresult(SELECT_CHANNEL_MANAGER_BY_NAME_AND_ID,
                                               (channel_manager_name, channel_manager_id))[0]
            except IndexError:
                raise KeyError("ChannelManager={} channel_manager_id={} not found".format(
                    channel_manager_name, channel_manager_id))
        else:
            try:
                return self._select_dictresult(SELECT_CHANNEL_MANAGER_BY_NAME,
                                               (channel_manager_name,))[0]
            except IndexError:
                raise KeyError(
                    "ChannelManager={} not found".format(channel_manager_name))

    def get_channel_managers(self,
                         channel_manager_name=None,
                         start_time=None,
                         end_time=None):

        SELECT_CHANNEL_MANAGERS = ("SELECT tm.name, "
                               "tm.sequence_id, "
                               "tm.channel_manager_id, "
                               "tm.datestamp "
                               "FROM channel_manager tm ")

        have_where = False
        if channel_manager_name:
            SELECT_CHANNEL_MANAGERS += " WHERE tm.name = '" + channel_manager_name + "'"
            have_where = True
        if start_time:
            if have_where:
                SELECT_CHANNEL_MANAGERS += " AND "
            else:
                SELECT_CHANNEL_MANAGERS += " WHERE "
                have_where = True
            SELECT_CHANNEL_MANAGERS += " tm.datestamp >= '" + start_time + "'"
        if end_time:
            if have_where:
                SELECT_CHANNEL_MANAGERS += " AND "
            else:
                SELECT_CHANNEL_MANAGERS += " WHERE "
                have_where = True
            SELECT_CHANNEL_MANAGERS += " tm.datestamp <=  '" + end_time + "'"
        try:
            return self._select_dictresult(SELECT_CHANNEL_MANAGERS +
                                           " ORDER BY tm.datestamp ASC")
        except IndexError:
            raise KeyError()

    def get_last_generation_id(self,
                               channel_manager_name,
                               channel_manager_id=None):
        if channel_manager_id:
            try:
                generation_id = self._select(SELECT_LAST_GENERATION_ID_BY_NAME_AND_ID,
                                             (channel_manager_name, channel_manager_id))[0][0]
                assert generation_id
                return generation_id
            except AssertionError:
                raise KeyError("Last generation id not found for channel_manager={} channel_manager_id={}".
                               format(channel_manager_name, channel_manager_id))
        else:
            try:
                generation_id = self._select(SELECT_LAST_GENERATION_ID_BY_NAME,
                                             (channel_manager_name, ))[0][0]
                assert generation_id
                return generation_id
            except AssertionError:
                raise KeyError("Last generation id not found for channel_manager={}".
                               format(channel_manager_name, ))

    def insert(self, channel_manager_id, generation_id, key,
               value, header, metadata):

        self._insert(ds.DataSource.dataproduct_table,
                     {'channel_manager_id': channel_manager_id,
                      'generation_id': generation_id,
                      'key': key,
                      'value': psycopg2.Binary(value)
                      })

        self._insert(ds.DataSource.header_table,
                     {'channel_manager_id': channel_manager_id,
                      'generation_id': generation_id,
                      'key': key,
                      'create_time': header.get('create_time'),
                      'scheduled_create_time': header.get('scheduled_create_time'),
                      'creator': header.get('creator'),
                      'schema_id': header.get('schema_id')
                      })

        self._insert(ds.DataSource.metadata_table,
                     {'channel_manager_id': channel_manager_id,
                      'generation_id': generation_id,
                      'key': key,
                      'state': metadata.get('state'),
                      'generation_time': metadata.get('generation_time'),
                      'missed_update_count': metadata.get('missed_update_count')
                      })

    def update(self, channel_manager_id, generation_id, key,
               value, header, metadata):

        q = """
            UPDATE {} SET value=%s
                      WHERE channel_manager_id=%s AND generation_id=%s AND key=%s
            """.format(ds.DataSource.dataproduct_table)

        self._update(q, (psycopg2.Binary(value),
                         channel_manager_id, generation_id, key))

        q = """
        UPDATE {} SET create_time=%s,
                      expiration_time=%s,
                      scheduled_create_time=%s,
                      creator=%s,
                      schema_id=%s
                  WHERE channel_manager_id=%s AND generation_id=%s AND key=%s
            """.format(ds.DataSource.header_table)
        self._update(q, (header.get('create_time'),
                         header.get('expiration_time'),
                         header.get('scheduled_create_time'),
                         header.get('creator'), header.get('schema_id'),
                         channel_manager_id, generation_id, key))

        q = """
             UPDATE {} SET state=%s,
                           generation_time=%s,
                           missed_update_count=%s
                        WHERE channel_manager_id=%s AND generation_id=%s AND key=%s
            """.format(ds.DataSource.metadata_table)
        self._update(q, (metadata.get('state'), metadata.get('generation_time'),
                         metadata.get('missed_update_count'),
                         channel_manager_id, generation_id, key))

    def get_header(self, channel_manager_id, generation_id, key):
        q = SELECT_QUERY.format(ds.DataSource.header_table)
        try:
            return self._select(q, (channel_manager_id, generation_id, key))[0]
        except IndexError:
            # do not log stack trace, Exception thrown is handled by the caller
            raise KeyError("channel_manager_id={} or generation_id={} or key={} not found".format(
                channel_manager_id, generation_id, key))

    def get_metadata(self, channel_manager_id, generation_id, key):
        q = SELECT_QUERY.format(ds.DataSource.metadata_table)
        try:
            return self._select(q, (channel_manager_id, generation_id, key))[0]
        except IndexError:
            # do not log stack trace, Exception thrown is handled by the caller
            raise KeyError("channel_manager_id={} or generation_id={} or key={} not found".format(
                channel_manager_id, generation_id, key))

    def get_dataproducts(self, channel_manager_id):
        q = SELECT_PRODUCTS.format(ds.DataSource.dataproduct_table)
        try:
            result = []
            rows = self._select_dictresult(q, (channel_manager_id,))
            for row in rows:
                result.append({'key': row['key'],
                               'channel_manager_id': row['channel_manager_id'],
                               'generation_id': row['generation_id'],
                               'value': row['value'].tobytes()})
            return result
        except IndexError:
            # do not log stack trace, Exception thrown is handled by the caller
            raise KeyError("channel_manager_id={} not found".format(channel_manager_id))

    def get_dataproduct(self, channel_manager_id, generation_id, key):
        q = SELECT_QUERY.format(ds.DataSource.dataproduct_table)
        try:
            value_row = self._select_dictresult(q, (channel_manager_id, generation_id, key))[0]
            return value_row['value'].tobytes()
        except IndexError:
            # do not log stack trace, Exception thrown is handled by the caller
            raise KeyError("channel_manager_id={} or generation_id={} or key={} not found".format(
                channel_manager_id, generation_id, key))

    def get_datablock(self, channel_manager_id, generation_id):
        return {}

    def duplicate_datablock(self, channel_manager_id, generation_id,
                            new_generation_id):
        for q in ("""
            INSERT INTO {} (channel_manager_id,
                            generation_id,
                            key,
                            value)
                   SELECT channel_manager_id,
                          %s,
                          key,
                          value
                   FROM {}
                   WHERE channel_manager_id=%s AND generation_id=%s
            """.format(ds.DataSource.dataproduct_table, ds.DataSource.dataproduct_table),
                  """
            INSERT INTO {} (channel_manager_id,
                            generation_id,
                            key,
                            state,
                            generation_time,
                            missed_update_count)
                   SELECT channel_manager_id,
                          %s,
                          key,
                          state,
                          generation_time,
                          missed_update_count
                   FROM {}
                   WHERE channel_manager_id=%s AND generation_id=%s
            """.format(ds.DataSource.metadata_table, ds.DataSource.metadata_table),
                  """
            INSERT INTO {} (channel_manager_id,
                        generation_id,
                        key,
                        create_time,
                        expiration_time,
                        scheduled_create_time,
                        creator,
                        schema_id)
                 SELECT channel_manager_id,
                        %s,
                        key,
                        create_time,
                        expiration_time,
                        scheduled_create_time,
                        creator,
                        schema_id
                 FROM {}
                 WHERE channel_manager_id=%s
                 AND   generation_id=%s
                 """.format(ds.DataSource.header_table, ds.DataSource.header_table)):
            self._insert(q, (new_generation_id, channel_manager_id, generation_id))

    def delete_data_older_than(self, days):
        """
        Delete data older that days interval
        :type days: :obj:`int`
        :arg days: remove data older than days interval
        """
        if days <= 0:
            # do not log stack trace, Exception thrown is handled by the caller
            raise ValueError("Argument has to be positive, non zero integer. Supplied {}".format(days))
        self._remove(DELETE_OLD_DATA_QUERY, (days, ))
        return

    def close(self):
        pass

    def connect(self):
        pass

    def get_schema(self, table=None):
        return {}

    def get_connection(self):
        i = self.retries + 1
        t = self.timeout
        while i:
            try:
                return self.connection_pool.connection()
            except Exception:
                # do not log stack trace, Exception thrown is handled by the caller
                i -= 1
                if not i:
                    raise
                else:
                    time.sleep(t)
                    t *= self.timeout

    def _select(self, query_string, values=None, cursor_factory=None):
        colnames, res = self.__query(query_string, values, cursor_factory)
        return res

    def __query(self, query_string, values=None, cursor_factory=None):
        db, cursor = None, None
        try:
            db = self.get_connection()
            if cursor_factory:
                cursor = db.cursor(cursor_factory=cursor_factory)
            else:
                cursor = db.cursor()
            if values:
                cursor.execute(query_string, values)
            else:
                cursor.execute(query_string)
            colnames = [desc[0] for desc in cursor.description]
            res = cursor.fetchall()
            return colnames, res
        except psycopg2.Error:
            # do not log stack trace, Exception thrown is handled by the caller
            raise
        finally:
            try:
                list([x.close if x else None for x in (cursor, db)])
            except psycopg2.Error:
                # do not log stack trace, Exception thrown is handled by the caller
                pass

    def _update(self, query_string, values=None):
        db, cursor = None, None
        try:
            db = self.connection_pool.connection()
            cursor = db.cursor()
            if values:
                cursor.execute(query_string, values)
            else:
                cursor.execute(query_string)
            db.commit()
        except psycopg2.Error:
            try:
                if db:
                    db.rollback()
            except psycopg2.Error:
                pass
            raise
        except psycopg2.Error:
            if db:
                db.rollback()
            raise
        finally:
            list([x.close if x else None for x in (cursor, db)])

    def _update_returning_result(self, query_string, values=None):
        db, cursor = None, None
        try:
            db = self.connection_pool.connection()
            cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query_string += " RETURNING *"
            if values:
                cursor.execute(query_string, values)
            else:
                cursor.execute(query_string)
            res = cursor.fetchone()
            db.commit()
            return res
        except psycopg2.Error:
            try:
                if db:
                    db.rollback()
            except psycopg2.Error:
                pass
            raise
        except psycopg2.Error:
            if db:
                db.rollback()
            raise
        finally:
            list([x.close if x else None for x in (cursor, db)])

    def _insert(self, table_name_or_sql_query, record=None):
        try:
            if record:
                if isinstance(record, dict):
                    q = generate_insert_query(
                        table_name_or_sql_query, list(record.keys()))
                    return self._update(q, list(record.values()))
                else:
                    return self._update(table_name_or_sql_query, record)
            else:
                return self._update(table_name_or_sql_query)
        except Exception:
            raise


    def _insert_returning_result(self, table_name_or_sql_query, record=None):
        try:
            if record:
                if isinstance(record, dict):
                    q = generate_insert_query(
                        table_name_or_sql_query, list(record.keys()))
                    return self._update_returning_result(q, list(record.values()))
                else:
                    return self._update_returning_result(table_name_or_sql_query, record)
            else:
                return self._update_returning_result(table_name_or_sql_query)
        except Exception:
            raise

    def _remove(self, sql_query, values=None):
        return self._update(sql_query, values)

    def _delete(self, sql_query, values=None):
        return self._remove(sql_query, values)

    def _select_dictresult(self, sql_query, values=None):
        if values:
            result = self._select(
                sql_query, values, cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            result = self._select(
                sql_query, cursor_factory=psycopg2.extras.RealDictCursor)
        return result

    def _select_getresult(self, sql_query, values=None):
        if values:
            result = self._select(
                sql_query, values, cursor_factory=psycopg2.extras.DictCursor)
        else:
            result = self._select(
                sql_query, cursor_factory=psycopg2.extras.DictCursor)
        return result

    def _select_tuple(self, sql_query, values):
        return self._select(sql_query, values)
