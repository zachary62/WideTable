from abc import ABC, abstractmethod
from .aggregator import *
import time


class ExecutorException(Exception):
    pass


def ExecutorFactory(con=None):
    # By default if con is not specified, user uses Pandas dataframe
    if con is None:
        try:
            import duckdb
        except:
            raise ExecutorException("To support Pandas dataframe, please install duckdb.")
        con = duckdb.connect(database=':memory:')
        return PandasExecutor(con)
    elif issubclass(type(con), Executor):
        return con
    elif type(con).__name__ == 'DuckDBPyConnection':
        return DuckdbExecutor(con, debug=True)
    else:
        raise ExecutorException("Unknown connector with type " + type(con).__name__)
        
        
class Executor(ABC):
    '''Assume input data are csvs'''

    def __init__(self):
        self.view_id = 0
        self.prefix = 'm_'

    def get_next_name(self):
        name = self.prefix + str(self.view_id)
        self.view_id += 1
        return name

    def add_integer_column(self, table: str, column: str, initial_value: int):
        pass


class DuckdbExecutor(Executor):
    def __init__(self, conn, debug=False):
        super().__init__()
        self.conn = conn
        self.debug = debug
        
    def get_schema(self, table):
        # duckdb stores table info in [cid, name, type, notnull, dflt_value, pk]
        table_info = self._execute_query('PRAGMA table_info(' + table + ')')
        return [x[1] for x in table_info]
        
    def add_table(self, table: str, table_address):
        if table_address is None:
            raise ExecutorException("Please pass in the csv file location")
        self.conn.execute(f"CREATE TABLE {table} AS SELECT * FROM '{table_address}'")

    def delete_table(self, table: str):
        self.conn.execute(f"DROP TABLE IF EXISTS {table};")

    def add_integer_column(self, table: str, column: str, initial_value: int):
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} integer")
        self.conn.execute(f"UPDATE {table} SET {column}={initial_value}")

    def delete_table(self, table: str):
        self.check_table(table)
        sql = 'DROP TABLE IF EXISTS ' + table + ';\n'
        self._execute_query(sql)

    def check_table(self, table):
        if not table.startswith(self.prefix):
            raise Exception("Don't modify user tables!")
    
    def update_query(self,
                     update_expression,
                     table,
                     select_conds: list = []):
        self.check_table(table)
        sql = "UPDATE " + table + " SET " + update_expression + " \n"
        if len(select_conds) > 0:
            sql += "WHERE " + " AND ".join(select_conds) + "\n"
        self._execute_query(sql)
    
    # mode = 1 will write the query result to a table and return table name
    # mode = 2 will create the query as view and return view name
    # mode = 3 will execute the query and return the result
    # mode = 4 will create the sql query and return the query string (for nested query)
    # mode = 5 will execute the query and return the result as pandas dataframe
    def execute_spja_query(self, 
                           # By default, we select all
                           aggregate_expressions: dict = {None: ('*', Aggregator.IDENTITY)},
                           from_tables: list = [],
                           select_conds: list = [],
                           group_by: list = [], 
                           window_by: list = [],
                           order_by: str = None,
                           limit: int = None,
                           sample_rate: float = None,
                           replace: bool = True,
                           mode: int = 4,
                           table_name: str = None):

        spja = self.spja_query(aggregate_expressions=aggregate_expressions,
                               from_tables=from_tables,
                               select_conds=select_conds,
                               group_by=group_by, 
                               window_by=window_by,
                               order_by=order_by,
                               limit=limit,
                               sample_rate=sample_rate)
        
        if mode == 1:
            if table_name is None:
                name_ = self.get_next_name()
            else:
                name_ = table_name
            entity_type_ = 'TABLE '
            sql = 'CREATE ' + ('OR REPLACE ' if replace else '') + entity_type_  + name_ + ' AS '
            sql += spja
            self._execute_query(sql)
            return name_
        
        elif mode == 2:
            name_ = self.get_next_name()
            entity_type_ = 'VIEW '
            sql = 'CREATE ' + ('OR REPLACE ' if replace else '') + entity_type_  + name_ + ' AS '
            sql += spja
            self._execute_query(sql)
            return name_
        
        elif mode == 3:
            return self._execute_query(spja)
        
        elif mode == 4:
            sql = '(' + spja + ')'
            return sql
        
        elif mode == 5:
            return self._execute_query(spja, pandas= True)
        
        else:
            raise ExecutorException('Unsupported mode for query execution!')
        
    
    
    def spja_query(self, 
                   aggregate_expressions: dict,
                   from_tables: list = [],
                   select_conds: list = [],
                   window_by: list = [],
                   group_by: list = [], 
                   order_by: str = None,
                   limit: int = None,
                   sample_rate: float = None,
                   ):
        
        parsed_aggregate_expressions = []
        for target_col, aggregation_spec in aggregate_expressions.items():
            para, agg = aggregation_spec
            parsed_aggregate_expressions.append(parse_agg(agg, para) \
                                + (' OVER joinboost_window ' if len(window_by) > 0 and is_agg(agg) else '')\
                                + (' AS ' + target_col if target_col is not None else ''))
                                                
        
        sql = 'SELECT ' + ', '.join(parsed_aggregate_expressions) + '\n'
        sql += "FROM " + ",".join(from_tables) + '\n'
        
        if len(select_conds) > 0:
            sql += "WHERE " + " AND ".join(select_conds) + '\n'
        if len(window_by) > 0:
            sql += 'WINDOW joinboost_window AS (ORDER BY ' + ','.join(window_by) + ')\n'
        if len(group_by) > 0:
            sql += "GROUP BY " + ",".join(group_by) + '\n'
        if order_by is not None:
            sql += 'ORDER BY ' + ",".join(order_by) + '\n'
        if limit is not None:
            sql += 'LIMIT ' + str(limit) + '\n'
        if sample_rate is not None:
            sql += 'USING SAMPLE ' + str(sample_rate*100) + ' %\n'
        return sql

    def set_query(self, operation, expr1, expr2):
        return f'({expr1} {operation} {expr2})'

    def _execute_query(self, q, pandas=False):
        start_time = time.time()
        if self.debug:
            print(q)
        self.conn.execute(q)
        elapsed_time = time.time() - start_time
        
        if self.debug:
            print(elapsed_time)
        
        result = None
        try:
            if not pandas:
                result = self.conn.fetchall()
            else:
                result = self.conn.fetchdf()
            if self.debug:
                print(result)
        except Exception as e:
            print(e)
        return result


class PandasExecutor(DuckdbExecutor):
    def add_table(self, table: str, table_address):
        if table_address is None:
            raise ExecutorException("Please pass in the pandas dataframe!")
        self.conn.register(table, table_address)

    def add_integer_column(self, table: str, initial_value: int):
        pass
