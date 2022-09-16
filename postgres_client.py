

from sqlalchemy import create_engine

class PostgresClient:
    def __init__(self, postgresURL ) -> None:
        self.engine = create_engine(postgresURL)

    def write_to_postgres (self, df, chunksize = 2000 ) -> None:
        def insert_do_nothing_on_conflicts(sqltable, conn, keys, data_iter):
            """
            Execute SQL statement inserting data, and do nothing on conflicting primary keys

            Parameters
            ----------
            sqltable : pandas.io.sql.SQLTable
            conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
            keys : list of str
                Column names
            data_iter : Iterable that iterates the values to be inserted
            """
            from sqlalchemy.dialects.postgresql import insert
            from sqlalchemy import table, column
            columns = []
            for c in keys:
                columns.append(column(c))

            if sqltable.schema:
                table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
            else:
                table_name = sqltable.name

            mytable = table(table_name, *columns)

            insert_stmt = insert(mytable).values(list(data_iter))
            do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['vionlabs_id'])

            conn.execute(do_nothing_stmt)
        df.to_sql('reconciled', if_exists='append', con= self.engine, index=False, method=insert_do_nothing_on_conflicts, chunksize=chunksize)

