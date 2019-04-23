# Copyright 2018 Shehriyar Qureshi <SShehriyar266@gmail.com>
"""
Aim for this file is to enable historia to act in a similar way to sqlite3
by having wrappers for sqlite3's functions.
"""
import sqlite3

from historia.intercept.create import CreateQueryBuilder
from historia.intercept.query_handler import QueryHandler
from historia.query_execution.create import CreateQuery


class Connection:
    """
    Act similar to sqlite3's connect class.
    """

    def __init__(self, database_file, sqlite_connection=None):
        """
        The basic connection object which will be used for executing queries.
        It is a wrapper for the sqlite3.connect function.
        """
        self.sqlite_connection = sqlite_connection
        self.database_file = database_file
        self.verify_file_path()

    def verify_file_path(self):
        """
        Verify the connection was successfully established.
        """
        # raises sqlite3's exception if file path not valid
        self.sqlite_connection = sqlite3.connect(self.database_file)

    def execute(self, args):
        """
        Wrapper for sqlite3's execute() and the point where we do our
        custom operations.
        """
        return QueryHandler.action_handler(self.sqlite_connection,
                                           str.lower(args))

    def commit(self):
        """
        Wrapper function for sqlite3's commit().
        """
        self.sqlite_connection.commit()

    def close(self):
        """
        Wrapper function for sqlite3's close().
        """
        self.sqlite_connection.close()

    def create_history_tables(self):
        """
        Create a history table for every table in the connection that currently
        does not have one.
        """
        # get all the table names
        query = self.sqlite_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        query_output = query.fetchall()
        table_names = []
        # We need the table schemas if we are going to create history tables
        table_schemas = []

        # add the table names to list
        for i in query_output:
            table_names.append(i[0])

        for table in table_names:
            # leave out already created history tables
            if table[-8:] == '_history':
                pass
            else:
                table_name = [table + "_history"]
                query = self.sqlite_connection.execute(
                    "select sql from sqlite_master where name=?", table_name)
                query_output = query.fetchone()
                # check if table has a history table
                if query_output is None:
                    query = self.sqlite_connection.execute(
                        "select sql from sqlite_master where name=?", [table])
                    table_schemas.append(str.lower(query.fetchone()[0]))

        for i in table_schemas:
            query_info = CreateQueryBuilder(i)
            CreateQuery.execute(self.sqlite_connection, query_info)
