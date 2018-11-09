# Copyright 2018 Shehriyar Qureshi <SShehriyar266@gmail.com>
import re
import datetime

from temporalite.intercept.create import CreateQueryBuilder
from temporalite.intercept.delete import DeleteQueryBuilder
from temporalite.intercept.insert import InsertQueryBuilder
from temporalite.intercept.select import TemporalSelectQueryBuilder
from temporalite.intercept.update import UpdateQueryBuilder
from temporalite.intercept.select_handler import SelectQueryHandler
from temporalite.query_execution.create import CreateQuery
from temporalite.query_execution.delete import DeleteQuery
from temporalite.query_execution.insert import InsertQuery
from temporalite.query_execution.select import (NormalSelectQuery,
                                                TemporalSelectQuery)
from temporalite.query_execution.update import UpdateQuery


class QueryHandler:
    def action_handler(connection, query):
        keyword_pattern = re.compile(r'(create|insert|update|delete|select)')
        keyword_matches = keyword_pattern.finditer(query)

        for match in keyword_matches:
            keyword_match = match.group(0)

        if keyword_match == "create":
            query_info = CreateQueryBuilder(query)
            CreateQuery.execute(connection, query_info)

        elif keyword_match == "insert":
            time_string = datetime.datetime.now().isoformat()
            query_info = InsertQueryBuilder(query, time_string)
            InsertQuery.execute(connection, query_info)

        elif keyword_match == "update":
            time_string = datetime.datetime.now().isoformat()
            query_info = UpdateQueryBuilder(query, connection, time_string)
            UpdateQuery.execute(connection, query_info)

        elif keyword_match == "delete":
            time_string = datetime.datetime.now().isoformat()
            query_info = DeleteQueryBuilder(query, time_string)
            DeleteQuery.execute(connection, query_info)

        elif keyword_match == "select":
            if SelectQueryHandler.is_temporal_query(query) is True:
                query_info = TemporalSelectQueryBuilder(query)
                return TemporalSelectQuery.execute(connection, query_info)

            else:
                return NormalSelectQuery.execute(connection, query)
