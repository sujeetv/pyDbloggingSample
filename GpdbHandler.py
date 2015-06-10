#! /usr/bin/python

import psycopg2
import logging

class GpdbHandler(logging.Handler):
    """
    Write logs to GPDB database
    """
    def __init__(self, connecton_string=None):
        """
        :param connecton_string: Something like "dbname=test user=gpadmin"
        :return:
        """
        logging.Handler.__init__(self)
        self.connect_string = connecton_string
        # connect to gpdb database:
        conn = psycopg2.connect()
        self.cur = conn.cursor()

    def emit(self, record):
        """
        We will write the record here to gpdb
        :param record:
        :return:
        """

        audit_sql = " select * from gvl_dea.gvl_dea_audit_log({batch_id}::BIGINT," \
                    "{process_nm}::character varying(30),null::character varying(30)," \
                    "{timestamp}::TIMESTAMP," \
                    "null::TIMESTAMP, " \
                    "null::numeric," \
                    "0::NUMERIC," \
                    "1::boolean);"

        # todo: fill the parameters for the above query from the record object

        # call the udf
        self.cur.execute(audit_sql)