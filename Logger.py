#! /usr/bin/python
import os
import logging
import settings   # alternatively from wherever import settings
import GpdbHandler

class Logger(object):

    def __init__(self, name):
        name = name.replace('.log','')
        logger = logging.getLogger('log_namespace.%s' % name)    # log_namespace can be replaced with your namespace
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            # We will add 2 handlers here. FileHandler and GpdbHandler. Handler will decide the destination
            # where the record will be logged

            # First we will add the File Handler
            file_name = os.path.join(settings.LOGGING_DIR, '%s.log' % name)    # usually I keep the LOGGING_DIR defined in some global settings file
            handler = logging.FileHandler(file_name)
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s %(message)s')
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)

            # Second we will add the GpdbHandler
            dbHandler = GpdbHandler.GpdbHandler(connecton_string=settings.GPDB_CONNECTION_STRING)
            logger.addHandler(dbHandler)

        self._logger = logger

    def get(self):
        return self._logger