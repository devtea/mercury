import sqlite3
import os

import mercury.log

log = mercury.log.getLogger()


class db(object):
    def __init__(self, config):
        if not config['database_file']:
            self._file = os.path.abspath(os.path.expanduser(config['watched_folder'])) + \
                '/mercury.sqlite'
        else:
            self._file = os.path.abspath(os.path.expanduser(config['database_file']))
        try:
            self._db = sqlite3.connect(self._file)
        except sqlite3.OperationalError:
            log.error('Unable to open database!')
            raise

    def connect(self):
        return sqlite3.connect(self._file)
