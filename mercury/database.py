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
        con = sqlite3.connect(self._file)
        try:
            con.execute(
                '''
                create table if not exists services (
                    service_id    integer primary key not null,
                    service_name  text
                    ) ''')
            con.execute(
                '''
                create table if not exists service_auth (
                    token_name   text not null,
                    token_value  text not null,
                    service_id   integer not null,
                    foreign key(service_id) references services(service_id)
                    ) ''')
        finally:
            con.close()

    def connect(self):
        return sqlite3.connect(self._file)
