#!/usr/bin/env python

import shutil
import sys
import tempfile

import mercury.config
import mercury.database
import mercury.dispatcher
import mercury.log
import mercury.services
import mercury.watcher

_configFile = 'config.yaml'
_tmp = None

log = mercury.log.getLogger()
log.info('Initializing project')


def main(args):
    global _tmp
    config = mercury.config.getConfig(_configFile)
    if not config:
        log.critical('Invalid config.')
        sys.exit(1)

    # Initialize tmp directory
    _tmp = tempfile.mkdtemp()
    config['tmp'] = _tmp

    # Initialize database
    db = mercury.database.db(config)
    config['db'] = db
    # TODO threading safety?

    # Initialize services
    for s in mercury.services.registry:
        #TODO Check for return values for errors / exceptions
        new_config = mercury.services.registry[s].authenticate(config)
        if new_config:
            config = new_config

    mercury.dispatcher.setup_resize_workers(config)
    mercury.dispatcher.setup_upload_workers(config)
    mercury.watcher.startWatcher(config, config['watched_folder'], config['check_interval'])


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        try:
            shutil.rmtree(_tmp)
        except TypeError:
            pass  # Probably a None from no tmp being created.
        log.warning('Caught keyboard interrupt. Closing.')
        sys.exit(1)
