'''
config.py
Module to handle YAML config loading and validation.
'''
import errno
import os

import yaml

import mercury.log

log = mercury.log.getLogger()


def getConfig(file):
    '''Parses given file and returns a config dictionary'''
    log.debug('Config File: %s' % file)
    try:
        with open(file) as c:
            config = yaml.safe_load(c)
    except IOError as e:
        if e.errno == errno.ENOENT:  # Doesn't Exist
            log.error('Config file not found')
            return None
        elif e.errno == errno.EACCES:  # Access Denied
            log.error('Cannot access config file')
            return None
        elif e.errno == errno.EISDIR:  # Is directory
            log.error('Config is a directory')
            return None
        else:
            raise
    except yaml.scanner.ScannerError:  # bad yaml
            log.error('Malformed YAML')
            return None
    log.debug('Config values: %s' % config)
    return validate(config)


def validate(config):
    #TODO run through RX for structural validation

    # Verify watched directory is ready
    path = config['watched_folder']
    try:
        if not os.path.isdir(path):
            try:
                os.makedirs(path)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    log.error('Watch path is a file.')
                    return None
            log.info('Created watch directory at %s' % path)
    except TypeError:
        log.error('Watch folder path must be specified!')
        return None

    # Verify archive directory is ready
    if not config['archive_folder']:
        #TODO this won't work on windows
        log.info('No archive folder provided, using default.')
        config['archive_folder'] = '%s/archive' % config['watched_folder']
    path = config['archive_folder']
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == errno.EEXIST:
                log.error('Archive path is a file.')
                return None
        log.info('Created archive directory at %s' % path)
    return config


if __name__ == '__main__':
    print __doc__
