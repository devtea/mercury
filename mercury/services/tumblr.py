'''
tumblr.py
tumblr.com specific uploader
'''
import pytumblr

import mercury.log

log = mercury.log.getLogger()


def authenticate(config):
    log.debug('[tumblr] Starting authentication setup.')

    pass


def upload(config, path, title=None, description=None, *args, **kwargs):
    log.debug('[tumblr] Upload called.')

    pass
