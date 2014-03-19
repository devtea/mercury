'''
tumblr.py
tumblr.com specific uploader
'''
from __future__ import print_function

import oauth2
import pytumblr
import urlparse

import mercury.log

log = mercury.log.getLogger()


def authenticate(config):
    log.debug('[tumblr] Starting authentication setup.')

    consumer_key = config[services][tumblr]['consumer_key']
    consumer_secret = config[services][tumblr]['client_secret']

    con = config['db'].connect()
    cur = con.cursor()
    try:
        '''TODO
        Create tables if not exists, services, auth tokens
        pull auth tokens from table auth_tokens
        '''
        oauth_token, oauth_token_secret = cur.fetchone()
    finally:
        cur.close()
        con.close()
    if oauth_token and oauth_token_secret:
        log.debug('[tumblr] Found stored auth: \n  oauth_token: %s \n  oauth_token_secret: %s' % (oauth_token, oauth_token_secret))

        ''' TODO
        initialize tumblr api object
        store in config
        return
        '''
    else:
        #Authorize with tumblr
        auth = oauth2.Consumer(key=consumer_key, secret=consumer_secret)
        client = oauth2.Client(auth)
        response, request_token = client.request('http://www.tumblr.com/oauth/request_token', "POST")
        request_token = urlparse.parse_qs(request_token)

        print('Visit the following url and authorize. Paste the resultant redirected URL here:')
        auth = raw_input(
            "http://www.tumblr.com/oauth/authorize?oauth_token=%s\n" % request_token['oauth_token'][0])
        url = urlparse.urlparse(auth)
        query = urlparse.parse_qs(url.query)
        oauth_verifier = query['oauth_verifier'][0]

        oauth_token = oauth2.Token(request_token['oauth_token'], request_token['oauth_token_secret'][0])
        oauth_token.set_verifier(oauth_verifier)
        client = oauth2.Client(auth, oauth_token)

        response, access_token = client.request('http://www.tumblr.com/oauth/access_token', 'POST')
        access_token = urlparse.parse_qs(access_token)

        # TODO store in Database
        # Create wrapper and store in config





    pass


def upload(config, path, title=None, description=None, *args, **kwargs):
    log.debug('[tumblr] Upload called.')

    pass
