'''
tumblr.py
tumblr.com specific uploader
'''
from __future__ import print_function

import oauth2
import pytumblr
import urlparse
from sqlite3 import IntegrityError

import mercury.log

log = mercury.log.getLogger()


def authenticate(config):
    log.debug('[tumblr] Starting authentication setup.')

    consumer_key = config['services']['tumblr']['consumer_key']
    consumer_secret = config['services']['tumblr']['consumer_secret']

    if 'tumblr' not in config:
        config['tumblr'] = {}

    if 'api_object' not in config['tumblr']:
        con = config['db'].connect()
        cur = con.cursor()
        try:
            try:
                log.debug('[tumblr] Inserting service name into services')
                with con:
                    con.execute("insert into services (service_name) values ('tumblr')")
            except IntegrityError:
                log.debug('[tumblr] Found service name in services')
                pass
            cur.execute(
                """
                select token_value
                from service_auth
                where token_name = 'oauth_token'
                and service_id = (
                    select service_id
                    from services
                    where service_name = 'tumblr') """)
            oauth_token = cur.fetchone()
            cur.execute(
                """
                select token_value
                from service_auth
                where token_name = 'oauth_token_secret'
                and service_id = (
                    select service_id
                    from services
                    where service_name = 'tumblr') """)
            oauth_token_secret = cur.fetchone()
        finally:
            cur.close()
            con.close()
    if oauth_token and oauth_token_secret:
        log.debug('[tumblr] Found stored auth: \n  oauth_token: %s \n  oauth_token_secret: %s' % (oauth_token, oauth_token_secret))
        api_wrapper = pytumblr.TumblrRestClient(consumer_key, consumer_secret, oauth_token[0], oauth_token_secret[0])
    else:
        #Authorize with tumblr
        auth = oauth2.Consumer(key=consumer_key, secret=consumer_secret)
        client = oauth2.Client(auth)
        response, request_token = client.request('http://www.tumblr.com/oauth/request_token', "POST")
        log.debug('[tumblr] request_token response: %s' % response)
        log.debug('[tumblr] request_token data: %s' % request_token)
        request_token = urlparse.parse_qs(request_token)
        log.debug('[tumblr] request_token: %s' % request_token)

        print('Visit the following url and authorize. Paste the resultant redirected URL here:')
        redirect_url = raw_input(
            "http://www.tumblr.com/oauth/authorize?oauth_token=%s\n" % request_token['oauth_token'][0])
        url = urlparse.urlparse(redirect_url)
        query = urlparse.parse_qs(url.query)
        oauth_verifier = query['oauth_verifier'][0]
        log.debug('[tumblr] oauth_verifier: %s' % oauth_verifier)
        oauth_token = oauth2.Token(request_token['oauth_token'], request_token['oauth_token_secret'][0])
        log.debug('[tumblr] full oauth_token: %s' % oauth_token)
        oauth_token.set_verifier(oauth_verifier)
        log.debug('[tumblr] oauth_token: %s' % oauth_token)

        client = oauth2.Client(auth, oauth_token)
        response, access_token = client.request('http://www.tumblr.com/oauth/access_token', 'POST')
        log.debug('[tumblr] respone: %s' % response)
        log.debug('[tumblr] full access_token: %s' % access_token)
        access_token = urlparse.parse_qs(access_token)
        log.debug('[tumblr] oauth_token: %s' % access_token['oauth_token'][0])
        log.debug('[tumblr] oauth_token: %s' % access_token['oauth_token_secret'][0])
        api_wrapper = pytumblr.TumblrRestClient(
            consumer_key,
            consumer_secret,
            access_token['oauth_token'][0],
            access_token['oauth_token_secret'][0])

        # Save tokens in database
        try:
            con = config['db'].connect()
            with con:
                con.execute(
                    """
                    delete from service_auth
                    where service_id = (
                        select service_id
                        from services
                        where service_name = 'tumblr') """)
            with con:
                con.execute(
                    """
                    insert into service_auth (token_name, token_value, service_id)
                    values(
                        'oauth_token',
                        ?,
                        (select service_id from services where service_name = 'tumblr')
                    ) """, (access_token['oauth_token'][0],))
            with con:
                con.execute(
                    """
                    insert into service_auth (token_name, token_value, service_id)
                    values(
                        'oauth_token_secret',
                        ?,
                        (select service_id from services where service_name = 'tumblr')
                    ) """, (access_token['oauth_token_secret'][0],))
        finally:
            con.close()
    config['tumblr']['api_object'] = api_wrapper
    return config


def upload(config, path, title=None, description=None, *args, **kwargs):
    log.debug('[tumblr] Upload called.')
    api = config['tumblr']['api_object']
    log.debug('[tumblr] posting image to %s' % config['services']['tumblr']['blog_url'])
    post = api.create_photo(config['services']['tumblr']['blog_url'], data=path)
    log.debug('[tumblr] posting finished')
    if post:
        log.debug('[tumblr] post: %s' % post)
    else:
        log.debug('[tumblr] no results')
    return post
