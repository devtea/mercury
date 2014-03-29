'''
stash.py
sta.sh specific uploader
'''
from __future__ import print_function

import json
import urllib
import urlparse
from sqlite3 import IntegrityError

import httplib2
import requests

import mercury.log

log = mercury.log.getLogger()
redirect_url = 'http://localhost/oauth2'


def authenticate(config):
    log.debug('Starting authentication setup.')

    client_id = config['services']['sta.sh']['client_id']
    client_secret = config['services']['sta.sh']['client_secret']

    config['stash'] = {}

    con = config['db'].connect()
    try:
        log.debug('Inserting service name into services')
        with con:
            con.execute("insert into services (service_name) values ('stash')")
    except IntegrityError:
        log.debug('Found service name in services')
        pass
    finally:
        con.close()

    stored_tokens = load_from_db(config)
    if stored_tokens:
        access_token = stored_tokens['access_token'][0]
        refresh_token = stored_tokens['refresh_token'][0]
        log.debug('Found stored auth: \n  access_token: %s \n  refresh_token: %s' % (access_token, refresh_token))
        try:
            refreshed_tokens = refresh_tokens(config, access_token, refresh_token)
        except UserWarning:
            log.error('Unable to refresh tokens, sta.sh may be unavailable.')
            #TODO we need to handle this better and either disable the plugin or exit completely
            return
        if refreshed_tokens:
            access_token = refreshed_tokens['access_token']
            refresh_token = refreshed_tokens['refresh_token']
    else:
        parameters = urllib.urlencode({'client_id': client_id, 'response_type': 'code', 'redirect_uri': redirect_url})
        print('Visit the following url and authorize. Paste the resultant redirected URL here:')
        redirect = raw_input("https://www.deviantart.com/oauth2/authorize?%s\n" % parameters)

        url = urlparse.urlparse(redirect)
        query = urlparse.parse_qs(url.query)
        code = query['code'][0]

        #We now have a code that can be used to request a token
        parameters = urllib.urlencode({'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'authorization_code', 'code': code, 'redirect_uri': redirect_url})
        resp, content = httplib2.Http(disable_ssl_certificate_validation=True).request('https://www.deviantart.com/oauth2/token?%s' % parameters)
        try:
            content = json.loads(content)
        except ValueError:
            log.error('Received bad JSON on token retrieval.')
            log.error(resp)
            log.error(content)
            return
        access_token = content['access_token']
        log.debug('New access token: %s' % access_token)
        refresh_token = content['refresh_token']
        log.debug('New refresh token: %s' % refresh_token)
    config = update_db(config, access_token, refresh_token)
    return config


def load_from_db(config):
    con = config['db'].connect()
    cur = con.cursor()
    try:
        cur.execute(
            """
            select token_value
            from service_auth
            where token_name = 'access_token'
            and service_id = (
                select service_id
                from services
                where service_name = 'stash') """)
        access_token = cur.fetchone()
        cur.execute(
            """
            select token_value
            from service_auth
            where token_name = 'refresh_token'
            and service_id = (
                select service_id
                from services
                where service_name = 'stash') """)
        refresh_token = cur.fetchone()
    finally:
        cur.close()
        con.close()
    if access_token and refresh_token:
        return {'access_token': access_token, 'refresh_token': refresh_token}
    else:
        return {}


def update_db(config, access_token, refresh_token):
    try:
        con = config['db'].connect()
        with con:
            con.execute(
                """
                delete from service_auth
                where service_id = (
                    select service_id
                    from services
                    where service_name = 'stash') """)
        with con:
            con.execute(
                """
                insert into service_auth (token_name, token_value, service_id)
                values(
                    'access_token',
                    ?,
                    (select service_id from services where service_name = 'stash')
                ) """, (access_token,))
        with con:
            con.execute(
                """
                insert into service_auth (token_name, token_value, service_id)
                values(
                    'refresh_token',
                    ?,
                    (select service_id from services where service_name = 'stash')
                ) """, (refresh_token,))
    finally:
        con.close()
    config['stash']['access_token'] = access_token
    config['stash']['refresh_token'] = refresh_token
    return config


def refresh_tokens(config, access_token, refresh_token):
    # First hit the placebo api endpoint to see if the access token is still
    # good.
    client_id = config['services']['sta.sh']['client_id']
    client_secret = config['services']['sta.sh']['client_secret']
    parameters = urllib.urlencode({'access_token': access_token})
    resp, content = httplib2.Http(disable_ssl_certificate_validation=True).request('https://www.deviantart.com/api/oauth2/placebo?%s' % parameters)
    if resp['status'] != '200':
        # If the token is not good anymore, refresh the token
        parameters = urllib.urlencode({'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'refresh_token', 'refresh_token': refresh_token, 'redirect_uri': redirect_url})
        resp, content = httplib2.Http(disable_ssl_certificate_validation=True).request('https://www.deviantart.com/oauth2/token?%s' % parameters)
        if resp['status'] == '200':
            try:
                content = json.loads(content)
            except ValueError:
                log.error('Received bad JSON on token refresh.')
                log.error(resp)
                log.error(content)
                raise UserWarning
            access_token = content['access_token']
            log.debug('New access token: %s' % access_token)
            refresh_token = content['refresh_token']
            log.debug('New refresh token: %s' % refresh_token)
            return {'access_token': access_token, 'refresh_token': refresh_token}
        else:
            log.error('Bad response when trying to refresh tokens.')
            log.error(resp)
            log.error(content)
            raise UserWarning
    else:
        log.debug('Current access token is still good.')
        return {}


def upload(config, path, title=None, description=None, tags=None, *args, **kwargs):
    log.debug('Upload called.')
    stored_tokens = load_from_db(config)
    access_token = stored_tokens['access_token'][0]
    refresh_token = stored_tokens['refresh_token'][0]
    submit_url = 'https://www.deviantart.com/api/oauth2/stash/submit?%s'

    try:
        refreshed_tokens = refresh_tokens(config, access_token, refresh_token)
    except UserWarning:
        log.error('Unable to refresh tokens, sta.sh may be unavailable ' +
                  'or another thread may have already fetched new tokens. ' +
                  'Pulling new tokens from DB and attempting upload anyway.')
        refreshed_tokens = None
        stored_tokens = load_from_db(config)
        access_token = stored_tokens['access_token'][0]
        refresh_token = stored_tokens['refresh_token'][0]

    if refreshed_tokens:
        access_token = refreshed_tokens['access_token']
        refresh_token = refreshed_tokens['refresh_token']

        config = update_db(config, access_token, refresh_token)
    parameters = {'access_token': access_token}
    if title:
        parameters['title'] = title
    if description:
        parameters['artist_comments'] = description
    if tags:
        parameters['keywords'] = tags
    parameters = urllib.urlencode(parameters)
    log.debug('Posting image to sta.sh')
    for attempt in range(2):
        resp = requests.post(
            submit_url % parameters,
            files={'file': open(path, 'rb')})
        log.debug('Finished upload.')
        if resp.status_code == 200:
            #exit for loop on successful upload
            break
        # May have failed due to another thread updating the tokens.
        stored_tokens = load_from_db(config)
        access_token = stored_tokens['access_token'][0]
        refresh_token = stored_tokens['refresh_token'][0]
    if resp.status_code != 200:
        #File did not upload correctly
        log.error('Error uploading file')
        log.error('status code %s' % resp.status_code)
        log.error(resp.text)
        return
    return resp
