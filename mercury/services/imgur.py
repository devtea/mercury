'''
imgur.py
imgur.com specific uploader
'''
from sqlite3 import IntegrityError

import pyimgur

import mercury.log

log = mercury.log.getLogger()


def authenticate(config):
    log.debug('[imgur] Starting authentication setup.')
    if not config['services']['imgur']['client_id'] and not config['services']['imgur']['client_secret']:
        log.error('Service "imgur" exists in config but is missing client_id or client_secret')
        # TODO return or raise?
        return False

    if 'imgur' not in config:
        config['imgur'] = {}

    if 'api_object' not in config['imgur']:
        con = config['db'].connect()
        cur = con.cursor()
        try:
            try:
                log.debug('[imgur] Inserting service name into services')
                with con:
                    con.execute("insert into services (service_name) values ('imgur')")
            except IntegrityError:
                log.debug('[imgur] Found service name in services')
                pass
            cur.execute(
                """
                select token_value
                from service_auth
                where token_name = 'refresh_token'
                and service_id = (
                    select service_id
                    from services
                    where service_name = 'imgur') """)
            auth_token = cur.fetchone()
        finally:
            cur.close()
            con.close()
        if auth_token:
            log.debug('[imgur] Found stored refresh_token: %s' % auth_token)
            # Setup the Imgur object with a previous refresh token
            api_wrapper = pyimgur.Imgur(
                client_id=config['services']['imgur']['client_id'],
                client_secret=config['services']['imgur']['client_secret'],
                refresh_token=auth_token)
            # Have had some issues with refresh tokens expiring - may see error
            # here if it happens again.
            api_wrapper.refresh_access_token()
        else:
            log.debug('[imgur] No stored refresh_token.')
            # Setup the imgur object as new
            api_wrapper = pyimgur.Imgur(
                client_id=config['services']['imgur']['client_id'],
                client_secret=config['services']['imgur']['client_secret'])
            url = api_wrapper.authorization_url('pin')
            pin = raw_input("Visit %s to authenticate the application. Enter the pin here: " % url)
            log.debug('[imgur] Got pin %s' % pin)
            # TODO handle bad pins
            _, refresh_token = api_wrapper.exchange_pin(pin)
            log.debug('[imgur] Got refresh token: %s' % refresh_token)

            # save refresh_token in database
            try:
                con = config['db'].connect()
                with con:
                    con.execute(
                        """
                        insert into services (service_name)
                            values ('imgur') """)
                with con:
                    con.execute(
                        """
                        insert into service_auth (service_id, token_name, token_value)
                        values (
                            (select service_id from services where service_name = 'imgur'),
                            'refresh_token',
                            ?) """, (refresh_token,))
            finally:
                con.close()
        config['imgur']['api_object'] = api_wrapper
    return config


def upload(config, path, title=None, description=None, *args, **kwargs):
    log.debug('[imgur] Upload called.')
    api = config['imgur']['api_object']

    # First things first, let's work around a limitation in the api wrapper. it
    # doesn't refresh tokens correctly it seems, and the refresh token is now
    # sent back as a new refresh_token on each refresh. This new refresh_token
    # needs to be stored in our database and refreshed each time we want to
    # upload an image. This could be overkill if we're uploading images more
    # than once an hour so a time limiter would be nice.
    api.refresh_access_token()
    # save refresh_token in database
    try:
        con = config['db'].connect()
        with con:
            con.execute(
                """
                update service_auth
                set token_value = ?
                where token_name = 'refresh_token'
                and service_id = (
                    select service_id
                    from services
                    where service_name = 'imgur') """, (api.refresh_token,))
    finally:
        con.close()
    # That housekeeping aside, we can move on to our uploading.

    # TODO Handle errors
    results = api.upload_image(path)
    if results:
        log.info('[imgur] image uploaded.')
        log.info('[imgur] link: %s' % results.link)
        log.info('[imgur] delete hash: %s' % results.deletehash)
    return results
