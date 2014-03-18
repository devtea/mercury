'''
watcher.py
Module to set up directory watching for file creation events.
'''
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import mercury.log
import mercury.dispatcher

log = mercury.log.getLogger()


class customHandler(FileSystemEventHandler):
    _config = None

    def on_created(self, event):
        if event.src_path.endswith('sqlite-journal'):
            return
        log.debug('CustomHandler triggered')
        if event.is_directory:
            log.debug('Event was a directory. Ignoring.')
            return
        #time.sleep(60)  # Wait a bit to make sure the file's been written
        time.sleep(10)
        #TODO file may be gone, handle quietly
        log.debug('Time to work with the file at %s' % event.src_path)
        mercury.dispatcher.dispatch(self._config, event.src_path)

    @property
    def config(self):
        #TODO see if you can remove this without breaking the attribute
        return self._config

    @config.setter
    def config(self, value):
        self._config = value


def startWatcher(config, path, interval):
    event_handler = customHandler()
    event_handler.config = config
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(interval)
    except:
        observer.stop()
        raise
    observer.join()
