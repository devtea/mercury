'''
dispatcher.py
tasked to create queue items for each file found
'''
import multiprocessing
import os.path
import shutil
import threading
import tempfile
import traceback

from PIL import Image

import mercury.services
import mercury.log

log = mercury.log.getLogger()

_upload = multiprocessing.Queue()

# A list of all the dynamically created resize worker objects
resizeworkers = []


def img_pickle(image):
    '''Return a pickleable data format for multiprocessing'''
    pickle = {
        'data': image.tobytes(),
        'size': image.size,
        'mode': image.mode
    }
    return pickle


def img_unpickle(pickled):
    '''Return a pillow image from pickled data.'''
    return Image.frombytes(**pickled)


def dispatch(config, file):
    global resizeworkers

    # Assume for now we have at least one service to send to
    log.debug('Dispatcher called for file: %s' % file)
    log.debug('I got a config file with %i elements.' % len(config))

    # Opening the image w/ Pillow should be all the filtering we need
    try:
        img = Image.open(file)
    except IOError:
        log.debug('Image file not appropriate for processing: %s' % file)
        log.debug(traceback.format_exc())
        return

    if 'archive_folder' in config and config['archive_folder']:
        dest = os.path.abspath(config['archive_folder'])
    else:
        dest = os.path.dirname(os.path.abspath(config['watched_folder']))

    # Check for collisions and rename appropriately
    newfile = dest.rstrip('/') + '/' + os.path.basename(file)
    if os.path.exists(newfile):
        log.debug('File already exists in archive location.')
        parts = os.path.basename(newfile).split('.')
        parts.insert(-1, 1)
        while os.path.exists(dest.rstrip('/') + '/' + '.'.join([str(i) for i in parts])):
            parts[-2] += 1
        newfile = dest.rstrip('/') + '/' + '.'.join([str(i) for i in parts])
        log.debug('New file location: %s' % newfile)

    shutil.move(file, newfile)
    file = newfile

    for i in resizeworkers:
        i.put(img_pickle(img.copy()), str(img.format))


def setup_resize_workers(config):
    global resizeworkers
    resolutions = {}

    log.debug('Starting setup_resize_workers')

    # Grab global resolutions
    xres = -1
    if 'max_xres' in config:
        if config['max_xres']:
            xres = config['max_xres']
    yres = -1
    if 'max_yres' in config:
        if config['max_yres']:
            yres = config['max_yres']
    default_res = (xres, yres)
    log.debug('Default resolution of %s set.' % str(default_res))

    for service in config['services']:
        # Grab resolutions from config and override with global as appropriate
        if 'xres' in config['services'][service]:
            xres = config['services'][service]['xres']
        if 'yres' in config['services'][service]:
            yres = config['services'][service]['yres']
        if not xres:
            xres = default_res[0]
        if not yres:
            yres = default_res[1]
        res = (xres, yres)

        # Build a unique set of resolutions for workers
        if res not in resolutions:
            resolutions[res] = []
        resolutions[res].append(service)

    log.debug('Services found for resize workers: ')
    log.debug(resolutions)

    for r in resolutions:
        log.debug('Creating rescaling process for %s' % str(r))
        resizeworkers.append(ResizeWorker(config, r, resolutions[r]))


def setup_upload_workers(config):
    log.debug('Setting up upload workers.')
    # TODO How will this handle services that are configured but not supported?
    for i in config['services']:
        UploadWorker(config)


class ResizeWorker(object):
    _services = None
    _size = None
    _queue = None
    _image = None
    _config = None
    _format = None

    def __init__(self, config, size, services):
        super(ResizeWorker, self).__init__()
        '''
        if either part of self._size is -1, treat it as 9,999,999 instead
        if both are, skip resizing and write file as is
        '''
        if bool(size[0] == -1) != bool(size[1] == -1):
            if size[0] == -1:
                self._size = (9999999, size[1])
            else:
                self._size = (size[0], 9999999)
        elif size[0] == -1 and size[1] == -1:
            self._size = None
        else:
            self._size = size

        self._services = services
        self._queue = multiprocessing.Queue()
        self._config = config

        newprocess = multiprocessing.Process(target=self._worker)
        newprocess.daemon = True
        newprocess.start()
        log.debug('Resize worker %s for started at size %s for service(s) %s' % (
            newprocess.name, self._size, str(self._services)))

    def _worker(self):
        try:
            while True:
                self._image, self._format = self._queue.get()
                self._image = img_unpickle(self._image)
                log.debug('[%s: %s] got something to resize, working with %s' % (
                    multiprocessing.current_process().name, self._services, self._image))

                # Only resize if there is a set size. Else work with full image
                if self._size:
                    log.debug('[%s: %s] Resizing to %s' % (
                        multiprocessing.current_process().name, self._services, self._size))
                    self._image.thumbnail(self._size, Image.ANTIALIAS)
                    log.debug('[%s: %s] done with resizing %s.' % (
                        multiprocessing.current_process().name, self._services, self._image))

                log.debug('Pushing to upload queue.')
                if len(self._services) == 1:
                    log.debug('[%s] Pushing image at %s for %s onto upload queue.' % (
                        multiprocessing.current_process().name, self._image, self._services[0]))
                    _upload.put((img_pickle(self._image), self._format, self._services[0]))
                else:  # If there's more than one, push copies
                    for s in self._services:
                        log.debug('[%s] Pushing copy of image at %s for %s onto upload queue.' % (
                            multiprocessing.current_process().name, self._image, s))
                        _upload.put((img_pickle(self._image.copy()), self._format, s))
                self._image = None
        except KeyboardInterrupt:
            #TODO may want to revisit this and allow process to finish gracefully
            return

    @property
    def services(self):
        return self._services

    @property
    def size(self):
        return self._size

    @property
    def queue(self):
        return self._queue

    def put(self, file, format):
        self._queue.put((file, format))


class UploadWorker(object):
    _servicename = None
    _image = None
    _format = None
    _path = None
    _config = None

    def __init__(self, config):
        super(UploadWorker, self).__init__()
        self._config = config
        newthread = threading.Thread(target=self._worker)
        newthread.daemon = True
        newthread.start()
        log.debug('Upload worker %s started.' % newthread.name)

    def _worker(self):
        try:
            while True:
                self._image, self._format, self._servicename = _upload.get()
                self._image = img_unpickle(self._image)

                log.debug('[%s] got something to upload, working with %s' % (
                    threading.current_thread().name, self._image))

                # check to see if specified service is in our list
                if self._servicename not in mercury.services.registry:
                    log.warning('Configured service %s is not loaded. Cannot upload.' % self._servicename)
                    continue

                # Open a temporary file and write the image to it
                with tempfile.NamedTemporaryFile(
                        suffix='.%s' % self._format.lower(),
                        dir=self._config['tmp'],
                        delete=False) as f:
                    self._path = f.name
                    log.debug('[%s] Saving temp file %s' % (
                        threading.current_thread().name, self._path))
                    self._image.save(f, self._format)

                log.debug('Calling %s upload()' % self._servicename)
                #TODO catch exceptions
                mercury.services.registry[self._servicename].upload(self._config, self._path)

                log.debug('[%s] Done with uploading image to %s' % (
                    threading.current_thread().name, self._servicename))

                log.debug('[%s] Removing temporary file %s' % (
                    threading.current_thread().name, self._path))
                os.remove(self._path)
        except KeyboardInterrupt:
            #TODO may want to revisit this and allow process to finish gracefully
            return
