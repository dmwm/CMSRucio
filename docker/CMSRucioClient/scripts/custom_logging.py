#! /bin/env python
"""
Module adding some custom logging levels.
"""
import logging

def logger_method(lvl):
    """
    Generates logger method
    """
    def clogger(message, *args, **kws):
        """
        logger function
        """
        logging.log(lvl, message, *args, **kws)

    return clogger

logging.VVERBOSE = 15
logging.vverbose = logger_method(15)
logging.addLevelName(15, 'VVERBOSE')

logging.VERBOSE = 17
logging.verbose = logger_method(17)
logging.addLevelName(17, 'VERBOSE')

logging.VNOTICE = 23
logging.vnotice = logger_method(23)
logging.addLevelName(23, 'VNOTICE')

logging.NOTICE = 25
logging.notice = logger_method(25)
logging.addLevelName(25, 'NOTICE')

logging.DRY = 27
logging.dry = logger_method(27)
logging.addLevelName(27, 'DRY')

logging.FAILURE = 33
logging.failure = logger_method(33)
logging.addLevelName(33, 'FAILURE')

logging.SUMMARY = 35
logging.summary = logger_method(35)
logging.addLevelName(35, 'SUMMARY')

CUSTOM_FMT = '%%(asctime)s;%%(levelname)s;%%(process)s;%%(name)s;%(label)s;%%(message)s'
CUSTOM_DATE_FMT = '%Y:%m:%d:%H:%M:%S;%s'


def my_fmt(**kwargs):
    """
    Just prepares the custom format
    :args:    array or dictionnary for the parameters
    :format:  format string, default is CUSTOM_FMT_HEAD
    :dformat: date format, default is CUSTOM_DATE_FMT
    """

    if 'format' not in kwargs:
        kwargs['format'] = CUSTOM_FMT

    if 'datefmt' not in kwargs:
        kwargs['datefmt'] = CUSTOM_DATE_FMT

    if logging.getLogger().handlers:

        oldhandler = logging.getLogger().handlers[0]

        oldhandler.flush()
        oldhandler.close()

        if 'sub_handler' in oldhandler.__dict__:
            oldhandler = oldhandler.__dict__['sub_handler']

        exists = True
    else:
        exists = False

    #pylint: disable=redefined-variable-type
    if exists and 'baseFilename' in oldhandler.__dict__:
        handler = logging.FileHandler(oldhandler.__dict__['baseFilename'])
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        fmt=kwargs['format'] % kwargs,
        datefmt=kwargs['datefmt']
    )
    handler.setFormatter(formatter)
    logging.getLogger().handlers = [handler]


def my_logfile(logs=None):
    """
    Set logfile
    :logs:  log file name
    """

    #pylint: disable=redefined-variable-type
    if logs:
        handler = logging.FileHandler(logs)
    else:
        handler = logging.StreamHandler()

    logging.getLogger().handlers = [handler]


def my_lvl(level):
    """
    Redefine logging level
    """
    logging.getLogger().setLevel(
        getattr(logging, level)
    )


def get_levels():
    """
    Get ordered list of levels
    """
    levels = []
    #pylint: disable=protected-access
    for key, value in sorted(logging._levelNames.items()):
        if isinstance(value, basestring):
            levels.append({'name': value, 'level': key})

    return levels


logging.my_fmt = my_fmt
logging.my_logfile = my_logfile
logging.my_lvl = my_lvl
logging.get_levels = get_levels
