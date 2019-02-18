#! /bin/env python
"""
Service synchronizing the sites
"""

from __future__ import absolute_import, division, print_function

import functools
import time
import argparse

def timer(func):
    """
    Create the wrapper.
    """
    @functools.wraps(func)
    def timer_wrapper(*args, **kwargs):
        """
        Wrapper for workers for instrumentation.
        """

        ftime = {'start': time.time()}

        ret = func(*args, **kwargs)

        ftime['end'] = time.time()
        ftime['etime'] = ftime['end'] - ftime['start']

        if isinstance(ret, dict) and 'timing' in ret:
            ret['timing'].update({func.__name__: ftime})
            return ret
        else:
            return {
                'return': ret,
                'timing': {func.__name__: ftime}
            }

    return timer_wrapper

def get_timing(ret, timing):
    """
    Helper
    """
    timing.update(ret.pop('timing'))
    if 'return' in ret:
        return ret['return']
    else:
        return ret

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='''Simple decorator for instrumenting functions. Testing command line''',
    )
    PARSER.add_argument('--seconds', dest='seconds', default=5,
                        help='Sleep seconds. Default 5.')

    OPTIONS = PARSER.parse_args()

    @timer
    def sleeping_function(sec, inner_sec=None):
        """
        Just sleeps and returns ok
        """

        time.sleep(sec)
        if inner_sec is not None:
            return {'return': 'OK', 'timing': {'inner_sleep': inner_sec}}
        else:
            return 'OK'

    RET = sleeping_function(OPTIONS.seconds)
    print('%s' % RET)
    RET = sleeping_function(OPTIONS.seconds, 10)
    print('%s' % RET)

