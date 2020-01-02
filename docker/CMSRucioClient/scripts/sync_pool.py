#! /usr/bin/env python

from __future__ import absolute_import, division, print_function


from multiprocessing import Pool
import time
from time import sleep


def f(x):
    return x*x
#
# if __name__ == '__main__':
#     pool = Pool(processes=4)              # start 4 worker processes
#
#     result = pool.apply_async(f, (10,))   # evaluate "f(10)" asynchronously in a single process
#     print( result.get(timeout=1)    )       # prints "100" unless your computer is *very* slow
#
#     print( pool.map(f, range(10))   )       # prints "[0, 1, 4,..., 81]"
#
#     it = pool.imap(f, range(10))
#     print( it.next()                )       # prints "0"
#     print (it.next()             )          # prints "1"
#     print( it.next(timeout=1)    )          # prints "4" unless your computer is *very* slow
#
#     result = pool.apply_async(time.sleep, (10,))
#     print( result.get(timeout=1)   )        # raises multiprocessing.TimeoutError
#
#
#     exit
#
#

# from multiprocessing import Pool

def my_func(pair):
    text, to_sleep = pair
    print('Sleeping: for %s' % text)
    sleep(to_sleep)
    print('Slept for %s' % to_sleep)


if __name__ == '__main__':

    pairs = []

    for secs in range(1,100):

        text = '%s seconds' % secs

        pairs.append((text, secs//10))

    print( len(pairs))

    pool = Pool(processes=4)              # start 4 worker processes
    pool.map(my_func, pairs, chunksize=1)

