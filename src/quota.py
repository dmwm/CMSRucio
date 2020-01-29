#! /usr/bin/env python

"""
This class store a <RSE-quota> couple. Each CricUser has a list of Quota objects.
"""

class Quota:

    def __init__(self, sitename, quota):
        self.sitename = sitename
        self.quota = quota

    def set_sitename(self, sitename):
        self.sitename = sitename

    def set_quota(self, new_quota):
        self.quota = new_quota


