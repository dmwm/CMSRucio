#! /usr/bin/env python

from policy import Policy
from quota import Quota

"""
TestPolicy is an example of default policy we can assign to a user whom information about the
institute or intitute_country are missing, so we can not apply the US CMS policy.
"""

class TestPolicy(Policy):

    def __init__(self):
        self.default_quota = 10

    def get_policy(self):
        pass

    def get_rse(self, **kwargs):
        rse_list = []
        return rse_list
