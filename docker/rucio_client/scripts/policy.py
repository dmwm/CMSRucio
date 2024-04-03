#! /usr/bin/env python

from abc import ABCMeta, abstractmethod

"""
This abstract class defines a way in which all the policy have to behave. In order to define a new policy
we have to derie this abstract class and to implement, at least, all the abstract methods.
This mechanism is transparent to the mapping algorithm, it does not need to know the particural policy of
one user, but it can just call the get_rse method of whatever policy.
"""

class Policy():

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_policy(self):
        pass

    @abstractmethod
    def get_rse(self, **kwargs):
        pass

