#! /usr/bin/env python

from __future__ import print_function

import csv

from rucio.common import exception
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.replica import __exists_replicas, update_replicas_states
from rucio.core.rse import get_rse_id
from rucio.db.sqla import models
from rucio.db.sqla.constants import (ReplicaState, BadFilesStatus)
from rucio.db.sqla.session import transactional_session
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.orm.exc import FlushError

# Adapted from __declare_bad_file_replicas in core/replica.py and removing the ATLAS PFN to LFN translation and the stuff for non-deterministic RSEs


@transactional_session
def __declare_bad_file_replicas(dids, rse_id, reason, issuer, status=BadFilesStatus.BAD, scheme='srm', session=None):
    """
    Declare a list of bad replicas.

    :param dids: The list of DIDs.
    :param rse_id: The RSE id.
    :param reason: The reason of the loss.
    :param issuer: The issuer account.
    :param status: Either BAD or SUSPICIOUS.
    :param scheme: The scheme of the PFNs.
    :param session: The database session in use.
    """
    unknown_replicas = []
    replicas = []
    if True:
        for did in dids:
            scope = InternalScope(did['scope'], vo=issuer.vo)
            name = did['name']
            __exists, scope, name, already_declared, size = __exists_replicas(rse_id, scope, name, path=None,
                                                                              session=session)
            if __exists and ((str(status) == str(BadFilesStatus.BAD) and not already_declared) or str(status) == str(
                    BadFilesStatus.SUSPICIOUS)):
                replicas.append({'scope': scope, 'name': name, 'rse_id': rse_id, 'state': ReplicaState.BAD})
                new_bad_replica = models.BadReplicas(scope=scope, name=name, rse_id=rse_id, reason=reason, state=status,
                                                     account=issuer, bytes=size)
                new_bad_replica.save(session=session, flush=False)
                session.query(models.Source).filter_by(scope=scope, name=name, rse_id=rse_id).delete(
                    synchronize_session=False)
            else:
                if already_declared:
                    unknown_replicas.append('%s:%s %s' % (did['scope'], did['name'], 'Already declared'))
                else:
                    unknown_replicas.append('%s:%s %s' % (did['scope'], did['name'], 'Unknown replica'))
        if str(status) == str(BadFilesStatus.BAD):
            # For BAD file, we modify the replica state, not for suspicious
            try:
                # there shouldn't be any exceptions since all replicas exist
                update_replicas_states(replicas, session=session)
            except exception.UnsupportedOperation:
                raise exception.ReplicaNotFound("One or several replicas don't exist.")
    try:
        session.flush()
    except IntegrityError as error:
        raise exception.RucioException(error.args)
    except DatabaseError as error:
        raise exception.RucioException(error.args)
    except FlushError as error:
        raise exception.RucioException(error.args)

    return unknown_replicas


issuer = InternalAccount('root')
with open('bad_replicas.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for rse, scope, name, reason in reader:
        rse_id = get_rse_id(rse=rse)
        dids = [{'scope': scope, 'name': name}]
        __declare_bad_file_replicas(dids=dids, rse_id=rse_id, reason=reason, issuer=issuer)
