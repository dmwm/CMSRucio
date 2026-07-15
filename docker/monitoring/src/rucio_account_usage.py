#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_account_usage.py
Author      : Panos Paparrigopoulos <panos.paparrigopoulos AT cern [DOT] ch>
Description : Sends aggregated Rucio data to monit-opensearch.
              The data send consist of Rucio AccountUsage and AccountLimits table contents.
              Data contain following fields:
                'rse_name': The Rucio RSE
                'size_in_tb': The ammount of data locked by the Subscription
                's_name': The subscription name
                's_comment': The comment field of the subscription
                'timestamp': The time that data were generated
"""
import sys

import oracledb

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
import time

import click
import cx_Oracle
from CMSMonitoring.amq_sender import credentials, send_to_amq
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    create_engine,
    join,
    select,
)


@click.command()
@click.option("--creds", required=True, help="etc/secrets/amq.json")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size", default=100)
def main(creds, amq_batch_size):
    # Load credentials and create engine
    creds_json = credentials(f_name=creds)
    creds_json['type'] = 'account_usage'
    with open('/etc/secrets/oracleconnection.txt') as f:
        engine = create_engine(f.read())

    metadata = MetaData(schema="cms_rucio_prod")

    # Define tables
    rses = Table('rses', metadata,
                 Column('rse', String),
                 Column('rse_type', String),
                 Column('id', Integer)
                 )

    account_usage = Table('account_usage', metadata,
                          Column('account', String),
                          Column('rse_id', Integer),
                          Column('bytes', Integer)
                          )

    accounts = Table('accounts', metadata,
                     Column('account', String),
                     Column('account_type', String),
                     Column('status', String)
                     )

    account_limits = Table('account_limits', metadata,
                           Column('account', String),
                           Column('rse_id', Integer),
                           Column('bytes', Integer)
                           )

    # Subquery to select active accounts
    subquery = select(accounts.c.account).where(
        accounts.c.status == 'ACTIVE'
    )

    # Main query to select data
    stmt = select(
        rses.c.rse,
        rses.c.rse_type,
        account_usage.c.account,
        accounts.c.account_type,
        account_usage.c.bytes.label('usage'),
        account_limits.c.bytes.label('quota')
    ).select_from(
        join(account_usage, rses, account_usage.c.rse_id == rses.c.id)
        .join(accounts, accounts.c.account == account_usage.c.account)
        .outerjoin(account_limits, and_(rses.c.id == account_limits.c.rse_id, accounts.c.account == account_limits.c.account))
    ).where(
        and_(
            account_usage.c.bytes > 0,
            ~rses.c.rse.like('%Test'),
            ~rses.c.rse.like('%Temp'),
            account_usage.c.account.in_(subquery)
        )
    ).order_by(rses.c.rse)

    # Execute the query and process results
    with engine.connect() as conn:
        rs = conn.execute(stmt)
        timestamp = int(time.time())
        data = []
        for row in rs:
            result = dict(row._mapping)
            result['timestamp'] = timestamp
            data.append(result)
        send_to_amq(data=data, confs=creds_json, batch_size=amq_batch_size, overwrite_meta_ts=True)

if __name__ == "__main__":
    main()
