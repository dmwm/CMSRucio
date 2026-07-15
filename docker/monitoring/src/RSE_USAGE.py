#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : RSE_USAGE.py
Author      : Jhonatan Amado
			  Panos Paparrigopoulos <panos.paparrigopoulos AT cern [DOT] ch>
			  Juan Pablo Salas <juan.pablo.salas.galindo AT cern [DOT] ch>
Description : 
"""
import sys
from datetime import datetime
import json
import requests

import oracledb
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
import cx_Oracle

from sqlalchemy import create_engine, MetaData, Table, Column, select, join, and_, func, case



now=datetime.now()
timestamp=int(datetime.timestamp(now))*1000



def send(document):
	return requests.post('http://monit-metrics:10012/', data=json.dumps(document),headers={ "Content-Type": "application/json; charset=UTF-8"})

def send_and_check(document, should_fail=False):
	response = send(document)
	assert( (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(document, response.status_code, response.text)



with open('/etc/secrets/oracleconnection.txt') as f:
        engine = create_engine(f.read())

metadata = MetaData(schema="cms_rucio_prod")


replicas = Table('replicas', metadata, Column('state'), Column('bytes'), Column('rse_id')).alias('replicas')

rses = Table('rses', metadata,
	    Column('rse'), Column('id')
		).alias('rses')

account_usage = Table('account_usage', metadata,
	    Column('account'), Column('rse_id'), Column('bytes')
		).alias('account_usage')

accounts = Table('accounts', metadata,
	    Column('account'), Column('account_type'), Column('status')
		).alias('accounts')

account_limits = Table('account_limits', metadata,
	    Column('account'), Column('rse_id'), Column('bytes')
		).alias('account_limits')

subquery = select(accounts.c.account).where(
	    accounts.c.status == 'ACTIVE'
		)#.subquery()

rse_usage = Table('rse_usage', metadata,
		Column('source'),Column('used'),Column('rse_id')
		).alias('rse_usage')

rse_limits = Table('rse_limits',metadata,
		Column('name'), Column('rse_id'), Column('value')
		).alias('rse_limits')

stmt = select(
				rses.c.rse, account_usage.c.account, accounts.c.account_type,
				account_usage.c.bytes.label('usage'),
				account_limits.c.bytes.label('limit_usage'),
				(account_limits.c.bytes - account_usage.c.bytes).label('remain_bytes')
				).select_from(
					join(account_usage, rses, account_usage.c.rse_id == rses.c.id)
					.join(accounts, accounts.c.account == account_usage.c.account)
					.outerjoin(account_limits, and_(
					account_usage.c.rse_id == account_limits.c.rse_id,
					account_usage.c.account == account_limits.c.account
					))
				).where(
				and_(
					account_usage.c.bytes > 0,
					~rses.c.rse.like('%Test'),
					~rses.c.rse.like('%Temp'),
					account_usage.c.account.in_(subquery)
				)
				).order_by(rses.c.rse)


stmt2 = select(
				rses.c.rse,
				func.max(case((rse_usage.c.source == 'static', rse_usage.c.used), else_=None)).label('static'),
				func.max(case((rse_usage.c.source == 'rucio', rse_usage.c.used), else_=None)).label('rucio'),
				func.max(case((rse_usage.c.source == 'expired', rse_usage.c.used), else_=None)).label('expired'),
				func.max(case((rse_usage.c.source == 'unavailable', rse_usage.c.used), else_=None)).label('unavailable'),
				func.max(case((rse_usage.c.source == 'obsolete', rse_usage.c.used), else_=None)).label('obsolete'),
				func.max(case((rse_limits.c.name == 'MinFreeSpace', rse_limits.c.value), else_=None)).label('minfreespace')
				).select_from(
					join(rse_usage, rses, rse_usage.c.rse_id == rses.c.id)
					.outerjoin(rse_limits, rse_usage.c.rse_id == rse_limits.c.rse_id)
				).where(
					and_(
						~rses.c.rse.like('%Test'),
						~rses.c.rse.like('%Temp'),
						~rses.c.rse.like('%Export')
					)
				).group_by(rses.c.rse).order_by(rses.c.rse)



stmt3 =  (
			    replicas.join(rses, replicas.c.rse_id == rses.c.id)
			    .select()
			    .where(~rses.c.rse.like('%_Temp'))
			    .where(~rses.c.rse.like('%_Test'))
			    .with_only_columns(
                rses.c.rse,
                func.sum(case((replicas.c.state == 'A', replicas.c.bytes), else_=0)).label('AVAILABLE'),
                func.sum(case((replicas.c.state == 'C', replicas.c.bytes), else_=0)).label('COPYING'),
                func.sum(case((replicas.c.state == 'U', replicas.c.bytes), else_=0)).label('UNAVAILABLE'),
                func.sum(case((replicas.c.state == 'D', replicas.c.bytes), else_=0)).label('BAD'),
                func.sum(case((replicas.c.state == 'B', replicas.c.bytes), else_=0)).label('BEING_DELETED'),
                func.sum(case((replicas.c.state == 'T', replicas.c.bytes), else_=0)).label('TEMPORARY_UNAVAILABLE'),
        )
        .group_by(rses.c.rse)
        .order_by(rses.c.rse)
        )

RSE_Usage = {}
replica_mapping = { 'A' : 'AVAILABLE',
                    'U' : 'UNAVAILABLE',
                    'C' : 'COPYING',
                    'B' : 'BEING_DELETED',
                    'D' : 'BAD',
                    'T' : 'TEMPORARY_UNAVAILABLE'}


with engine.connect() as conn:
	print(f"Executing stmt...")
	rs = conn.execute(stmt).fetchall()
	print(f"Executed stmt. Executing stmt2...")
	rs2 = conn.execute(stmt2).fetchall()
	print(f"Executed stmt2. Executing stmt3...")
	rs3 = conn.execute(stmt3).fetchall()

i = 0
for row in rs:
	i += 1
	print(f"Row in rs: {i}")
	result = dict(row._asdict())
	print(f"Row with RSE {result['rse']}")
	if result['rse'] not in RSE_Usage.keys():
		RSE_Usage.update({result['rse']:{result['account']:{'RSE' : result['rse'],
		'AccountName' : result['account'],
		'Usage' : result['usage'],
		'AccountType' : result['account_type'],
		'AccountQuota' : result['limit_usage'],
		'UsageNotUsed' : result['remain_bytes']}}})
	else:
		if result['account'] not in RSE_Usage[result['rse']].keys():
			RSE_Usage[result['rse']].update({result['account']:{'RSE' : result['rse'],
			'AccountName' : result['account'],
			'Usage' : result['usage'],
			'AccountType' : result['account_type'],
			'AccountQuota' : result['limit_usage'],
			'UsageNotUsed' : result['remain_bytes']}})
			print(f"RSE {result['rse']} not in RSE Usage.")
			for row2 in rs2:
				resultrse = dict(row2._asdict())
				print(f"Result RSE stmt2 {resultrse['rse']}")
				if resultrse['rse'] not in RSE_Usage.keys():
					print("Entered here!!")
					RSE_Usage.update({resultrse['rse']:{'storage': {'RSE' : resultrse['rse'],'static':resultrse['static'],
					'rucio':resultrse['rucio'],
					'expired':resultrse['expired'],
					'unavailable':resultrse['unavailable'],
					'obsolete':resultrse['obsolete'],
					'minfreespace':resultrse['minfreespace']}}})
				else:
					print("RSE usage yes in RSE")
					RSE_Usage[resultrse['rse']].update({'storage': {'RSE' : resultrse['rse'],'static':resultrse['static'],
					'rucio':resultrse['rucio'],
					'expired':resultrse['expired'],
					'unavailable':resultrse['unavailable'],
					'obsolete':resultrse['obsolete'],
					'minfreespace':resultrse['minfreespace']}})
					for row3 in rs3:
						print(f"Doing stuff on row3")
						resultrepl = dict (row3._asdict())
						print(f"Result RSE stmt3 {resultrepl['rse']}")
						if resultrepl['rse'] not in RSE_Usage.keys():
							RSE_Usage.update({resultrepl['rse']:{'replica_info': {'RSE' : resultrepl['rse'], 'AVAILABLE' : resultrepl['AVAILABLE'],
							'COPYING': resultrepl['COPYING'],
							'UNAVAILABLE' : resultrepl['UNAVAILABLE'],
							'BAD' : resultrepl['BAD'],
							'BEING_DELETED' : resultrepl['BEING_DELETED'],
							'TEMPORARY_UNAVAILABLE':resultrepl['TEMPORARY_UNAVAILABLE']}}})
						else:
							RSE_Usage[resultrepl['rse']].update({'replica_info': {'RSE' : resultrepl['rse'], 'AVAILABLE' : resultrepl['AVAILABLE'],
							'COPYING': resultrepl['COPYING'],
							'UNAVAILABLE' : resultrepl['UNAVAILABLE'],
							'BAD' : resultrepl['BAD'],
							'BEING_DELETED' : resultrepl['BEING_DELETED'],
							'TEMPORARY_UNAVAILABLE':resultrepl['TEMPORARY_UNAVAILABLE']}})


print(json.dumps(RSE_Usage, indent=4))

listRSEUsage = []
for key in list(RSE_Usage.keys()):
	for subkey in RSE_Usage[key]:
		innervalues=RSE_Usage[key][subkey]
		idbfields = [ikey for ikey, ivalue in innervalues.items() if isinstance(ivalue, int)]
		idbtags = [ikey for ikey, ivalue in innervalues.items() if isinstance(ivalue, str)]
		innervalues.update({'idb_tags':idbtags,
		'idb_fields':idbfields,
		'producer':'cmsaaa',
		'type':'RSEFullUsage',
		'timestamp':timestamp})
		listRSEUsage.append(innervalues)

print(listRSEUsage)
send_and_check(listRSEUsage)
