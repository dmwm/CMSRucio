from file_integrity_checker.models import FileIntegrityRequest, FileReplica
import uuid

def make_request(user, rse_expression=None, full_scan=False, status='COMPLETED', job_id=None):
    return FileIntegrityRequest.objects.create(
        request_id=uuid.uuid4(),
        requested_by=user,
        rse_expression=rse_expression,
        full_scan=full_scan,
        status=status,
        job_id=job_id or uuid.uuid4().hex[:8]
    )

def make_replica(request, lfn, rse, replica_status, scope='cms', pfn=None):
    return FileReplica.objects.create(
        request=request,
        scope=scope,
        lfn=lfn,
        rse=rse,
        pfn=pfn or f"davs://{rse}.example.cern.ch:2880{lfn}",
        status=replica_status
    )

# --- Request 1: fully healthy ---
r1 = make_request('username_c', status='COMPLETED', job_id='aabb1122')
make_replica(r1, '/store/data/Run2024/file1.root', 'T1_US_FNAL_Disk', 'OK')
make_replica(r1, '/store/data/Run2024/file1.root', 'T1_UK_RAL_Disk',  'OK')
make_replica(r1, '/store/data/Run2024/file2.root', 'T1_US_FNAL_Disk', 'OK')
make_replica(r1, '/store/data/Run2024/file2.root', 'T1_DE_KIT_Disk',  'OK')
print(f"Request 1 (FULLY_OK): {r1.request_id}")

# --- Request 2: mixed — some corrupted, some OK, some error ---
r2 = make_request('testuser@cern.ch', status='COMPLETED', job_id='ccdd3344')
make_replica(r2, '/store/data/Run2024/file3.root', 'T1_US_FNAL_Disk',   'CORRUPTED')
make_replica(r2, '/store/data/Run2024/file3.root', 'T1_UK_RAL_Disk',    'OK')
make_replica(r2, '/store/data/Run2024/file3.root', 'T0_CH_CERN_Tape',   'ERROR')
make_replica(r2, '/store/data/Run2024/file4.root', 'T1_US_FNAL_Disk',   'CORRUPTED')
make_replica(r2, '/store/data/Run2024/file4.root', 'T1_DE_KIT_Disk',    'CORRUPTED')
make_replica(r2, '/store/data/Run2024/file5.root', 'T1_FR_CCIN2P3_Disk','OK')
print(f"Request 2 (mixed):    {r2.request_id}")

# --- Request 3: fully corrupted ---
r3 = make_request('username_c', status='COMPLETED', job_id='eeff5566')
make_replica(r3, '/store/data/Run2023/file6.root', 'T1_US_FNAL_Disk', 'CORRUPTED')
make_replica(r3, '/store/data/Run2023/file6.root', 'T1_UK_RAL_Disk',  'CORRUPTED')
make_replica(r3, '/store/data/Run2023/file7.root', 'T1_US_FNAL_Disk', 'CORRUPTED')
make_replica(r3, '/store/data/Run2023/file7.root', 'T1_DE_KIT_Disk',  'CORRUPTED')
print(f"Request 3 (FULLY_CORRUPTED): {r3.request_id}")

# --- Request 4: still in progress ---
r4 = make_request('devuser@cern.ch', status='IN_PROGRESS', job_id='aabb9900')
make_replica(r4, '/store/data/Run2024/file8.root',  None, 'pending')
make_replica(r4, '/store/data/Run2024/file9.root',  None, 'pending')
make_replica(r4, '/store/data/Run2024/file10.root', None, 'pending')
print(f"Request 4 (IN_PROGRESS): {r4.request_id}")

# --- Request 5: failed job, no replicas updated ---
r5 = make_request('username_c', status='FAILED', job_id='ffee1234')
make_replica(r5, '/store/data/Run2022/file11.root', None, 'pending')
print(f"Request 5 (FAILED): {r5.request_id}")

# --- Request 6: with RSE expression filter ---
r6 = make_request('username_c', rse_expression='T1_US_FNAL_Disk', status='COMPLETED', job_id='1234abcd')
make_replica(r6, '/store/data/Run2024/file12.root', 'T1_US_FNAL_Disk', 'OK')
make_replica(r6, '/store/data/Run2024/file13.root', 'T1_US_FNAL_Disk', 'CORRUPTED')
print(f"Request 6 (RSE filtered): {r6.request_id}")

print("\nDone. Copy the request IDs above for manual testing.")