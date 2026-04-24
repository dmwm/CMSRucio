import logging
from collections import defaultdict
from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, JSONParser
from rest_framework import status

from .models import FileIntegrityRequest, FileReplica
from .serializers import FileIntegrityRequestSerializer
from .tasks import process_integrity_check
from fi_manager.utils import get_cern_username

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_username(request):
    return get_cern_username(request) or 'unknown'


def derive_file_status(replica_statuses):
    """
    Derives a single human-readable status for a file based on its replicas.

    Priority order:
        FULLY_CORRUPTED     — every replica is CORRUPTED
        PARTIALLY_CORRUPTED — at least one replica is CORRUPTED
        FULLY_OK            — every replica is OK
        OK                  — at least one replica is OK
        PENDING             — at least one replica is pending
        ERROR               — at least one replica is ERROR
        UNKNOWN             — no replicas or unrecognised combination
    """
    if not replica_statuses:
        return 'UNKNOWN'
    unique = set(replica_statuses)
    if unique == {'CORRUPTED'}:
        return 'FULLY_CORRUPTED'
    if 'CORRUPTED' in unique:
        return 'PARTIALLY_CORRUPTED'
    if unique == {'OK'}:
        return 'FULLY_OK'
    if 'OK' in unique:
        return 'OK'
    if 'pending' in unique:
        return 'PENDING'
    if 'ERROR' in unique:
        return 'ERROR'
    return 'UNKNOWN'


def build_request_summary(integrity_request):
    """
    Returns aggregate counts for a request across all its replicas.
    Used in list views and the detail view header.

    Fields:
        lfn_count      — number of distinct LFNs in this request
        replica_count  — total number of replica rows
        OK             — replicas with status OK
        CORRUPTED      — replicas with status CORRUPTED
        ERROR          — replicas with status ERROR
        pending        — replicas still waiting for job results
    """
    replicas = list(integrity_request.replicas.all())
    lfns = set(r.lfn for r in replicas)

    status_counts = defaultdict(int)
    for r in replicas:
        status_counts[r.status] += 1

    return {
        'lfn_count':     len(lfns),
        'replica_count': len(replicas),
        'OK':            status_counts.get('OK', 0),
        'CORRUPTED':     status_counts.get('CORRUPTED', 0),
        'ERROR':         status_counts.get('ERROR', 0),
        'pending':       status_counts.get('pending', 0),
    }


def build_files_view(integrity_request):
    """
    Groups replicas by LFN and returns a structured list of files.

    Each file entry contains:
        lfn         — the logical file name
        scope       — the Rucio scope (e.g. 'cms')
        file_status — derived status based on all replicas for this LFN
        summary     — per-status replica counts for this LFN
        replicas    — list of replica dicts (rse, pfn, status)
    """
    replicas = list(integrity_request.replicas.all())

    # Group replicas by LFN preserving insertion order
    files = defaultdict(lambda: {'scope': None, 'replicas': []})
    for r in replicas:
        files[r.lfn]['scope'] = r.scope
        files[r.lfn]['replicas'].append({
            'rse':    r.rse,
            'pfn':    r.pfn,
            'status': r.status,
        })

    result = []
    for lfn, data in files.items():
        replica_statuses = [rep['status'] for rep in data['replicas']]
        status_counts    = defaultdict(int)
        for s in replica_statuses:
            status_counts[s] += 1

        result.append({
            'scope':       data['scope'],
            'lfn':         lfn,
            'file_status': derive_file_status(replica_statuses),
            'summary': {
                'replica_count': len(data['replicas']),
                'OK':            status_counts.get('OK', 0),
                'CORRUPTED':     status_counts.get('CORRUPTED', 0),
                'ERROR':         status_counts.get('ERROR', 0),
                'pending':       status_counts.get('pending', 0),
            },
            'replicas': data['replicas'],
        })

    return result


def get_request_or_error(request_id):
    """
    Fetches a FileIntegrityRequest by request_id.
    Returns (integrity_request, None) on success.
    Returns (None, Response) on failure — caller must return the Response.
    """
    try:
        return FileIntegrityRequest.objects.get(request_id=request_id), None
    except FileIntegrityRequest.DoesNotExist:
        return None, Response(
            {'message': f'No request found for request_id: {request_id}'},
            status=status.HTTP_404_NOT_FOUND
        )


def build_links(request_obj, base_request):
    """
    Builds full absolute URLs for the three query views for a given request.
    base_request is the Django HTTP request object, used to build absolute URIs.
    """
    rid = str(request_obj.request_id)
    return {
        "details":   base_request.build_absolute_uri(
            f"/api/integrity/query/requests/?request_id={rid}"
        ),
        "files":    base_request.build_absolute_uri(
            f"/api/integrity/query/files/?request_id={rid}"
        ),
        "replicas": base_request.build_absolute_uri(
            f"/api/integrity/query/replicas/?request_id={rid}"
        ),
    }


# ---------------------------------------------------------------------------
# Submit view
# ---------------------------------------------------------------------------

class FileIntegritySubmitRequestView(APIView):
    """
    Submit a new file integrity check request.

    GET  — renders the DRF browsable API form
    POST — validates input, saves to DB, triggers Kubernetes job

    POST fields:
        lfns           (required) — one LFN per line, scope optional
        rse_expression (optional) — filter replicas by RSE expression
        full_scan      (optional) — read every basket, slower but thorough
    """
    serializer_class = FileIntegrityRequestSerializer
    parser_classes   = [MultiPartParser, JSONParser]

    def get_serializer_class(self, *args, **kwargs):
        return self.serializer_class(*args, **kwargs)

    def get(self, request):
        return Response(
            [{'POST': 'Submit a file integrity check request'}],
            status=status.HTTP_200_OK
        )

    def post(self, request):
        serializer = FileIntegrityRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(
                serializer.errors,
                status=status.HTTP_400_BAD_REQUEST
            )

        lfns           = serializer.validated_data['lfns']
        rse_expression = serializer.validated_data.get('rse_expression') or None
        full_scan      = serializer.validated_data.get('full_scan', False)
        requested_by   = get_username(request)

        integrity_request = FileIntegrityRequest.objects.create(
            requested_by=requested_by,
            rse_expression=rse_expression,
            full_scan=full_scan,
            status=FileIntegrityRequest.Status.SUBMITTED
        )

        try:
            job_id, job_status = process_integrity_check(integrity_request, lfns)
        except ValueError as e:
            integrity_request.delete()
            return Response(
                {'message': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            integrity_request.status = FileIntegrityRequest.Status.FAILED
            integrity_request.logs   = str(e)
            integrity_request.save(update_fields=['status', 'logs', 'updated_at'])
            logger.error(
                f"Job trigger failed for request "
                f"{integrity_request.request_id}: {e}"
            )
            return Response(
                {'message': 'Job trigger failed.', 'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {
                'message':    (
                    f"{len(lfns)} LFN(s) submitted for integrity check. "
                    f"Job ID: {job_id}."
                ),
                'request_id': str(integrity_request.request_id),
                'job_id':     job_id,
                'status':     job_status,
                'links':      build_links(integrity_request, request),
            },
            status=status.HTTP_201_CREATED
        )


# ---------------------------------------------------------------------------
# Query view — requests
# ---------------------------------------------------------------------------

class FileIntegrityQueryRequestView(APIView):
    """
    Query file integrity check requests.

    --- Detail (always requires request_id) ---

    GET /api/integrity/query/requests/?request_id=<uuid>
        Returns full detail for one request with files grouped by LFN.
        Each file shows its derived file_status and per-replica breakdown.

    Optional params for detail view:
        include_replicas=false   Omit the files/replica breakdown.
                                 Useful for a quick status check.
                                 Default: true
        include_logs=true        Include raw Kubernetes pod logs.
                                 Logs can be large — omitted by default.
                                 Default: false
        include_summary=false    Omit the aggregate summary counts.
                                 Default: true

    --- List (no request_id) ---

    GET /api/integrity/query/requests/
        Returns all requests with basic fields.

    GET /api/integrity/query/requests/?status=<status>
        Filters requests by status.
        Valid values: SUBMITTED, IN_PROGRESS, COMPLETED, FAILED

    GET /api/integrity/query/requests/?requested_by=<username>
        Filters requests by the user who submitted them.

    GET /api/integrity/query/requests/?status=<status>&requested_by=<username>
        Combines status and requested_by filters.

    Optional params for list view:
        include_summary=true     Include aggregate replica counts per request.
                                 Adds one DB query per request — use with care
                                 on large lists.
                                 Default: false
    """

    def get(self, request):
        request_id       = request.query_params.get('request_id')
        query_status     = request.query_params.get('status')
        requested_by     = request.query_params.get('requested_by')
        include_replicas = request.query_params.get(
            'include_replicas', 'true'
        ).lower() != 'false'
        include_logs     = request.query_params.get(
            'include_logs', 'false'
        ).lower() == 'true'
        include_summary  = request.query_params.get(
            'include_summary', 'true' if request_id else 'false'
        ).lower() != 'false'

        # --- Detail view ---
        if request_id:
            integrity_request, err = get_request_or_error(request_id)
            if err:
                return err

            links = build_links(integrity_request, request)
            response_data = {
                'request_id':     str(integrity_request.request_id),
                'requested_by':   integrity_request.requested_by,
                'status':         integrity_request.status,
                'rse_expression': integrity_request.rse_expression,
                'full_scan':      integrity_request.full_scan,
                'job_id':         integrity_request.job_id,
                'created_at':     integrity_request.created_at,
                'updated_at':     integrity_request.updated_at,
                'links':          {"files": links['files'], "replicas": links['replicas']},
            }

            if include_summary:
                response_data['summary'] = build_request_summary(integrity_request)

            if include_logs:
                response_data['logs'] = integrity_request.logs

            if include_replicas:
                response_data['files'] = build_files_view(integrity_request)

            return Response(response_data, status=status.HTTP_200_OK)

        # --- List view ---
        requests_qs = FileIntegrityRequest.objects.all()

        # Apply filters
        if query_status:
            requests_qs = requests_qs.filter(status=query_status)
        if requested_by:
            requests_qs = requests_qs.filter(requested_by=requested_by)

        if include_summary:
            requests_qs = requests_qs.prefetch_related('replicas')

        result = []
        for r in requests_qs:
            entry = {
                'request_id':     str(r.request_id),
                'requested_by':   r.requested_by,
                'status':         r.status,
                'rse_expression': r.rse_expression,
                'full_scan':      r.full_scan,
                'job_id':         r.job_id,
                'created_at':     r.created_at,
                'updated_at':     r.updated_at,
                'links':          build_links(r, request),
            }
            if include_summary:
                entry['summary'] = build_request_summary(r)
            result.append(entry)

        return Response(result, status=status.HTTP_200_OK)


# ---------------------------------------------------------------------------
# Files view — query files (LFNs) by file_status
# ---------------------------------------------------------------------------

class FileIntegrityQueryFilesView(APIView):
    """
    Query files (LFNs) within a specific request.
    request_id is always required.

    GET /api/integrity/query/files/?request_id=<uuid>
        Returns all files for the request, each with derived file_status,
        per-file summary counts, and full replica breakdown.

    GET /api/integrity/query/files/?request_id=<uuid>&file_status=<status>
        Filters files by their derived file_status.
        Valid values:
            FULLY_CORRUPTED      — all replicas corrupted
            PARTIALLY_CORRUPTED  — at least one replica corrupted
            FULLY_OK             — all replicas OK
            OK                   — at least one replica OK
            PENDING              — at least one replica pending
            ERROR                — at least one replica errored
            UNKNOWN              — no replicas or unrecognised state

    The output=lfns parameter can be added to ANY of the above
    to switch from JSON to a plain text LFN list. Examples:

    GET /api/integrity/query/files/?request_id=<uuid>&output=lfns
        All LFNs in the request, one per line.

    GET /api/integrity/query/files/?request_id=<uuid>&file_status=<status>&output=lfns
        LFNs filtered by file_status, one per line.

    Plain text output format:

        /store/data/file1.root
        /store/data/file2.root
    """

    def get(self, request):
        request_id  = request.query_params.get('request_id')
        file_status = request.query_params.get('file_status')
        output      = request.query_params.get('output')

        if not request_id:
            return Response(
                {'message': 'request_id is required.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        integrity_request, err = get_request_or_error(request_id)
        if err:
            return err

        # Build full file view and apply file_status filter if requested
        files = build_files_view(integrity_request)

        if file_status:
            files = [
                f for f in files
                if f['file_status'] == file_status
            ]

        # Plain text output — one LFN per line
        if output == 'lfns':
            lfn_list = '\n'.join(f['lfn'] for f in files)
            return HttpResponse(lfn_list, content_type='text/plain')

        links = build_links(integrity_request, request)
        files = [
            {
                'request_id': request_id,
                'links': {"request": links['details'], "replicas": links['replicas']},
                **f,
            }
            for f in files
        ]
        return Response(files, status=status.HTTP_200_OK)


# ---------------------------------------------------------------------------
# Replicas view — query individual replicas
# ---------------------------------------------------------------------------

class FileIntegrityQueryReplicasView(APIView):
    """
    Query individual replicas within a specific request.
    request_id is always required.

    GET /api/integrity/query/replicas/?request_id=<uuid>
        Returns all replicas for the request.

    GET /api/integrity/query/replicas/?request_id=<uuid>&replica_status=<status>
        Filters replicas by their raw status.
        Valid values: OK, CORRUPTED, ERROR, pending
        Any value the tool returns is accepted.

    GET /api/integrity/query/replicas/?request_id=<uuid>&lfn=<lfn>
        Returns all replicas for a specific LFN within this request.

    GET /api/integrity/query/replicas/?request_id=<uuid>&lfn=<lfn>&replica_status=<status>
        Combines LFN and status filters.

    The output=lfns_per_rse parameter can be added to ANY of the above
    to switch from JSON to plain text grouped by RSE. Examples:

    GET /api/integrity/query/replicas/?request_id=<uuid>&output=lfns_per_rse
        All replicas grouped by RSE.

    GET /api/integrity/query/replicas/?request_id=<uuid>&replica_status=<status>&output=lfns_per_rse
        Replicas with a specific status, grouped by RSE.

    GET /api/integrity/query/replicas/?request_id=<uuid>&lfn=<lfn>&output=lfns_per_rse
        All replicas for a specific LFN, grouped by RSE.

    GET /api/integrity/query/replicas/?request_id=<uuid>&lfn=<lfn>&replica_status=<status>&output=lfns_per_rse
        Both filters applied, grouped by RSE.

    Plain text output format:

        T1_US_FNAL_Disk
        /store/data/file1.root
        /store/data/file2.root

        T1_UK_RAL_Disk
        /store/data/file1.root
    """

    def get(self, request):
        request_id     = request.query_params.get('request_id')
        replica_status = request.query_params.get('replica_status')
        lfn_filter     = request.query_params.get('lfn')
        output         = request.query_params.get('output')

        if not request_id:
            return Response(
                {'message': 'request_id is required.'},
                status=status.HTTP_400_BAD_REQUEST
            )

        integrity_request, err = get_request_or_error(request_id)
        if err:
            return err

        replicas_qs = FileReplica.objects.filter(request=integrity_request)

        if replica_status:
            replicas_qs = replicas_qs.filter(status=replica_status)

        if lfn_filter:
            replicas_qs = replicas_qs.filter(lfn=lfn_filter)

        replicas = list(replicas_qs)

        # Plain text output grouped by RSE
        if output == 'lfns_per_rse':
            return HttpResponse(
                _format_lfns_per_rse(replicas),
                content_type='text/plain'
            )

        links = build_links(integrity_request, request)
        return Response(
            [
                {
                    'request_id':      request_id,
                    'links':           {"request": links['details'], "files": links['files']},
                    'rse':             r.rse,
                    'scope':           r.scope,
                    'lfn':             r.lfn,
                    'pfn':             r.pfn,
                    'status':          r.status,
                }
                for r in replicas
            ],
            status=status.HTTP_200_OK
        )


def _format_lfns_per_rse(replicas):
    """
    Formats a list of FileReplica objects as plain text grouped by RSE.

    Output format:
        T1_US_FNAL_Disk
        /store/data/file1.root
        /store/data/file2.root

        T1_UK_RAL_Disk
        /store/data/file1.root
    """
    grouped = defaultdict(list)
    for r in replicas:
        rse = r.rse or 'UNKNOWN_RSE'
        if r.lfn not in grouped[rse]:
            grouped[rse].append(r.lfn)

    blocks = []
    for rse, lfns in sorted(grouped.items()):
        block = rse + '\n' + '\n'.join(lfns)
        blocks.append(block)

    return '\n\n'.join(blocks)