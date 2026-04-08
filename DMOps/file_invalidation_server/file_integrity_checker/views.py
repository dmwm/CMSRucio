import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, JSONParser
from rest_framework import status

from .models import FileIntegrityRequest, FileReplica
from .serializers import FileIntegrityRequestSerializer
from .tasks import process_integrity_check
from fi_manager.utils import get_cern_username

logger = logging.getLogger(__name__)


class FileIntegrityRequestView(APIView):
    """
    Submit a new file integrity check request.
    GET  — renders the DRF browsable form
    POST — validates input, creates DB rows, triggers job
    """
    serializer_class = FileIntegrityRequestSerializer
    parser_classes = [MultiPartParser, JSONParser]

    def get_serializer_class(self, *args, **kwargs):
        return self.serializer_class(*args, **kwargs)

    def get(self, request):
        return Response(
            [{"POST": "Submit a file integrity check request"}],
            status=status.HTTP_200_OK
        )

    def post(self, request):
        serializer = FileIntegrityRequestSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        lfns           = serializer.validated_data['lfns']
        rse_expression = serializer.validated_data.get('rse_expression') or None
        full_scan      = serializer.validated_data.get('full_scan', False)
        requested_by   = get_cern_username(request) or 'unknown'

        integrity_request = FileIntegrityRequest.objects.create(
            requested_by=requested_by,
            rse_expression=rse_expression,
            full_scan=full_scan,
            status=FileIntegrityRequest.Status.SUBMITTED
        )

        try:
            job_id, job_status = process_integrity_check(integrity_request, lfns)
        except ValueError as e:
            # tasks.py raised a validation error — clean up the request row
            integrity_request.delete()
            return Response({"message": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            integrity_request.status = FileIntegrityRequest.Status.FAILED
            integrity_request.logs = str(e)
            integrity_request.save(update_fields=['status', 'logs'])
            logger.error(f"Job trigger failed for request {integrity_request.request_id}: {e}")
            return Response(
                {"message": "Job trigger failed.", "detail": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            {
                "message": (
                    f"{len(lfns)} LFN(s) submitted for integrity check. "
                    f"Job ID: {job_id}."
                ),
                "request_id": str(integrity_request.request_id),
                "job_id": job_id,
                "status": job_status,
                "query_url": f"/api/integrity/query/?request_id={integrity_request.request_id}"
            },
            status=status.HTTP_201_CREATED
        )


class FileIntegrityQueryView(APIView):
    """
    Query file integrity check requests and their replica results.
    GET ?request_id=<uuid>  — returns one request with all its replicas
    GET ?status=<status>    — returns all requests with that status
    GET (no params)         — returns all requests (summary, no replicas)
    """

    def get(self, request):
        request_id = request.query_params.get('request_id')
        query_status = request.query_params.get('status')

        # Query by request_id — return full detail with replicas
        if request_id:
            try:
                integrity_request = FileIntegrityRequest.objects.get(
                    request_id=request_id
                )
            except FileIntegrityRequest.DoesNotExist:
                return Response(
                    {"message": f"No request found for request_id: {request_id}"},
                    status=status.HTTP_404_NOT_FOUND
                )

            replicas = integrity_request.replicas.all()
            return Response(
                {
                    "request_id":     str(integrity_request.request_id),
                    "requested_by":   integrity_request.requested_by,
                    "rse_expression": integrity_request.rse_expression,
                    "full_scan":      integrity_request.full_scan,
                    "status":         integrity_request.status,
                    "job_id":         integrity_request.job_id,
                    "logs":           integrity_request.logs,
                    "created_at":     integrity_request.created_at,
                    "updated_at":     integrity_request.updated_at,
                    "replicas": [
                        {
                            "scope":  r.scope,
                            "lfn":    r.lfn,
                            "rse":    r.rse,
                            "status": r.status,
                        }
                        for r in replicas
                    ]
                },
                status=status.HTTP_200_OK
            )

        # Query by status — return list of matching requests (no replicas)
        if query_status:
            requests_qs = FileIntegrityRequest.objects.filter(status=query_status)
            return Response(
                [
                    {
                        "request_id":   str(r.request_id),
                        "requested_by": r.requested_by,
                        "status":       r.status,
                        "job_id":       r.job_id,
                        "created_at":   r.created_at,
                    }
                    for r in requests_qs
                ],
                status=status.HTTP_200_OK
            )

        # No params — return all requests as summary
        all_requests = FileIntegrityRequest.objects.all()
        return Response(
            [
                {
                    "request_id":   str(r.request_id),
                    "requested_by": r.requested_by,
                    "status":       r.status,
                    "job_id":       r.job_id,
                    "created_at":   r.created_at,
                }
                for r in all_requests
            ],
            status=status.HTTP_200_OK
        )