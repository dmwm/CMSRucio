from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, JSONParser
from rest_framework import status, serializers
from .models import FileInvalidationRequests
import base64
from django.http import HttpResponse
from .utils import process_invalidation, get_cern_username
from django.views.generic import TemplateView
from django.db.models import Count, CharField, Value, F, Case, When
from django.db.models.functions import StrIndex, Substr
import logging
import uuid

class FileInvalidationRequestSerializer(serializers.Serializer):
    reason = serializers.CharField(required=True,help_text="Enter the reason for the file invalidation request.")
    file_content = serializers.CharField(required=True,
                                        style={
                                            "base_template": "textarea.html",
                                            "rows":8,
                                            "cols":100,
                                            "resize": "none"},
                                        help_text="Please enter the list of DIDs to be invalidated. The file content should be a list of DIDs of the same type without the scope prefix, one per line.")
    dry_run = serializers.BooleanField(initial=True)
    mode = serializers.ChoiceField(choices=['global','local'],allow_blank=False)
    rse = serializers.CharField(required=False,help_text="ONLY FOR LOCAL MODE. The RSE at which the list of files should be invalidated.")
    global_invalidate_last_replicas = serializers.BooleanField(
        initial=False,required=False,
        help_text="ONLY FOR LOCAL MODE. If set to True, files with their last available replica under invalidation will be invalidated globally."
    )

    def validate(self,data):
        mode = data.get('mode')
        rse = data.get('rse')
        global_invalidate_last_replicas = data.get('global_invalidate_last_replicas',False)

        if mode == 'global' and rse:
            raise serializers.ValidationError({'rse':"RSE must be empty when mode is 'global'"})

        if mode == 'local' and not rse:
            raise serializers.ValidationError({'rse':"RSE is required when mode is 'local'"})
        
        if global_invalidate_last_replicas and mode != 'local':
            raise serializers.ValidationError({'global_invalidate_last_replicas':" This option it's only allowed when the mode is set to 'local'."})

        file_content = data.get('file_content')
        if 'cms:/' in file_content:
            raise serializers.ValidationError({'file_content':"Scope should not be part of the file contents."})

        return data

class FileInvalidationRequestsView(APIView):
    serializer_class = FileInvalidationRequestSerializer
    parser_classes = [MultiPartParser, JSONParser]

    def get_serializer_class(self, *args, **kwargs):
        return self.serializer_class(*args, **kwargs)

    def get(self, request):

        return Response(
            [{"POST": "Upload file invalidation requests"}],
            status=status.HTTP_200_OK
        )

    def post(self, request, *args, **kwargs):
        serializer = FileInvalidationRequestSerializer(data=request.data)
        if serializer.is_valid():
            reason = serializer.validated_data['reason']
            file_lines = serializer.validated_data['file_content']
            dry_run = serializer.validated_data['dry_run']
            mode = serializer.validated_data['mode']
            if mode == 'local':
                rse = serializer.validated_data['rse']
            else:
                rse = None
            global_invalidate_last_replicas = serializer.validated_data['global_invalidate_last_replicas']
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        file_lines = file_lines.splitlines()
        request_id = uuid.uuid4()
        logging.info({k: v for k, v in request.META.items() if k.startswith("HTTP_") or k == "REMOTE_USER"})
        user = get_cern_username(request)

        cnt = 0

        raw_file_message = ""
        already_serviced_files = ""
        for fn in file_lines:
            fn = fn.strip()
            fn = fn.replace('cms:/store','/store')
            obj = FileInvalidationRequests.objects.filter(file_name=fn).filter(dry_run=False).first()
            if obj:
                already_serviced_files = already_serviced_files + f'\nRequest was not created for file {fn} because it has already been submitted and it is currently in status: {obj.status}.'
                if obj.status=="in_progress":
                    already_serviced_files = already_serviced_files + ' Please wait 30min for the CronJob to update it on the database'
                elif obj.status=="waiting_approval":
                    already_serviced_files = already_serviced_files + ' Please ask DMOps to approve the invalidation.'
            else:
                input_vals = {'request_id':request_id,'file_name':fn,'status':'waiting_approval','mode':mode,'dry_run':dry_run,'reason':reason,'global_invalidate_last_replicas':global_invalidate_last_replicas,'request_user':user, 'rse': rse}
                file_record = FileInvalidationRequests.objects.create(**input_vals)
                cnt += 1

        logging.info(f'{cnt} of {len(file_lines)} files were created in the database with waiting_approval status.')


        response_message = raw_file_message + already_serviced_files

        return Response({"message": response_message,
                         "redirect_url":f"https://file-invalidation.app.cern.ch/api/query/?request_id={request_id}",
                         "redirect_description":"View request_id details"}, status=status.HTTP_201_CREATED)

class FileQueryView(APIView):
    def get(self, request, *args, **kwargs):
        file_status = request.query_params.get("status")
        file_request_id = request.query_params.get("request_id")
        job_id = request.query_params.get("job_id")
        file_name_regex = request.query_params.get("file_name_regex")
        reason_regex = request.query_params.get("reason_regex")
        files = FileInvalidationRequests.objects.all()
        
        if file_request_id:
            files = files.filter(request_id=file_request_id)

        if job_id:
            files = files.filter(job_id=job_id)

        if file_status:
            files = files.filter(status=file_status)

        if file_name_regex:
            files = files.filter(file_name__regex=file_name_regex)

        if reason_regex:
            files = files.filter(reason__regex=reason_regex)

        if not file_request_id and not file_status and not file_name_regex and not reason_regex and not job_id:
            grouped = FileInvalidationRequests.objects.annotate(
                delimiter_position=StrIndex('reason',Value('- Request aborted')),
                truncated_reason = Case(
                    # Check if the delimiter was found (StrIndex returns > 0 if found)
                    When(delimiter_position__gt=0, then=Substr('reason', Value(1), F('delimiter_position') - 1)),
                    # If delimiter not found (delimiter_position is 0), use the original reason
                    default=F('reason'),
                    output_field=CharField() # Ensure the resulting field is a character field
                )
            ).values(
                'request_id','status','mode','dry_run','truncated_reason','job_id','logs'
                ).annotate(
                    total_objects=Count('id')
                ).order_by(
                    'request_id', 'status'
                )
                        
            response = Response(
            grouped,
            status=status.HTTP_200_OK)
            return response

        
        return Response(
            [{"request_id": f.request_id, "file_name": f.file_name, "status": f.status,"mode":f.mode,"dry_run":f.dry_run,"reason":f.reason,"job_id":f.job_id,"logs":f.logs} for f in files],
            status=status.HTTP_200_OK
        )


class InvalidationApproval(APIView):
    
    def get(self, request, request_id):
        files = FileInvalidationRequests.objects.filter(request_id=request_id)

        return Response(
            [{"request_id": f.request_id, "file_name": f.file_name, "status": f.status,"mode":f.mode,"dry_run":f.dry_run,"reason":f.reason,"job_id":f.job_id,"logs":f.logs,"request_user":f.request_user} for f in files],
            status=status.HTTP_200_OK
        )

    
    def post(self, request, request_id):
        files = FileInvalidationRequests.objects.filter(request_id=request_id)

        if not files.exists():
            return Response({"message":f"Files with request_id {request_id} not found"},
                            status=status.HTTP_404_NOT_FOUND)
        
        approval_user = get_cern_username(request)
        request_user = files.first().request_user

        if approval_user==request_user:
            return Response({"message":f"Approval user cannot be the same as request user"},
                            status=status.HTTP_403_FORBIDDEN)
        
        updated = files.update(status="approved",approval_user=approval_user)
        reason = updated.first().reason
        dry_run = updated.first().dry_run
        mode = updated.first().mode
        rse = updated.first().rse
        global_invalidate_last_replicas = updated.first().global_invalidate_last_replicas

        try:
            response_message = process_invalidation(request_id, reason, dry_run=dry_run, mode=mode, rse=rse,to_process="approved",global_invalidate_last_replicas=global_invalidate_last_replicas)
            return Response({"message": response_message,
                             "redirect_url":f"https://file-invalidation.app.cern.ch/api/query/?request_id={request_id}",
                            "redirect_description":"View request_id details"}, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response({"message":f"None of the files could be invalidated - {str(e)}"})