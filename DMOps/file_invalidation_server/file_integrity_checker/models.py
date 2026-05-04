from django.db import models
import uuid

class FileIntegrityRequest(models.Model):

    class Status(models.TextChoices):
        SUBMITTED   = 'SUBMITTED'
        IN_PROGRESS = 'IN_PROGRESS'
        COMPLETED   = 'COMPLETED'
        FAILED      = 'FAILED'

    request_id     = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    requested_by   = models.CharField(max_length=100)
    rse_expression = models.CharField(max_length=255, blank=True, null=True)
    full_scan      = models.BooleanField(default=False)
    status         = models.CharField(max_length=20, choices=Status.choices, default=Status.SUBMITTED)
    job_id         = models.CharField(max_length=64, blank=True, null=True)
    logs           = models.TextField(blank=True, default='')
    created_at     = models.DateTimeField(auto_now_add=True)
    updated_at     = models.DateTimeField(auto_now=True)

    class Meta:
        managed  = False
        db_table = 'file_integrity_checker_requests'
        ordering = ['-created_at']

    def __str__(self):
        return f"Request {self.request_id} by {self.requested_by} [{self.status}]"


class FileReplica(models.Model):

    request = models.ForeignKey(
        FileIntegrityRequest,
        on_delete=models.CASCADE,
        related_name='replicas'
    )
    scope  = models.CharField(max_length=50)
    lfn    = models.CharField(max_length=512)
    pfn    = models.CharField(max_length=1024, blank=True, null=True)
    rse    = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=20)

    class Meta:
        managed  = False
        db_table = 'file_integrity_checker_replicas'

    def __str__(self):
        return f"{self.scope}:{self.lfn} @ {self.rse} from request {self.request.request_id} → {self.status}"