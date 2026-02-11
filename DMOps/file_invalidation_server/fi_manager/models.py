# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
import uuid

class FileInvalidationRequests(models.Model):
    id = models.AutoField(primary_key=True)
    request_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    file_name = models.CharField(max_length=255)
    status = models.CharField(max_length=20)
    mode = models.CharField(max_length=10)
    dry_run = models.BooleanField()
    reason = models.TextField(blank=True, null=True)
    job_id = models.CharField(max_length=8,null=True,blank=True)
    logs = models.TextField()
    rse = models.TextField()
    global_invalidate_last_replicas = models.BooleanField(default=False)
    request_user = models.TextField()
    approve_user = models.TextField()

    class Meta:
        managed = False
        db_table = 'file_invalidation_requests'
        unique_together = (('request_id', 'file_name'),)

    def __str__(self):
        #return f"{self.request_id} - {self.name}" 
        return f"ID#: {self.id} REQUEST NUMBER {self.request_id} FOR FILE {self.file_name}" 
