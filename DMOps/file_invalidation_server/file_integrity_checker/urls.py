from django.urls import path
from .views import (
    FileIntegritySubmitRequestView,
    FileIntegrityQueryRequestView,
    FileIntegrityQueryFilesView,
    FileIntegrityQueryReplicasView,
)

urlpatterns = [
    # Submit a new file integrity check request
    path(
        'integrity/submit/',
        FileIntegritySubmitRequestView.as_view(),
        name='integrity-submit'
    ),
    # Query requests — list or detail
    path(
        'integrity/query/requests/',
        FileIntegrityQueryRequestView.as_view(),
        name='integrity-query-requests'
    ),
    # Query files (LFNs) by file_status within a request
    path(
        'integrity/query/files/',
        FileIntegrityQueryFilesView.as_view(),
        name='integrity-query-files'
    ),
    # Query individual replicas within a request
    path(
        'integrity/query/replicas/',
        FileIntegrityQueryReplicasView.as_view(),
        name='integrity-query-replicas'
    ),
]