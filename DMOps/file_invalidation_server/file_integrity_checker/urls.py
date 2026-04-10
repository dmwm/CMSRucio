from django.urls import path
from .views import (
    FileIntegrityRequestView,
    FileIntegrityQueryView,
    FileIntegrityFilesView,
    FileIntegrityReplicasView,
)

urlpatterns = [
    # Submit a new file integrity check request
    path(
        'integrity/submit/',
        FileIntegrityRequestView.as_view(),
        name='integrity-submit'
    ),
    # Query requests — list or detail
    path(
        'integrity/query/',
        FileIntegrityQueryView.as_view(),
        name='integrity-query'
    ),
    # Query files (LFNs) by file_status within a request
    path(
        'integrity/files/',
        FileIntegrityFilesView.as_view(),
        name='integrity-files'
    ),
    # Query individual replicas within a request
    path(
        'integrity/replicas/',
        FileIntegrityReplicasView.as_view(),
        name='integrity-replicas'
    ),
]