from django.urls import path
from .views import FileInvalidationRequestsView, FileQueryView, InvalidationApproval


urlpatterns = [
    path("upload/", FileInvalidationRequestsView.as_view(), name="file-upload"),
    path("query/", FileQueryView.as_view(), name="file-query-filters"),
    path("query/<uuid:request_id>", FileQueryView.as_view(), name="file-query-request"),
    path("approve/<uuid:request_id>", InvalidationApproval.as_view(), name="invalidation-approval"),
]