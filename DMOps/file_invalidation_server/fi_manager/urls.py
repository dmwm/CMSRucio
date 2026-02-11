from django.urls import path
from .views import FileInvalidationRequestsView, FileQueryView, InvalidationApproval


urlpatterns = [
    path("upload/", FileInvalidationRequestsView.as_view(), name="file-upload"),
    path("query/", FileQueryView.as_view(), name="file-query"),
    path("approve/<uuid:request_id>", InvalidationApproval.as_view(), name="invalidation-approval"),
]