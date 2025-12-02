from django.urls import path
from .views import FileInvalidationRequestsView, FileQueryView


urlpatterns = [
    path("upload/", FileInvalidationRequestsView.as_view(), name="file-upload"),
    path("query/", FileQueryView.as_view(), name="file-query"),
]