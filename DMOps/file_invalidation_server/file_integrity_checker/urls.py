from django.urls import path
from .views import FileIntegrityRequestView, FileIntegrityQueryView

urlpatterns = [
    path("integrity/submit/", FileIntegrityRequestView.as_view(), name="integrity-submit"),
    path("integrity/query/",  FileIntegrityQueryView.as_view(),  name="integrity-query"),
]