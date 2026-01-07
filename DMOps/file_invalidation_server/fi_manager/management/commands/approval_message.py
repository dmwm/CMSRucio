from django.core.management.base import BaseCommand
from django.core.mail import send_mail
from ...models import FileInvalidationRequests
from django.conf import settings
from django.utils import timezone
from collections import defaultdict

class Command(BaseCommand):
    help = "Send daily summary of requests waiting approval"

    def handle(self, *args, **kwargs):
        today = timezone.now().date()
        waiting_requests = FileInvalidationRequests.objects.filter(status='waiting_approval')

        if not waiting_requests.exists():
            self.stdout.write("No waiting requests today.")
            return
        
        grouped = defaultdict(list)
        for r in waiting_requests:
            grouped[r.request_id].append(r)

        # Build email body
        body_lines = []
        for request_id, items in grouped.items():
            user = items[0].request_user  # collect unique users
            line = f"Request ID {request_id}: {len(items)} items waiting, submitted by {user}. Approve at: https://file-invalidation.app.cern.ch/api/approve/{r.request_id}"
            body_lines.append(line)

        body = "\n".join(body_lines)

        # Send email
        send_mail(
            subject=f"[File Invalidation] Daily Waiting Requests Summary ({today})",
            message=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[settings.ADMIN_EMAIL],
        )

        self.stdout.write(f"Sent summary email with {waiting_requests.count()} requests.")
