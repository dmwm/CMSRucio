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
            line = f"\t-Request ID {request_id}\n\t\tFiles waiting approval:{len(items)}\n\t\tSubmitted by {user}\n\t\tApprove at: https://file-invalidation.app.cern.ch/api/approve/{request_id}"
            body_lines.append(line)

        body = "\n\n".join(body_lines)

        html_body = f"""
            <html>
            <body>
            <h2>[File Invalidation] Weekly Waiting Requests Summary ({today})</h2>
            <ul>
            {''.join(f"<li>Request ID {request_id}\nFiles waiting for approval: {len(items)}\nSubmitted by {items[0].request_user}\nApprove at: https://file-invalidation.app.cern.ch/api/approve/{request_id}</li>" for request_id, items in grouped.items())}
            </ul>
            </body>
            </html>
        """

        # Send email
        send_mail(
            subject=f"[File Invalidation] Weekly Waiting Requests Summary ({today})",
            message=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=settings.ADMIN_EMAILS,
            html_message=html_body,
        )

        self.stdout.write(f"Sent summary email with {waiting_requests.count()} requests.")
