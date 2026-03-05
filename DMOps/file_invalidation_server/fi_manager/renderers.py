from rest_framework.renderers import BrowsableAPIRenderer
from django.utils.safestring import mark_safe

class CustomBrowsableAPIRenderer(BrowsableAPIRenderer):
    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context['site_name'] = 'File Invalidation API'
        return context
    

class ApprovalBrowsableAPIRenderer(BrowsableAPIRenderer):
    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context['site_name'] = 'File Invalidation API'
        context['description'] = mark_safe("""
            <h3> ⚠️ <b>Approval Criteria</b> </h3> <br> 
            <ul> 
            <li> You cannot approve your own request and only DMOps can approve </li>
            <li> Verify RSE and mode are correct before approving </li>
            <li> Check the original ticket to understand why the files are being invalidated </li>
            <li> Check that there are no /RAW/ files being invalidated (unless exceptionally requested by T0)</li>
            </ul>
        """)
        return context
    
    def render(self, data, accepted_media_type=None, renderer_context=None):
        html = super().render(data, accepted_media_type, renderer_context)
        html = html.replace('>Django REST framework<', '>CMS DM File Invalidation Tool<')
        html = html.replace('href="https://www.django-rest-framework.org/"', 'href="/"')
        html = html.replace('>POST<', '>APPROVE<')
        return html