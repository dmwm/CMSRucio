from rest_framework.renderers import BrowsableAPIRenderer

class CustomBrowsableAPIRenderer(BrowsableAPIRenderer):
    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context['site_name'] = 'File Invalidation API'
        return context
    

class ApprovalBrowsableAPIRenderer(BrowsableAPIRenderer):
    def get_context(self, *args, **kwargs):
        context = super().get_context(*args, **kwargs)
        context['site_name'] = 'File Invalidation API'
        context['description'] = """
            ⚠️ **Approval Criteria**
            - You cannot approve your own request and only DMOps can approve
            - Verify RSE and mode are correct before approving
            - Check the original ticket to understand why the files are being invalidated
            - Check that there are no /RAW/ files being invalidated (unless exceptionally requested by T0)
        """
        return context