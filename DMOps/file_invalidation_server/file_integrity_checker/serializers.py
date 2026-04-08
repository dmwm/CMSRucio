from rest_framework import serializers
from .tasks import MAX_LFNS_PER_REQUEST


class FileIntegrityRequestSerializer(serializers.Serializer):

    lfns = serializers.CharField(
        required=True,
        label="LFNs",
        style={
            "base_template": "textarea.html",
            "rows": 8,
            "cols": 100,
            "resize": "none",
        },
        help_text=(
            f"List of LFNs to check, one per line. "
            f"Maximum {MAX_LFNS_PER_REQUEST} LFNs per request. "
            f"Scope is optional — 'cms' is used as default. "
            f"Example: cms:/store/data/Run2024/file.root"
        )
    )
    rse_expression = serializers.CharField(
        required=False,
        allow_blank=True,
        label="RSE Expression",
        help_text="RSE expression to filter replicas (optional). If omitted all replicas are checked."
    )
    full_scan = serializers.BooleanField(
        initial=False,
        required=False,
        label="Full Scan",
        help_text="If True, performs a full scan by reading every basket. More time-consuming."
    )

    def validate_lfns(self, value):
        lines = [l.strip() for l in value.splitlines() if l.strip()]

        if not lines:
            raise serializers.ValidationError("No LFNs provided.")

        if len(lines) > MAX_LFNS_PER_REQUEST:
            raise serializers.ValidationError(
                f"Too many LFNs: {len(lines)} provided, "
                f"maximum is {MAX_LFNS_PER_REQUEST}."
            )

        return lines