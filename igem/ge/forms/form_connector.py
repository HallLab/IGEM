# from collections import defaultdict

# from django import forms
# from ge.models import Connector


# class ConnectorForm(forms.ModelForm):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self._my_errors = defaultdict(list)

#     class Meta:
#         model = Connector
#         fields = [
#             "connector",
#             "datasource",
#             "description",
#             "update_ds",
#             "source_path",
#             "source_web",
#             "source_compact",
#             "source_file_name",
#             "source_file_format",
#             "source_file_sep",
#             "source_file_skiprow",
#             "target_file_name",
#             "target_file_format",
#             "target_file_keep",
#         ]
