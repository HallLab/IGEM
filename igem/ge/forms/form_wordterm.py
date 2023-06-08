# from collections import defaultdict

# from django import forms
# from ge.models import Term, WordTerm


# class WordTermForm(forms.ModelForm):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self._my_errors = defaultdict(list)

#     class Meta:
#         model = WordTerm
#         fields = [
#             "id",
#             "word",
#             # "term",
#             # "status",
#             # "commute",
#         ]


# # https://stackoverflow.com/questions/16755312/django-admin-change-form-load-quite-slow
# class WTForm(forms.ModelForm):
#     term_id = forms.ChoiceField(
#         required=False, choices=Term.objects.values_list("id", "term")
#     )

#     class Meta:
#         model = WordTerm
#         fields = [
#             "id",
#             "word",
#             "term_id",
#             # "status",
#             # "commute",
#         ]
