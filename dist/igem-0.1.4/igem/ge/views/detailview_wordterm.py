# from django.contrib import messages
# from django.http.response import Http404
# from django.shortcuts import redirect, render
# from django.urls import reverse
# from django.views import View
# from ge.forms import WordTermForm, WTForm
# from ge.models import WordTerm


# class WordTermView(View):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     def setup(self, *args, **kwargs):
#         return super().setup(*args, **kwargs)

#     def dispatch(self, *args, **kwargs):
#         return super().dispatch(*args, **kwargs)

#     def get_wordterm(self, id=None):
#         wordterm = None

#         if id is not None:
#             wordterm = WordTerm.objects.filter(pk=id).first()

#             if not wordterm:
#                 raise Http404()

#         return wordterm

#     def render_wordterm(self, form, wordterm):
#         return render(
#             self.request,
#             "ge/pages/detailview_wordterm.html",
#             context={"form": form, "wordterm": wordterm},
#         )

#     def get(self, request, id=None):
#         wordterm = self.get_wordterm(id)
#         form = WTForm(instance=wordterm)
#         return self.render_wordterm(form, wordterm)

#     def post(self, request, id=None):
#         wordterm = self.get_wordterm(id)
#         form = WTForm(
#             data=request.POST or None, files=request.FILES or None, instance=wordterm
#         )

#         if form.is_valid():
#             wordterm = form.save(commit=False)
#             # recipe.author = request.user
#             # recipe.preparation_steps_is_html = False
#             # recipe.is_published = False
#             wordterm.save()

#             messages.success(request, "wordterm Saved!")
#             return redirect(reverse("ge:edit_wordterm", args=(wordterm.id,)))

#         return self.render_wordterm(form)
