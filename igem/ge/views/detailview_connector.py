# from django.contrib import messages
# from django.http.response import Http404
# from django.shortcuts import redirect, render
# from django.urls import reverse
# from django.views import View
# from ge.forms import ConnectorForm
# from ge.models import Connector


# class ConnectorView(View):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#     def setup(self, *args, **kwargs):
#         return super().setup(*args, **kwargs)

#     def dispatch(self, *args, **kwargs):
#         return super().dispatch(*args, **kwargs)

#     def get_connector(self, id=None):
#         connector = None

#         if id is not None:
#             connector = Connector.objects.filter(pk=id).first()

#             if not connector:
#                 raise Http404()

#         return connector

#     def render_connector(self, form, connector):
#         return render(
#             self.request,
#             "ge/pages/detailview_connector.html",
#             context={"form": form, "connector": connector},
#         )

#     def get(self, request, id=None):
#         connector = self.get_connector(id)
#         form = ConnectorForm(instance=connector)
#         return self.render_connector(form, connector)

#     def post(self, request, id=None):
#         connector = self.get_connector(id)
#         form = ConnectorForm(
#             data=request.POST or None, files=request.FILES or None, instance=connector
#         )

#         if form.is_valid():
#             connector = form.save(commit=False)
#             # recipe.author = request.user
#             # recipe.preparation_steps_is_html = False
#             # recipe.is_published = False
#             connector.save()

#             messages.success(request, "Connector Saved!")
#             return redirect(reverse("ge:edit_connector", args=(connector.id,)))

#         return self.render_connector(form)
