# from django.shortcuts import render
# from ge.models import Connector, WordTerm  # noqa E501


# def listview_connector(request):
#     connectors = Connector.objects.values(
#         "id",
#         "connector",
#         "datasource_id__datasource",
#         "update_ds",
#     )

#     return render(
#         request,
#         "ge/pages/listview_connector.html",
#         context={
#             "connectors": connectors,
#         },
#     )


# def listview_wordterm(request):
#     wordterms = WordTerm.objects.values(
#         "id",
#         "word",
#         "term_id",
#         "term_id__term",
#         "status",
#         "commute",
#     )

#     return render(
#         request,
#         "ge/pages/listview_wordterm.html",
#         context={
#             "wordterms": wordterms,
#         },
#     )
