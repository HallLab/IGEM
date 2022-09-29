from django.http import HttpResponseRedirect, Http404
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views import generic
from django.utils import timezone

from ge.models import Database

# from .models import Choice, Question

def FilterView(request, database_id):
    try:
        dbs = Database.objects.get(pk=database_id)
    except Database.DoesNotExist:
        raise Http404("Database does not exist")
    return render(request, 'ge/filter.html', {'database': dbs})

