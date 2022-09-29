from django.urls import path

from . import views

app_name = 'ge'


urlpatterns = [
    #path('', views.IndexView.as_view(), name='index'),
    path('filter/<int:database_id>/', views.FilterView, name='filter'),
]
