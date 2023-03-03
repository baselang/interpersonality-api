from . import views
from django.urls import path
from django.contrib.staticfiles.urls import staticfiles_urlpatterns

urlpatterns = [
    path('', views.form_view, name='home'),

]

urlpatterns += staticfiles_urlpatterns()
