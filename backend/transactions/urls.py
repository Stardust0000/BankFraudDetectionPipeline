from django.urls import path
from .views import create_transaction

urlpatterns = [
    path('transactions/',create_transaction),
]