from django.db import models

class Transaction(models.Model):
    user_id = models.CharField(max_length=50)
    amount = models.FloatField()
    location = models.CharField(max_length=100)
    merchant = models.CharField(max_length=100)
    timestamp = models.DateTimeField()
