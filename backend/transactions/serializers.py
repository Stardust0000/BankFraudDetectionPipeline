from rest_framework import serializers
from .models import Transaction
class TransactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Transaction
        fields = '__all__'
        # user_id = serializers.CharField()
        # amount = serializers.FloatField(min_value = 1)
        # location = serializers.CharField()
        # merchant = serializers.CharField()
        # timestamp = serializers.DateTimeField()