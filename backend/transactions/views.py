from django.shortcuts import render
import random
from .models import Transaction
from .serializers import TransactionSerializer
from rest_framework.decorators import api_view
from rest_framework.response import Response
from datetime import datetime

from .kafka_producer import send_transaction

cities = ["Bangalore","Delhi","Pune","Kolkata"]
merchants = ["Swiggy","Flipkart","Amazon","Uber"]
# user_ids = ['U101','U102','U103','U104']

@api_view(['GET','POST'])
def create_transaction(request):
    if request.data:
        serializer = TransactionSerializer(data = request.data)
        if serializer.is_valid():
            transaction = serializer.save()
            send_transaction(serializer.data)
            return Response(serializer.data, status=201)
        else:
            return Response(serializer.errors,status=400)
    # if no input data generate synthetic data:
    txn = {
        "user_id": f"U{random.randint(1, 6)}",
        "amount":random.randint(100,10000),
        "location":random.choice(cities),
        "merchant":random.choice(merchants),
        "timestamp":datetime.now().isoformat()
    }
    # create transaction and serializer it:
    transaction = Transaction.objects.create(**txn)
    serializer = TransactionSerializer(transaction)
    
    send_transaction(serializer.data)

    return Response(serializer.data, status=201)

