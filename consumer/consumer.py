from kafka import KafkaConsumer
import json
from datetime import datetime,timedelta
from collections import defaultdict
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_db"]
collection = db["transactions"]

# In-memory storage
user_amount = defaultdict(list)
user_last_city = {}
user_last_city_time = {}
user_recent_transactions = defaultdict(list)

consumer = KafkaConsumer(
    'transactions', #topic name, just a string, talks to Kafka
    bootstrap_servers='localhost:9092', #glue connecting consumer n producer
    auto_offset_reset='earliest',
    value_deserializer=lambda x:json.loads(x.decode('utf-8'))
)
print("Listening to transactions...\n")
print("="*60)

# Fraud Rules 

# 1: High amount
def check_high_amount(user_id,amount):
    amounts = user_amount[user_id]
    if len(amounts) >=3:
        avg = sum(amounts) / len(amounts)
        if amount > (3*avg):
            return True, f"Amount Rs.{amount} is more than 3x user average"
    return False,""
    
# 2. Location anomaly
def check_location_anomaly(user_id, city, transaction_time):
    if user_id in user_last_city:
        last_city = user_last_city[user_id]
        last_time = user_last_city_time[user_id]
        time_diff = transaction_time - last_time
        if last_city != city and time_diff<=timedelta(minutes=10):
            return True, f"City changed from {last_city} to {city} within 10mins"
    return False,""

# 3. velocity 
def check_velocity(user_id, transaction_time):
    recent = user_recent_transactions[user_id]
    recent = [time for time in recent if transaction_time-time <= timedelta(minutes=1)]
    user_recent_transactions[user_id] = recent
    if len(recent) >= 3:
        # print(len(recent))
        return True, f"{len(recent)} transactions in the last 1 minute"
    return False,""

# Update user data after each transaction
def update_user_data(user_id, amount, city, transaction_time):
    user_amount[user_id].append(amount)
    user_last_city[user_id] = city
    user_last_city_time[user_id] = transaction_time
    user_recent_transactions[user_id].append(transaction_time)

# Listening to kafka
for msg in consumer:
    transaction = msg.value
    user_id = transaction['user_id']
    amount = transaction['amount']
    city = transaction['location']
    timestamp = transaction['timestamp']
    timestamp = timestamp.replace("Z","")
    transaction_time = datetime.fromisoformat(timestamp)

    print(f"Transaction Received:")
    print(f"  ID       : {transaction['id']}")
    print(f"  User     : {user_id}")
    print(f"  Amount   : ₹{amount}")
    print(f"  Merchant : {transaction['merchant']}")
    print(f"  City     : {city}")
    print(f"  Time     : {timestamp}")

    fraud_detected = False
    reasons = []

    is_fraud, reason = check_high_amount(user_id, amount)
    if is_fraud:
        fraud_detected = True
        reasons.append(reason)
        print(f" Fraud Alert - Rule 1 High Amount: {reason}")
    
    is_fraud, reason = check_location_anomaly(user_id, city,transaction_time)
    if is_fraud:
        fraud_detected = True
        reasons.append(reason)
        print(f" Fraud Alert - Rule 2 Location Anomaly: {reason}")
    
    is_fraud, reason = check_velocity(user_id, transaction_time)
    if is_fraud:
        fraud_detected = True
        reasons.append(reason)
        print(f" Fraud Alert - Rule 3 Velocity Check: {reason}")
    
    if not fraud_detected:
        print(f"Transaction is clean")

    doc = {
        "user_id":user_id,
        "amount":amount,
        "location":city,
        "timestamp":timestamp,
        "fraud": fraud_detected,
        "reasons":reasons
    }

    collection.insert_one(doc)
    update_user_data(user_id,amount,city, transaction_time)
    print("-" * 60)