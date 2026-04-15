# 🚀 Real-Time Bank Transaction Fraud Detection Pipeline

## 🧠 Problem

Traditional banking systems detect fraud in batch mode, causing delays in identifying suspicious transactions. This can lead to financial losses before fraud is flagged.

This project builds a **real-time fraud detection pipeline** that flags suspicious transactions instantly using a streaming architecture.

---

## ⚙️ Architecture

Django REST API → Kafka → Python Consumer → MongoDB

---

## 🛠️ Tech Stack

* **Django REST Framework** — Transaction ingestion API
* **Apache Kafka** — Real-time event streaming
* **Python** — Stream processing & fraud detection
* **MongoDB** — Storage for processed transactions
* **Docker** — Containerized infrastructure

---

## 🔥 Features

* Real-time transaction ingestion via REST API
* Kafka-based streaming pipeline
* Stateful fraud detection:

  * High amount anomaly (3× average)
  * Velocity check (multiple transactions in short time)
  * Location anomaly
* Sliding window processing
* MongoDB persistence

---

## 🧪 Sample Output

Fraud Alert - High Amount
Fraud Alert - Velocity

---

## ▶️ How to Run

### 1. Start Kafka & MongoDB

cd docker
docker-compose up -d

### 2. Run Consumer

cd ../consumer
python consumer.py

### 3. Run Django API

cd ../backend
python manage.py runserver

---

## 📌 Future Improvements

* Add Kafka fraud-alerts topic for real-time notifications
* Build dashboard for analytics
* Deploy using cloud infrastructure

---
