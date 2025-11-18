# Week 12 Assignment: Real-Time Ride-Sharing Streaming System
A full real-time data streaming pipeline using **Apache Kafka**, **PostgreSQL**, and **Streamlit**, built for generating, ingesting, storing, and visualizing synthetic ride-sharing trip data.

This project implements an end-to-end streaming workflow:

1. **Producer** generates synthetic ride events.
2. **Kafka** receives and queues these events.
3. **Consumer** listens to Kafka and writes events into Postgres.
4. **Streamlit Dashboard** live-refreshes and visualizes ride activity in real time.

---

# Project Structure

```
week12assignment/
│
├── producer.py
├── consumer.py
├── dashboard.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Docker Services (Kafka + Postgres)

## docker-compose.yml
Defines:
- Kafka broker (KRaft mode) on port 9092
- PostgreSQL database on port 5432
- Persistent volume for storage

---

# Virtual Environment

A custom conda environment was created because Python 3.13 caused pandas/Cython build issues:

```
conda create -n week12env python=3.11
conda activate week12env
pip install -r requirements.txt
```

---

# Python Dependencies (`requirements.txt`)
Includes:
- streamlit
- pandas
- sqlalchemy
- psycopg2-binary
- kafka-python
- faker
- plotly

---

# Synthetic Ride Generator (`producer.py`)
Generates realistic synthetic ride events:
- pickup/dropoff area
- timestamps
- fare, distance
- surge multiplier
- driver/rider IDs

Sends JSON events into Kafka topic **rides**.

---

# Kafka Consumer (`consumer.py`)
Consumes messages from Kafka and inserts them into PostgreSQL:
- JSON decoding
- SQL inserts
- Automatic table creation
- Continuous streaming loop

---

# Real-Time Dashboard (`dashboard.py`)
Features:
- Auto-refresh using Streamlit `st.rerun()`
- Metrics (total rides, avg fare, avg distance)
- Recent rides table
- Plotly visualizations:
  - rides per minute
  - avg fare by pickup area
  - avg fare by dropoff area

---

# Demo Video
A `.mp4` demo is included in the repo showing the dashboard updating with real-time data.
https://github.com/josephong610/week12assignment/blob/main/video.mp4

---

# How to Run the System

### 1. Start backend services
```
docker compose up
```

### 2. Start producer
```
python producer.py
```

### 3. Start consumer
```
python consumer.py
```

### 4. Launch the dashboard
```
streamlit run dashboard.py
```

---

# Final Result
A fully functional real-time ride‑sharing data pipeline using Kafka + PostgreSQL + Streamlit, similar to simplified Uber/Lyft analytics systems.

