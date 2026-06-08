import streamlit as st
from kafka import KafkaConsumer
import json
import pymongo
from pymongo import MongoClient
import pandas as pd
import time

# Konfiguracja
KAFKA_TOPIC = 'alarms-topic'
KAFKA_BROKER = 'localhost:9092'
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'anomaly_db'
MONGO_COLLECTION = 'alarms'

# Inicjalizacja
st.set_page_config(page_title="Dashboard Alarmów", layout="wide")

@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

@st.cache_resource
def get_mongo_client():
    client = MongoClient(MONGO_URI)
    return client

def save_to_mongodb(alarm, db):
    collection = db[MONGO_COLLECTION]
    collection.insert_one(alarm)

def fetch_all_alarms(db):
    return list(db[MONGO_COLLECTION].find({}, {'_id': 0}))

def main():
    st.title("Detektor Anomalii - Panel Alarmów")

    # Inicjalizacja połączeń
    consumer = get_kafka_consumer()
    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DB]

    # Placeholder dla dynamicznej zawartości
    live_feed_placeholder = st.empty()
    table_placeholder = st.empty()

    # Pętla do odświeżania danych
    while True:
        # 1. Konsumuj nowe alarmy z Kafki i zapisz do MongoDB
        new_alarms = []
        # Ustawiamy timeout, aby aplikacja nie blokowała się w oczekiwaniu na wiadomości
        messages = consumer.poll(timeout_ms=1000, max_records=50)
        if messages:
            for tp, msg_list in messages.items():
                for msg in msg_list:
                    alarm_data = msg.value
                    new_alarms.append(alarm_data)
                    save_to_mongodb(alarm_data, db)
        
        # 2. Wyświetl ostatni alarm (Live Feed)
        if new_alarms:
            with live_feed_placeholder.container():
                st.subheader("Ostatni Wykryty Alarm")
                st.json(new_alarms[-1])

        # 3. Wyświetl historię alarmów z MongoDB
        all_alarms = fetch_all_alarms(db)
        if all_alarms:
            df = pd.DataFrame(all_alarms).sort_values(by='card_id', ascending=False)
            
            # Poprawki wizualne dla DataFrame
            if 'speed_kmh' in df.columns:
                df['speed_kmh'] = df['speed_kmh'].fillna(0).astype(int)
            if 'amount' in df.columns:
                df['amount'] = df['amount'].map('{:,.2f} PLN'.format)
            
            with table_placeholder.container():
                st.subheader("Historia Wykrytych Anomalii")
                st.dataframe(df, use_container_width=True)

        time.sleep(2) # Odświeżaj co 2 sekundy

if __name__ == "__main__":
    main()
