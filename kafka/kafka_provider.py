from kafka import KafkaProducer
import pandas as pd
import json

# Cargar los datos limpios del CSV
data = pd.read_csv('C:/Users/natal/OneDrive/Escritorio/data_engineer_proyects/ecommerce_data_cleaned.csv', dtype={'InvoiceNo': str})

# Configurar el productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Enviar cada fila como un mensaje a Kafka
for _, row in data.iterrows():
    message = row.to_dict()
    producer.send('e-commerce-group', value=message)
    print(f"Enviando mensaje: {message}")

producer.flush()
producer.close()
