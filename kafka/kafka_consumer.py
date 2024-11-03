from kafka import KafkaConsumer
import pymysql
import json

# Configuración de la conexión a la base de datos MySQL
connection = pymysql.connect(
    host='localhost',
    port=3307, 
    user='natalio.altube@gmail.com',
    password='Holasi123456!',
    database='ecommerce_data'
)
cursor = connection.cursor()

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    'e-commerce-group',  # Nombre del tópico
    bootstrap_servers=['localhost:9092'],  # Servidor de Kafka
    auto_offset_reset='earliest',  # Comienza desde el principio del tópico
    enable_auto_commit=True,
    group_id='ecommerce-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Esperando mensajes del tópico 'e-commerce-group'...")

# Procesar cada mensaje recibido y almacenarlo en la base de datos
for message in consumer:
    event_data = message.value  # El mensaje recibido
    print(f"Nuevo evento recibido: {event_data}")

    # SQL para insertar datos en la tabla con los tipos de datos correctos
    sql = """
    INSERT INTO transactions (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, Revenue, Month)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        str(event_data['InvoiceNo']),  # String
        str(event_data['StockCode']),  # String
        str(event_data['Description']),  # String
        int(event_data['Quantity']),  # Integer
        event_data['InvoiceDate'],  # Datetime
        float(event_data['UnitPrice']),  # Float
        str(event_data['CustomerID']),  # String
        str(event_data['Country']),  # String
        float(event_data['Revenue']),  # Float
        int(event_data['Month'])  # Integer
    )

    cursor.execute(sql, values)
    connection.commit()

consumer.close()
connection.close()



