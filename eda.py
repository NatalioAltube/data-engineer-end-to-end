import pandas as pd

# Cargar el dataset con codificación correcta
data = pd.read_csv('data.csv', encoding='ISO-8859-1')

# Convertir 'InvoiceDate' a datetime
data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], format='%m/%d/%Y %H:%M')

# Convertir 'CustomerID' a string y convertir otros campos a string
data['CustomerID'] = data['CustomerID'].astype(str)
data['StockCode'] = data['StockCode'].astype(str)
data['Description'] = data['Description'].astype(str)

# Filtrar valores negativos en Quantity
data_cleaned = data[data['Quantity'] > 0]

# Eliminar duplicados
data_cleaned.drop_duplicates(inplace=True)

# Eliminar filas con valores nulos en campos importantes
data_cleaned.dropna(subset=['CustomerID', 'InvoiceDate', 'Quantity', 'UnitPrice'], inplace=True)

# Asegurarse de que 'Quantity' y 'UnitPrice' son numéricos
data_cleaned['Quantity'] = pd.to_numeric(data_cleaned['Quantity'], errors='coerce')
data_cleaned['UnitPrice'] = pd.to_numeric(data_cleaned['UnitPrice'], errors='coerce')

# Calcular los ingresos totales por fila
data_cleaned['Revenue'] = data_cleaned['Quantity'] * data_cleaned['UnitPrice']

# Extraer el mes de la columna 'InvoiceDate'
data_cleaned['Month'] = data_cleaned['InvoiceDate'].dt.month

# Guardar el dataset limpio
data_cleaned.to_csv('ecommerce_data_cleaned.csv', index=False)





