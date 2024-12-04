
import pandas as pd
import os
from pymongo import InsertOne
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ETIB_TFM"]

# Define la ruta de la carpeta que contiene los archivos CSV
folder_path = "../data/transactions"

# Lista todos los archivos en la carpeta que terminan en .csv
files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".csv")]

# Cargar todos los CSV
dataframes = [pd.read_csv(file) for file in files]

# Concatenar todos los DataFrames en uno solo
df = pd.concat(dataframes, ignore_index=True)

df['Fecha_Transaccion'] = pd.to_datetime(df['Fecha_Transaccion'])

df['cenefa'] = df['Estacion_Parada'].str.split(' ').apply(lambda x: x[1] if len(x) > 1 else x).apply(
    lambda x: x.split('|')[0] if isinstance(x, str) and '|' in x else x).apply(
    lambda x: x.split('_')[0] if isinstance(x, str) and '_' in x else x)

# Filtrar filas donde 'Estacion_Parada' no contenga "Unknown"
df = df[~df['Estacion_Parada'].str.contains("Unknown", na=False)]

# Convertir todas las columnas de tipo 'object' en string y manejar listas
df[df.select_dtypes(include=['object']).columns] = df[df.select_dtypes(include=['object']).columns].applymap(lambda x: x[0] if isinstance(x, list) else str(x))

# Convertir las columnas numéricas a string
df[['ID_Vehiculo', 'Tipo_Tarifa', 'Dispositivo']] = df[['ID_Vehiculo', 'Tipo_Tarifa', 'Dispositivo']].astype(str)

# Crear la colección con validación de esquema
db.create_collection("transactions", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "Dispositivo", "Emisor", "Estacion_Parada", "Fase", "Fecha_Clearing", 
            "Fecha_Transaccion", "Hora_Pico_SN", "ID_Vehiculo", "Linea", "Nombre_Perfil", 
            "Numero_Tarjeta", "Operador", "Ruta", "Saldo_Despues_Transaccion", 
            "Saldo_Previo_a_Transaccion", "Sistema", "Tipo_Tarifa", "Tipo_Tarjeta", 
            "Tipo_Vehiculo", "Valor", "archivo", "cenefa"
        ],
        "properties": {
            "Dispositivo": {"bsonType": "string"},
            "Emisor": {"bsonType": "string"},
            "Estacion_Parada": {"bsonType": "string"},
            "Fase": {"bsonType": "string"},
            "Fecha_Clearing": {"bsonType": "string"},  # Solo la fecha
            "Fecha_Transaccion": {"bsonType": "date"},  # Fecha y hora (timestamp)
            "Hora_Pico_SN": {"bsonType": "string"},
            "ID_Vehiculo": {"bsonType": "string"},
            "Linea": {"bsonType": "string"},
            "Nombre_Perfil": {"bsonType": "string"},
            "Numero_Tarjeta": {"bsonType": "string"},
            "Operador": {"bsonType": "string"},
            "Ruta": {"bsonType": "string"},
            "Saldo_Despues_Transaccion": {"bsonType": "double"},
            "Saldo_Previo_a_Transaccion": {"bsonType": "double"},
            "Sistema": {"bsonType": "string"},
            "Tipo_Tarifa": {"bsonType": "string"},
            "Tipo_Tarjeta": {"bsonType": "string"},
            "Tipo_Vehiculo": {"bsonType": "string"},
            "Valor": {"bsonType": "double"},
            "archivo": {"bsonType": "string"},
            "cenefa": {"bsonType": "string"}
        }
    }
})

collection = db["transactions"]

# Tamaño del lote
batch_size = 10000

# Convierte el DataFrame a una lista de diccionarios
data_dict = df.to_dict("records")

# Inserta los documentos en lotes
for i in range(0, len(data_dict), batch_size):
    batch = data_dict[i:i + batch_size]
    requests = [InsertOne(doc) for doc in batch]
    try:
        result = collection.bulk_write(requests)
        print(f'Batch {i//batch_size + 1} inserted successfully.')
    except Exception as e:
        print(f'Error inserting batch {i//batch_size + 1}: {e}')


# Crear índice en la columna Fecha_Clearing (solo fecha)
collection.create_index([("Fecha_Clearing", 1)])

# Crear índice en la columna Fecha_Transaccion (timestamp)
collection.create_index([("Fecha_Transaccion", 1)])

# Crear índice en la columna cenefa
collection.create_index([("cenefa", 1)])

# Crear índice en la columna cenefa
collection.create_index([("Ruta", 1)])
