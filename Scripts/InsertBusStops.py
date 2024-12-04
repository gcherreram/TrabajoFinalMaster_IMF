# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 12:47:53 2024

@author: Gabo0
"""

import pandas as pd
import os
from pymongo import InsertOne
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ETIB_TFM"]

# Define la ruta de la carpeta que contiene los archivos Excel
folder_path = "../data/bus_stop"

# Lista todos los archivos en la carpeta que terminan en .xlsx
files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith(".xlsx")]

# Cargar la primera hoja de todos los archivos .xlsx
dataframes = [pd.read_excel(file, sheet_name=0) for file in files]

# Concatenar todos los DataFrames en uno solo
df = pd.concat(dataframes, ignore_index=True)

df.info()

df = df.drop(['log_replica','log_replica.1','X','Y','cenefa.1','zona_nueva_sitp.1','modulo_paradero.1'], axis=1)

df = df.dropna()

df.info()

df = df.astype(str)

df['longitud_paradero'] = df['longitud_paradero'].astype(float)
df['latitud_paradero'] = df['latitud_paradero'].astype(float)

# Crear la colección con validación de esquema
db.create_collection("bus_stops", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
                "cenefa", "objectid", "consecutivo_paradero",
               "zona_paradero", "nombre_paradero", "via_paradero",
               "direccion_paradero", "localidad_paradero", "consola_paradero",
               "panel_paradero", "audio_paradero", "longitud_paradero",
               "latitud_paradero", "coordenada_x_paradero", "coordenada_y_paradero",
               "globalid", "created_user", "created_date", "last_edited_user",
               "last_edited_date", "zona_nueva_sitp", "modulo_paradero"
        ],
        "properties": {
            "cenefa": {"bsonType": "string"},
            "objectid": {"bsonType": "string"},
            "consecutivo_paradero": {"bsonType": "string"},
            "zona_paradero": {"bsonType": "string"}, 
            "nombre_paradero": {"bsonType": "string"},
            "via_paradero": {"bsonType": "string"},
            "direccion_paradero": {"bsonType": "string"},
            "localidad_paradero": {"bsonType": "string"},
            "consola_paradero": {"bsonType": "string"},
            "panel_paradero": {"bsonType": "string"},
            "audio_paradero": {"bsonType": "string"},
            "longitud_paradero": {"bsonType": "double"},
            "latitud_paradero": {"bsonType": "double"},
            "coordenada_x_paradero": {"bsonType": "string"},
            "coordenada_y_paradero": {"bsonType": "string"},
            "globalid": {"bsonType": "string"},
            "created_user": {"bsonType": "string"},
            "created_date": {"bsonType": "string"},
            "last_edited_user": {"bsonType": "string"},
            "last_edited_date": {"bsonType": "string"},
            "zona_nueva_sitp": {"bsonType": "string"},
            "modulo_paradero": {"bsonType": "string"}
        }
    }
})

collection = db["bus_stops"]

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
collection.create_index([("cenefa", 1)])