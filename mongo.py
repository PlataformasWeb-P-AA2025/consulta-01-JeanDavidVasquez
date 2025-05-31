import pandas as pd
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["mi_base_datos"]
coleccion = db["datos_excel"]

# Función para cargar datos desde un archivo Excel
def cargar_excel_a_mongo(ruta_archivo, anio):
    df = pd.read_excel(ruta_archivo)
    df["anio"] = anio  # Agregamos el año como campo adicional
    datos = df.to_dict(orient="records")
    coleccion.insert_many(datos)
    print(f"Datos de {anio} insertados correctamente.")

# Cargar archivos
cargar_excel_a_mongo("data/2022.xlsx", 2022)
cargar_excel_a_mongo("data/2023.xlsx", 2023)

# CONSULTAS

# Consulta 1: Contar cuántos registros hay por año
print("\nConsulta 1: Cantidad de registros por año")
for anio in [2022, 2023]:
    cantidad = coleccion.count_documents({"anio": anio})
    print(f"Año {anio}: {cantidad} registros")

# Consulta 2: Top 5 jugadores con más victorias en 2023
print("\nConsulta 2: Top 5 jugadores con más victorias en 2023")
pipeline = [
    {"$match": {"anio": 2023}},
    {"$group": {"_id": "$Winner", "victorias": {"$sum": 1}}},
    {"$sort": {"victorias": -1}},
    {"$limit": 5}
]
resultados = coleccion.aggregate(pipeline)

for jugador in resultados:
    print(f"{jugador['_id']}: {jugador['victorias']} victorias")




