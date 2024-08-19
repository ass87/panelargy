import requests
import mysql.connector
from datetime import datetime

# Paso 1: Hacer la solicitud a la API
url = "https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial/"
response = requests.get(url)
data = response.json()

# Paso 2: Conectar a la base de datos MySQL
conn = mysql.connector.connect(
    host="sommellerie-lp.com",
    user="blpyyxyg",
    password="J4sySu;HdLOa",
    database="blpyyxyg_panelargy"
)

cursor = conn.cursor()

# Paso 3: Iterar sobre cada registro y agregarlo o actualizarlo en la base de datos
for item in data:  # data es una lista de diccionarios
    # Verificar la estructura del elemento
    if isinstance(item, dict) and 'fecha' in item and 'venta' in item:
        try:
            fecha = datetime.strptime(item['fecha'], '%Y-%m-%d').date()
            valor = item['venta']
            
            # Insertar o actualizar en la base de datos
            cursor.execute("""
                INSERT INTO dolar (fecha, valor) 
                VALUES (%s, %s) 
                ON DUPLICATE KEY UPDATE valor = VALUES(valor)
            """, (fecha, valor))
        except ValueError as e:
            print(f"Error al procesar la fecha {item['fecha']}: {e}")
        except mysql.connector.Error as err:
            print(f"Error de MySQL: {err}")

# Confirmar la transacción y cerrar la conexión
conn.commit()
conn.close()

print("Datos insertados o actualizados correctamente.")
