import boto3
import mysql.connector
from mysql.connector import Error

# Parámetros de conexión a MySQL
mysql_config = {
    'host': '52.45.141.206',
    'port': 8005,
    'user': 'root',
    'password': 'utec',
    'database': 'tienda'
}

# Parámetros de S3
ficheroUpload = "tienda_sql.csv"
nombreBucket = "gcr-output-01"

# Conectar a MySQL
try:
    conexion = mysql.connector.connect(**mysql_config)

    if conexion.is_connected():
        print("Conexión exitosa a la base de datos MySQL")
        cursor = conexion.cursor()
        s3 = boto3.client('s3')
        response = s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
        print("Archivo subido a S3 correctamente")

except Error as e:
    print("Error al conectar a MySQL:", e)

finally:
    if conexion.is_connected():
        cursor.close()
        conexion.close()
        print("Conexión a MySQL cerrada")
