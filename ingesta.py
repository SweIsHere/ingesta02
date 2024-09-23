import boto3
import mysql.connector
import csv
from mysql.connector import Error

mysql_config = {
    'host': '52.45.141.206',
    'port': 8005,
    'user': 'root',
    'password': 'utec',
    'database': 'tienda'
}

ficheroUpload = "tienda_sql.csv"
nombreBucket = "ingesta-output"
nombreTabla = "fabricantes"

try:
    conexion = mysql.connector.connect(**mysql_config)

    if conexion.is_connected():
        print("Conexión exitosa a la base de datos MySQL")

        cursor = conexion.cursor()
        cursor.execute(f"SELECT * FROM {nombreTabla}")
        registros = cursor.fetchall()
        columnas = [i[0] for i in cursor.description]

        with open(ficheroUpload, mode='w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerow(columnas)
            escritor_csv.writerows(registros)

        print(f"Archivo {ficheroUpload} generado correctamente con los datos de la tabla {nombreTabla}")

        s3 = boto3.client('s3')
        s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
        print(f"Archivo {ficheroUpload} subido a S3 correctamente")

except Error as e:
    print(f"Error al conectar a MySQL o al generar el CSV: {e}")

finally:
    if conexion.is_connected():
        cursor.close()
        conexion.close()
        print("Conexión a MySQL cerrada")
