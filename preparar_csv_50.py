import boto3
import json
import sys
import math

def main():
    # 1. Leer información del clúster
    try:
        with open('emr_cluster_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ emr_cluster_info.json no existe. Asegúrate de levantar el clúster primero.")
        sys.exit(1)

    bucket = info['s3_bucket']
    s3 = boto3.client('s3', region_name='us-east-1')

    print("==================================================")
    print("Preparando Escala del 50% CSV (Para Índice Invertido)")
    print("==================================================\n")
    
    # 2. Listar todos los archivos CSV limpios
    paginator = s3.get_paginator('list_objects_v2')
    csv_files = []
    
    print("Buscando archivos CSV originales en S3...")
    for page in paginator.paginate(Bucket=bucket, Prefix='output/csv_limpios/part-'):
        if 'Contents' in page:
            for obj in page['Contents']:
                csv_files.append(obj['Key'])
    
    if not csv_files:
        print("\n❌ No se encontraron archivos CSV. Verifica que la ruta 'output/csv_limpios/' exista.")
        sys.exit(1)
        
    csv_files.sort() # Ordenar para siempre tomar la misma primera mitad
    total_csv = len(csv_files)
    
    # 3. Calcular la mitad exacta
    limite_50 = max(1, math.floor(total_csv * 0.50))
    print(f"📊 Se encontraron un total de {total_csv} archivos CSV.")
    print(f"🔪 Se copiarán {limite_50} archivos (El 50% exacto).\n")
    
    # 4. Copiar los archivos a la nueva carpeta
    prefijo_destino = 'input_escala/csv_50/'
    archivos_a_copiar = csv_files[:limite_50]
    
    for i, origen in enumerate(archivos_a_copiar, 1):
        nombre_archivo = origen.split('/')[-1]
        destino = f"{prefijo_destino}{nombre_archivo}"
        
        print(f"[{i}/{limite_50}] Copiando {nombre_archivo} -> {destino}")
        s3.copy(
            {'Bucket': bucket, 'Key': origen},
            bucket,
            destino
        )
        
    print("\n✅ ¡Listo! Tu carpeta 'input_escala/csv_50/' está lista en S3 para la prueba.")

if __name__ == '__main__':
    main()
