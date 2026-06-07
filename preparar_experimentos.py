import boto3
import json
import sys
import math

def main():
    try:
        with open('emr_cluster_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ emr_cluster_info.json no existe. levantar el clúster.")
        sys.exit(1)

    bucket = info['s3_bucket']
    region = 'us-east-1'
    s3 = boto3.client('s3', region_name=region)

    def copiar_directorio(prefijo_origen, prefijo_destino):
        print(f"  Copiando {prefijo_origen} -> {prefijo_destino}")
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefijo_origen):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('/'): 
                        continue
                    
                    origen = obj['Key']
                    destino = origen.replace(prefijo_origen, prefijo_destino, 1)
                    
                    # Usamos s3.copy en lugar de s3.copy_object porque copy() maneja automáticamente 
                    # archivos de más de 5 GB (como tu wikipedia de 7GB) usando Multipart Copy.
                    s3.copy(
                        {'Bucket': bucket, 'Key': origen},
                        bucket,
                        destino
                    )

    print("==================================================")
    print(" Preparando Escalas Crudas (Para WordCount)")
    
    copiar_directorio('input/novelas/', 'input_escala/crudos_25/novelas/')
    
    copiar_directorio('input/novelas/', 'input_escala/crudos_75/novelas/')
    copiar_directorio('input/comentarios/', 'input_escala/crudos_75/comentarios/')
    copiar_directorio('input/corpus/', 'input_escala/crudos_75/corpus/')

    copiar_directorio('input/novelas/', 'input_escala/crudos_100/novelas/')
    copiar_directorio('input/comentarios/', 'input_escala/crudos_100/comentarios/')
    copiar_directorio('input/corpus/', 'input_escala/crudos_100/corpus/')
    copiar_directorio('input/wikipedia/', 'input_escala/crudos_100/wikipedia/')


    print("\n==================================================")
    print("Preparando Escalas CSV (Para Índice Invertido)")
    
    paginator = s3.get_paginator('list_objects_v2')
    csv_files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix='output/csv_limpios/part-'):
        if 'Contents' in page:
            for obj in page['Contents']:
                csv_files.append(obj['Key'])
    
    if not csv_files:
        print("\nNo se encontraron archivos CSV.")
        print(" no has ejecutado 'lanzar_job_hadoop.py'.")
    else:
        csv_files.sort()
        total_csv = len(csv_files)
        
        limite_25 = max(1, math.floor(total_csv * 0.25))
        limite_75 = max(1, math.floor(total_csv * 0.75))
        
        print(f"hay {total_csv} archivos CSV ")
        
        def copiar_lista_archivos(lista, prefijo_destino):
            for origen in lista:
                nombre_archivo = origen.split('/')[-1]
                destino = f"{prefijo_destino}{nombre_archivo}"
                s3.copy(
                    {'Bucket': bucket, 'Key': origen},
                    bucket,
                    destino
                )
        
        copiar_lista_archivos(csv_files[:limite_25], 'input_escala/csv_25/')
        copiar_lista_archivos(csv_files[:limite_75], 'input_escala/csv_75/')
        copiar_lista_archivos(csv_files, 'input_escala/csv_100/')
        
        print("\n✅ Carpetas de experimentos preparadas ")

if __name__ == '__main__':
    main()
