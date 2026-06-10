import boto3
import json
import sys
from datetime import datetime, timedelta

def main():
    try:
        with open('emr_cluster_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ Error: emr_cluster_info.json no existe.")
        sys.exit(1)

    cluster_id = info['cluster_id']
    region = 'us-east-1'
    emr = boto3.client('emr', region_name=region)
    cw = boto3.client('cloudwatch', region_name=region)

    print(f"Extrayendo métricas de {cluster_id}...\n")
    
    try:
        response = emr.list_steps(ClusterId=cluster_id)
        steps = response.get('Steps', [])
        
        if not steps:
            print("No se encontraron trabajos (Steps).")
            return

        print(f"{'Nombre del Trabajo':<42} | {'Estado':<10} | {'Duración (seg)':<15} | {'Vcore-seconds':<15} | {'MB-seconds'}")
        print("-" * 110)
        
        for step in steps:
            name = step['Name']
            state = step['Status']['State']
            
            duracion_s = "N/A"
            vcore_sec = "N/A"
            mb_sec = "N/A"
            timeline = step['Status']['Timeline']
            
            if state in ['COMPLETED', 'FAILED'] and 'EndDateTime' in timeline:
                # Usamos StartDateTime para no contar el tiempo que estuvo en la cola esperando
                start = timeline.get('StartDateTime', timeline.get('CreationDateTime'))
                end = timeline['EndDateTime']
                
                duracion = end - start
                segundos_totales = int(duracion.total_seconds())
                duracion_s = f"{segundos_totales} s"

                max_cores = 0
                max_ram_mb = 0
                
                try:
                    cw_cores = cw.get_metric_statistics(
                        Namespace='AWS/ElasticMapReduce',
                        MetricName='ContainerAllocated',
                        Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
                        StartTime=start - timedelta(minutes=5),
                        EndTime=end + timedelta(minutes=5),
                        Period=300,
                        Statistics=['Maximum']
                    )
                    if cw_cores['Datapoints']:
                        max_cores = max([dp['Maximum'] for dp in cw_cores['Datapoints']])
                        
                    cw_ram = cw.get_metric_statistics(
                        Namespace='AWS/ElasticMapReduce',
                        MetricName='MemoryAllocatedMB',
                        Dimensions=[{'Name': 'JobFlowId', 'Value': cluster_id}],
                        StartTime=start - timedelta(minutes=5),
                        EndTime=end + timedelta(minutes=5),
                        Period=300,
                        Statistics=['Maximum']
                    )
                    if cw_ram['Datapoints']:
                        max_ram_mb = max([dp['Maximum'] for dp in cw_ram['Datapoints']])
                        
                    if max_cores > 0:
                        vcore_sec = f"{int(max_cores * segundos_totales)}"
                    if max_ram_mb > 0:
                        mb_sec = f"{int(max_ram_mb * segundos_totales)}"
                except Exception as e:
                    pass
                
            print(f"{name:<42} | {state:<10} | {duracion_s:<15} | {vcore_sec:<15} | {mb_sec}")
            
    except Exception as e:
        print(f"❌ Error al obtener métricas: {e}")

if __name__ == '__main__':
    main()
