import boto3
import json
import time

def enviar_step():
    try:
        with open('emr_cluster_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("Error: No se encontró emr_cluster_info.json")
        return

    cluster_id = info['cluster_id']
    bucket = info['s3_bucket']
    region = 'us-east-1'

    ds3_cliente = boto3.client('s3', region_name=region)
    emr_cliente = boto3.client('emr', region_name=region)

    script_local = 'spark_wordcount_df.py'
    script_s3_key = 'scripts/spark_wordcount_df.py'
    
    print(f"Subiendo {script_local} a s3://{bucket}/{script_s3_key}...")
    ds3_cliente.upload_file(script_local, bucket, script_s3_key)
    print("✅ Script subido correctamente.")

    
    ruta_entrada = f"s3://{bucket}/input_escala/crudos_75/*/*" 
    ruta_salida = f"s3://{bucket}/output/spark_wordcount_75/"

    step_config = {
        'Name': 'spark_wordCount',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                f's3://{bucket}/{script_s3_key}',
                ruta_entrada,
                ruta_salida
            ]
        }
    }

    print(f"Enviando Step al clúster {cluster_id}...")
    response = emr_cliente.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step_config]
    )
    
    step_id = response['StepIds'][0]
    print(f"✅ Step enviado exitosamente. Step ID: {step_id}")

if __name__ == '__main__':
    enviar_step()
