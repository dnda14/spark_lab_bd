import boto3
import json
import time
import sys

# ✅
# ❌
# ⚔️
# 🔨
# 🔍
# ============================================================
# CONFIGURACIÓN
# ============================================================
REGION = 'us-east-1'
CLUSTER_NAME = 'cluster-bigdata-spark'
EMR_RELEASE = 'emr-7.2.0'           
KEY_NAME = 'vockey'                   

ROL_EMR = 'EMR_DefaultRole'
ROL_EC2 = 'EMR_EC2_DefaultRole'

TIPO_MASTER = 'm4.large'
TIPO_CORE = 'm4.large'
N_CORES = 4            

S3_BUCKET = 'mi-bucket-emr-wimc-bigdata'  

MANTENER_ACTIVO = True

def crear_bucket_si_no_existe(cliente_S3, nombre_bucket):
    try:
        cliente_S3.head_bucket(Bucket=nombre_bucket)
        print(f"Bucket '{nombre_bucket}' ya existe.")
    except cliente_S3.exceptions.ClientError:
        print(f"Creando bucket...")
        cliente_S3.create_bucket(Bucket=nombre_bucket)
        
        print(f"✅ Bucket '{nombre_bucket}' creado.")


def create_grupo_seguridad_SSH(cliente_EC2):
    '''Quien puede entrar,salir,puertos'''
    sg_nombre = 'emr-ssh-access'

    try:
        print(f"Creando Group ...")
        response = cliente_EC2.create_security_group(
            GroupName=sg_nombre,
            Description='para acceso SSH al cluster'
        )
        sg_id = response['GroupId']

        cliente_EC2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
            ]
        )
        print(f" Group  creado con SSH abierto ")

    except cliente_EC2.exceptions.ClientError as e:
        if 'InvalidGroup.Duplicate' in str(e):
            sgs = cliente_EC2.describe_security_groups(GroupNames=[sg_nombre])
            sg_id = sgs['SecurityGroups'][0]['GroupId']
            print(f" Group ya existe. Reutilizándolo...")
        else:
            raise e

    return sg_id

def crear_roles_emr():
    try:
        import subprocess
        print("Verificando roles IAM para EMR...")
        resultado = subprocess.run(
            ['aws', 'emr', 'create-default-roles', '--region', REGION],
            capture_output=True, text=True
        )
        if resultado.returncode == 0:
            print("✅ Roles IAM creados.")
        else:
            print("❌ Error en los roles.")
    except Exception as e:
        print(f"❌ No se pudo verificar roles: {e}")


def obtener_ips_nodos(cliente_EMR, cliente_EC2, cluster_id):
    print("🔍 Obteniendo IPs de todos los nodos del clúster.")

    groups = cliente_EMR.list_instance_groups(ClusterId=cluster_id)['InstanceGroups']
    resultado = {'master': {}, 'core': []}

    for g in groups:
        role = g['InstanceGroupType']  # MASTER o CORE
        instances = cliente_EMR.list_instances(
            ClusterId=cluster_id,
            InstanceGroupId=g['Id']
        )['Instances'] 

        for inst in instances:
            ec2_id = inst['Ec2InstanceId']
            # Obtener IPs desde EC2
            ec2_desc = cliente_EC2.describe_instances(InstanceIds=[ec2_id])
            ec2_inst = ec2_desc['Reservations'][0]['Instances'][0]

            datos = {
                'id': ec2_id,
                'ip_publica': ec2_inst.get('PublicIpAddress', ''),
                'ip_privada': ec2_inst.get('PrivateIpAddress', ''),
            }

            if role == 'MASTER':
                resultado['master'] = datos
                print(f"  [MASTER]  IP Pública: {datos['ip_publica']} | IP Privada: {datos['ip_privada']}")
            else:
                resultado['core'].append(datos)
                print(f"  [CORE]    IP Pública: {datos['ip_publica'] or 'N/A':15} | IP Privada: {datos['ip_privada']}")

    with open('emr_nodes_ips.json', 'w') as f:
        json.dump(resultado, f, indent=4)

    print("\nIPs guardadas en 'emr_nodes_ips.json'")
    return resultado


def crear_cluster_emr():
    print("=" * 60)
    print("CREANDO CLÚSTER ")
    print("=" * 60)
#935309171 
    cliente_EMR = boto3.client('emr', region_name=REGION)
    cliente_EC2 = boto3.client('ec2', region_name=REGION)
    cliente_S3 = boto3.client('s3', region_name=REGION)

    crear_bucket_si_no_existe(cliente_S3, S3_BUCKET)

    crear_roles_emr()

    sg_id = create_grupo_seguridad_SSH(cliente_EC2)

    print(f"\nLanzando clúster '{CLUSTER_NAME}'...")
    print(f"   Versión EMR: {EMR_RELEASE}")
    print(f"   Master: 1x {TIPO_MASTER}")
    print(f"   Core:   {N_CORES}x {TIPO_CORE}")
    print(f"   Bucket: s3://{S3_BUCKET}/")
    print(f"   SSH SG: {sg_id}")

    params = {
        'Name': CLUSTER_NAME,
        'ReleaseLabel': EMR_RELEASE,
        'Applications': [
            {'Name': 'Hadoop'},
            #{'Name': 'Hive'},
            {'Name': 'Spark'},
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': TIPO_MASTER,
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': TIPO_CORE,
                    'InstanceCount': N_CORES,
                },
            ],
            'Ec2KeyName': KEY_NAME,
            'KeepJobFlowAliveWhenNoSteps': MANTENER_ACTIVO,
            'TerminationProtected': False,
            'AdditionalMasterSecurityGroups': [sg_id],
            'AdditionalSlaveSecurityGroups': [sg_id],
        },
        'ServiceRole': ROL_EMR,
        'JobFlowRole': ROL_EC2,
        'VisibleToAllUsers': True,
        
    }

    response = cliente_EMR.run_job_flow(**params)
    cluster_id = response['JobFlowId']

    print(f"\n✅ Clúster creado con ID: {cluster_id}")

    while True:
        descripcion = cliente_EMR.describe_cluster(ClusterId=cluster_id)
        estado = descripcion['Cluster']['Status']['State']
        print(f"Estado actual: {estado}")

        if estado == 'WAITING':
            print("Clúster listo para recibir jobs!")
            break
        elif estado == 'RUNNING':
            print("¡Clúster en ejecución!")
            break
        elif estado in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            reason = descripcion['Cluster']['Status'].get('StateChangeReason', {})
            print(f"❌ El clúster terminó: {reason.get('Message', 'Sin detalle')}")
            sys.exit(1)

        time.sleep(30)

    master_dns = descripcion['Cluster'].get('MasterPublicDnsName', 'No disponible')

    nodos = obtener_ips_nodos(cliente_EMR, cliente_EC2, cluster_id)

    emr_info = {
        'cluster_id': cluster_id,
        'cluster_name': CLUSTER_NAME,
        'master_dns': master_dns,
        'estado': estado,
        's3_bucket': S3_BUCKET,
        'emr_release': EMR_RELEASE,
        'master_type': TIPO_MASTER,
        'core_type': TIPO_CORE,
        'core_count': N_CORES,
        'ssh_sg_id': sg_id,
    }

    with open('emr_cluster_info.json', 'w') as f:
        json.dump(emr_info, f, indent=4)

    print("\n" + "=" * 60)
    print("RESUMEN DEL CLÚSTER")
    print("=" * 60)
    print(f"  Cluster ID:     {cluster_id}")
    print(f"  Master DNS:     {master_dns}")
    print(f"  Estado:         {estado}")
    print(f"  Bucket S3:      s3://{S3_BUCKET}/")
    print(f"  Nodos Core:     {N_CORES}")
    print(f"  SSH SG:         {sg_id}")
    print(f"\n  Para conectarte por SSH:")
    print(f"  ssh -i labsuser.pem hadoop@{master_dns}")
    print(f"\n  Info guardada en: emr_cluster_info.json")
    print(f"  IPs de nodos en: emr_nodes_ips.json")
    print("=" * 60)


def terminar_cluster():
    import os
    try:
        with open('emr_cluster_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ No se encontró emr_cluster_info.json")
        print("   aws emr terminate-clusters --cluster-ids <CLUSTER_ID>")
        sys.exit(1)

    cluster_id = info['cluster_id']
    cliente_EMR = boto3.client('emr', region_name=REGION)

    try:
        print(f"Terminando clúster {cluster_id}...")
        cliente_EMR.terminate_job_flows(JobFlowIds=[cluster_id])
        print("✅ Señal de terminación enviada.")

        for archivo in ['emr_cluster_info.json', 'emr_nodes_ips.json']:
            if os.path.exists(archivo):
                os.remove(archivo)
                print(f"Archivo local '{archivo}' eliminado ")

    except Exception as e:
        print(f"❌ Error al intentar terminar el clúster: {e}")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--terminar':
        terminar_cluster()
    else:
        crear_cluster_emr()


''' 'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 150
                                },
                                'VolumesPerInstance': 1
                            }
                        ],
                        'EbsOptimized': True
                    }'''
