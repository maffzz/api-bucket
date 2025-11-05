import json
import uuid
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    body = event.get('body')
    if isinstance(body, str):
        body = json.loads(body)

    tenant_id = body['tenant_id']
    comentario = body['comentario']

    uuid_v1 = str(uuid.uuid1())
    timestamp = datetime.utcnow().isoformat()

    # insertar en DynamoDB
    table_name = os.environ['TABLE_NAME']
    table = dynamodb.Table(table_name)
    item = {
        'tenant_id': tenant_id,
        'uuid': uuid_v1,
        'comentario': comentario,
        'fecha_creacion': timestamp
    }
    table.put_item(Item=item)

    # guardar JSON en S3 (estrategia de Ingesta Push)
    bucket_name = os.environ['BUCKET_INGESTA']
    key = f"{tenant_id}/{uuid_v1}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(item),
        ContentType='application/json'
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            'mensaje': 'Comentario registrado correctamente',
            'uuid': uuid_v1,
            'bucket': bucket_name,
            'archivo': key
        })
    }
