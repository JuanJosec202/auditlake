"""Script de entrada para ejecutar el ingestor existente como AWS Glue Job (referencia: SIN EJECUTAR en AWS).

El job lee su configuración de variables de entorno (pipelines/auditlake/config.py) y Glue entrega parámetros
--CLAVE. Este script solo traslada esos parámetros (y las credenciales de Secrets Manager) a variables de entorno
y llama al mismo main que corre en Docker. Pasos de despliegue en conf/aws/README.md.
"""

import json
import os
import sys

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["OUTBOX_JDBC_URL", "OUTBOX_SECRET_ID", "OUTBOX_BATCH_SIZE"])
secret = json.loads(
    boto3.client("secretsmanager").get_secret_value(SecretId=args["OUTBOX_SECRET_ID"])["SecretString"]
)
os.environ.update(
    OUTBOX_JDBC_URL=args["OUTBOX_JDBC_URL"],
    OUTBOX_USER=secret["username"],
    OUTBOX_PASSWORD=secret["password"],
    OUTBOX_BATCH_SIZE=args["OUTBOX_BATCH_SIZE"],
)

from auditlake.main import main  # noqa: E402  (después de fijar el entorno)

sys.exit(main([]))
