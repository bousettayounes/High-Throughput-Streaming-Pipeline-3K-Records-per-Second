import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

schema_registry_conf = {'url': 'http://localhost:8081'}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_schema = {
  "type": "record",
  "name": "Transactions_schema",
  "fields": [
    {"name": "transactionId", "type": ["string", "null"]},
    {"name": "userId", "type": ["string", "null"]},
    {"name": "merchantId", "type": ["string", "null"]},
    {"name": "amount", "type": ["double", "null"]},
    {"name": "transactionTime", "type": ["long", "null"]},
    {"name": "transactionType", "type": ["string", "null"]},
    {"name": "location", "type": ["string", "null"]},
    {"name": "paymentMethod", "type": ["string", "null"]},
    {"name": "isInternational", "type": ["string", "null"]},
    {"name": "currency", "type": ["string", "null"]}
  ]
}

schema_str = json.dumps(avro_schema)

schema = Schema(schema_str, schema_type="AVRO")

try:

    schema_id = schema_registry_client.register_schema(subject_name="Financial_TRANSACTIONS_schema", schema=schema)
    print(f"Schema registered successfully. Schema ID: {schema_id}")

except Exception as e:

    print(f"Error registering schema: {e}")
try:

    registered_schema = schema_registry_client.get_latest_version("Financial_TRANSACTIONS_schema")

    print(f"Retrieved schema: {registered_schema.schema.schema_str}")

except Exception as e:
    print(f"Error retrieving schema: {e}")