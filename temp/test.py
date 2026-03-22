import boto3
import os
from dotenv import load_dotenv

load_dotenv()

endpoint = os.getenv("R2_ENDPOINT_URL", "")
key_id   = os.getenv("R2_ACCESS_KEY_ID", "")
secret   = os.getenv("R2_SECRET_ACCESS_KEY", "")
bucket   = os.getenv("R2_BUCKET_NAME", "")

print("=== Config chargee ===")
print(f"Endpoint : {endpoint}")
print(f"Key ID   : {key_id[:8]}..." if key_id else "Key ID   : MANQUANT")
print(f"Secret   : {secret[:4]}..." if secret else "Secret   : MANQUANT")
print(f"Bucket   : {bucket}")
print()

if not all([endpoint, key_id, secret, bucket]):
    print("ERREUR : variables manquantes dans .env")
    exit(1)

if "<" in endpoint or ">" in endpoint:
    print("ERREUR : retire les < > dans R2_ENDPOINT_URL dans ton .env")
    exit(1)

try:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name="auto",
    )
    print("Client S3 cree OK")

    resp = s3.list_objects_v2(Bucket=bucket)
    print(f"Bucket accessible — {resp.get('KeyCount', 0)} objets existants")

    s3.put_object(Bucket=bucket, Key="test/ping.txt", Body=b"ok")
    print("Upload test OK")

    s3.delete_object(Bucket=bucket, Key="test/ping.txt")
    print("Cleanup OK")

    print()
    print("=== R2 est pret ! ===")

except Exception as e:
    print(f"ERREUR : {e}")