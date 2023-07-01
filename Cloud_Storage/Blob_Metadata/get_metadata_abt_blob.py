from google.cloud import storage
from datetime import datetime

#"""Prints out a blob's metadata."""  gs://mybucket/Arguments.json
bucket_name = 'mybucket'
blob_name = 'Arguments.json'

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Retrieve a blob, and its metadata, from Google Cloud Storage.
# Note that `get_blob` differs from `Bucket.blob`, which does not
# make an HTTP request.
blob = bucket.get_blob(blob_name)

print(f"Blob: {blob.name}")
print(f"Bucket: {blob.bucket.name}")
print(f"Storage class: {blob.storage_class}")
print(f"ID: {blob.id}")
print(f"Size: {blob.size} bytes")
print(f"Updated: {blob.updated}")
print(f"Generation: {blob.generation}")
print(f"Metageneration: {blob.metageneration}")
print(f"Etag: {blob.etag}")
print(f"Owner: {blob.owner}")
print(f"Component count: {blob.component_count}")
print(f"Crc32c: {blob.crc32c}")
print(f"md5_hash: {blob.md5_hash}")
print(f"Cache-control: {blob.cache_control}")
print(f"Content-type: {blob.content_type}")
print(f"Content-disposition: {blob.content_disposition}")
print(f"Content-encoding: {blob.content_encoding}")
print(f"Content-language: {blob.content_language}")
print(f"Metadata: {blob.metadata}")
print(f"Medialink: {blob.media_link}")
print(f"Custom Time: {blob.custom_time}")
print("Temporary hold: ", "enabled" if blob.temporary_hold else "disabled")
print("Created_date :", datetime.fromtimestamp(blob.generation/(10**6)).date())
print("Created_datetime :", datetime.fromtimestamp(blob.generation/(10**6)))
print(
    "Event based hold: ",
    "enabled" if blob.event_based_hold else "disabled",
)
if blob.retention_expiration_time:
    print(
        f"retentionExpirationTime: {blob.retention_expiration_time}"
    )
