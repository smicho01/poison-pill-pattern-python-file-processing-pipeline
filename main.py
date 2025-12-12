import queue
import threading
import time
import random
from uuid import UUID, uuid4

s3_queue = queue.Queue()  # dispatcher → s3 uploaders
api_queue = queue.Queue()  # s3 uploaders → api uploaders
verify_queue = queue.Queue()  # used to verify if all uploads are ok; api uploaders -> verifier
DONE = None

# Simulated "database" of files to process
# `upload_id` will be populated after API returns UUID after upload
# `dest_key` will be populated after S3 -> S3 copy
FILES = [
    {"id": 1, "name": "file_100.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project1/uuid1",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "100", "type": "project1"}, "upload_id": "", "status": "READY"},
    {"id": 2, "name": "file_101.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project1/uuid2",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "101", "type": "project1"}, "upload_id": "", "status": "READY"},
    {"id": 3, "name": "file_102.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project1/uuid3",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "102", "type": "project1"}, "upload_id": "", "status": "READY"},
    {"id": 4, "name": "file_103.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project2/uuid4",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "103", "type": "project2"}, "upload_id": "", "status": "READY"},
    {"id": 5, "name": "file_104.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project2/uuid5",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "104", "type": "project2"}, "upload_id": "", "status": "READY"},
    {"id": 6, "name": "file_105.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project3/uuid6",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "105", "type": "project3"}, "upload_id": "", "status": "READY"},
    {"id": 7, "name": "file_106.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project3/uuid7",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "106", "type": "project3"}, "upload_id": "", "status": "READY"},
    {"id": 8, "name": "file_107.pdf",
     "src_bucket": "src-bucket-proj", "src_key": "project3/uuid8",
     "dest_bucket": "dest-bucket-proj", "dest_key": "",
     "meta": {"fileId": "107", "type": "project3"}, "upload_id": "", "status": "READY"},
]


def dispatcher(files):
    """Stage 1: Dispatch files to S3 upload queue"""
    # put each file into s3_queue
    for file in files:
        time.sleep(random.uniform(0.1, 0.6))
        print(f"🔖 Dispatcher: dispatching file {file["name"]}")
        s3_queue.put(file)


def s3_uploader(name):
    """Stage 2: Copy file S3 → S3 (fast: 0.2-0.5s)"""
    # get file from s3_queue
    while True:
        file = s3_queue.get()
        if file is DONE:
            print(f"☑️ Uploader {name} is DONE. Finishing work ... Finished")
            break

        # simulate S3 copy, add "s3_key" to the file dict
        time.sleep(random.uniform(0.1, 0.5))
        print(f"    Uploader {name} copy file {file['name']} S3 -> S3")
        upload_key = copy_file_s3_s3(file)

        # Update file dict
        file['dest_key'] = upload_key
        file['status'] = "S3_COPIED"

        # put result into api_queue
        api_queue.put(file)
        print(
            f"💾 Uploader {name} stored file [{file['name']}] in S3 bucket=[{file['dest_bucket']}] and key=[{file['dest_key']}]")


def api_uploader(name):
    """Stage 3: Upload metadata to API (slow: 1-2s)"""
    # Cet from api_queue
    while True:
        file = api_queue.get()
        if file is DONE:
            print(f"☑️  - 🔐 API Uploader {name} got DONE signal. Finishing ... Finished ")
            break
        # Simulate slow API call
        time.sleep(random.randint(1, 2))
        file['status'] = "API_UPLOADED"
        file['upload_id'] = upload_file_to_api(file)  # Update field with UUID returned from [pseudo] API upload
        print(f"🔐 API Uploader: file [{file['name']}] uploaded ! API_UPLOADED")
        verify_queue.put(file)  # put file into verifier queue so that verifier can check if all correct


def verifier(expected_count):
    """Collect all processed files, verify all succeeded"""
    processed = []

    while True:
        file = verify_queue.get()
        if file is DONE:
            break
        processed.append(file)
        print(f"✓ Verified: {file['name']} - {file['status']}")

    # Final report
    success = [f for f in processed if f['status'] == "API_UPLOADED"]
    failed = [f for f in processed if f['status'] != "API_UPLOADED"]

    print(f"\n=== VERIFICATION REPORT ===")
    print(f"Expected: {expected_count}")
    print(f"Processed: {len(processed)}")
    print(f"Success: {len(success)}")
    print(f"Failed: {len(failed)}")

    if len(processed) != expected_count:
        print(f"⚠️  MISSING FILES: {expected_count - len(processed)}")


# Pseudocode only; Helper function
def copy_file_s3_s3(file):
    """
    Simulate S3 to S3 copy.
    Returns the destination key where file was copied.

    In real code, this would call:
        s3_client.copy_object(
            CopySource={"Bucket": file["src_bucket"], "Key": file["src_key"]},
            Bucket=target_bucket,
            Key=dest_key
        )
    """
    # Simulate copy time
    time.sleep(random.uniform(0.2, 0.5))

    # Generate destination key (timestamp + uuid)
    timestamp = time.strftime("%Y/%m/%d/%H/%M/%S")
    dest_key = f"{timestamp}/{uuid4()}"

    # In real code: perform actual S3 copy here

    return dest_key


def upload_file_to_api(file):
    """ Uploading file metadata to the REST API [pseudocode]"""
    # In real code: perform actual API upload
    # Here we are just extracting UUID from the S3 upload key to have the same ID in S3 and in DB (after API upload)
    file_uuid = UUID(file['dest_key'].rsplit("/", 1)[-1])
    return file_uuid  # Let's assume the API returned ID of the uploaded file


# Workers
s3_workers = ["S3-Worker-1", "S3-Worker-2", "S3-Worker-3"]
api_workers = ["API-Worker-1", "API-Worker-2"]

# Create threads
dispatcher_thread = threading.Thread(target=dispatcher, args=(FILES,))
s3_uploader_threads = [threading.Thread(target=s3_uploader, args=(name,)) for name in s3_workers]
api_uploader_threads = [threading.Thread(target=api_uploader, args=(name,)) for name in api_workers]
verifier_thread = threading.Thread(target=verifier, args=(len(FILES),))

# Start ALL threads
for t in [dispatcher_thread] + s3_uploader_threads + api_uploader_threads + [verifier_thread]:
    t.start()

# Wait for dispatcher to finish
dispatcher_thread.join()

# NOW send DONE to s3_queue (dispatcher is done)
for _ in range(len(s3_workers)):
    s3_queue.put(DONE)

# Wait for S3 workers to finish
for t in s3_uploader_threads:
    t.join()

# NOW send DONE to api_queue (S3 workers are done)
for _ in range(len(api_workers)):
    api_queue.put(DONE)

# Wait for API workers to finish
for t in api_uploader_threads:
    t.join()

verify_queue.put(DONE)  # only 1 verifier , so only 1 x DONE

verifier_thread.join()

print("\n=== Pipeline complete ===")
