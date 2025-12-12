import queue
import threading
import time
import random
import uuid

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
    # TODO: put each file into s3_queue
    # TODO: send DONE signals (how many?)
    pass


def s3_uploader(name):
    """Stage 2: Copy file S3 → S3 (fast: 0.2-0.5s)"""
    # TODO: get file from s3_queue
    # TODO: simulate S3 copy, add "s3_key" to the file dict
    # TODO: put result into api_queue
    # TODO: handle DONE - what do you forward to api_queue?
    pass


def api_uploader(name):
    """Stage 3: Upload metadata to API (slow: 1-2s)"""
    # TODO: get from api_queue
    # TODO: simulate slow API call
    # TODO: handle DONE - how many DONE signals to expect?
    pass


def verifier(expected_count):
    """Collect all processed files, verify all succeeded"""
    pass


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
    dest_key = f"{timestamp}/{uuid.uuid4()}"

    # In real code: perform actual S3 copy here

    return dest_key


# Workers
s3_workers = ["S3-Worker-1", "S3-Worker-2", "S3-Worker-3"]
api_workers = ["API-Worker-1", "API-Worker-2"]

# Create threads


# Start ALL threads


# Wait for dispatcher to finish


# NOW send DONE to s3_queue (dispatcher is done)


# Wait for S3 workers to finish


# NOW send DONE to api_queue (S3 workers are done)


# Wait for API workers to finish


print("\n=== Pipeline complete ===")
