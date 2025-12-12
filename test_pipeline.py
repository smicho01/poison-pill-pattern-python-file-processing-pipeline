from datetime import datetime
from uuid import UUID
import queue
import threading
from main import (
    copy_file_s3_s3,
    upload_file_to_api,
    dispatcher,
    s3_uploader,
    api_uploader,
    verifier,
    s3_queue,
    api_queue,
    verify_queue,
    DONE,
)


def test_copy_file_s3_s3_returns_valid_key():
    """Happy path: S3 copy returns a valid destination key with timestamp and UUID"""
    file = {
        "id": 1,
        "name": "test.pdf",
        "src_bucket": "src-bucket",
        "src_key": "project/file1",
    }

    before = datetime.now()
    dest_key = copy_file_s3_s3(file)
    after = datetime.now()

    # Should return string
    assert isinstance(dest_key, str)

    # Should have format: YYYY/MM/DD/HH/MM/SS/uuid
    parts = dest_key.split("/")
    assert len(parts) == 7

    # Validate timestamp is parseable and recent
    timestamp_str = "/".join(parts[:6])  # YYYY/MM/DD/HH/MM/SS
    timestamp = datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")

    # Timestamp should be between before and after
    assert before.replace(microsecond=0) <= timestamp <= after.replace(microsecond=0)

    # Last part should be valid UUID
    uuid_part = parts[-1]
    UUID(uuid_part)  # raises if invalid


def test_upload_file_to_api_returns_valid_uuid():
    """Happy path: API upload returns the UUID from dest_key"""
    file = {
        "id": 1,
        "name": "test.pdf",
        "dest_key": "2025/01/15/14/30/45/550e8400-e29b-41d4-a716-446655440000",
    }

    result = upload_file_to_api(file)

    # Should return UUID object
    assert isinstance(result, UUID)

    # Should match the UUID in dest_key
    assert str(result) == "550e8400-e29b-41d4-a716-446655440000"


def test_full_pipeline_processes_all_files():
    """Integration test: all files flow through the pipeline"""

    # Small test dataset
    test_files = [
        {"id": 1, "name": "test_1.pdf",
         "src_bucket": "src", "src_key": "p1/f1",
         "dest_bucket": "dest", "dest_key": "",
         "meta": {"fileId": "1"}, "upload_id": "", "status": "READY"},
        {"id": 2, "name": "test_2.pdf",
         "src_bucket": "src", "src_key": "p1/f2",
         "dest_bucket": "dest", "dest_key": "",
         "meta": {"fileId": "2"}, "upload_id": "", "status": "READY"},
    ]

    # Track verified files
    verified_files = []

    def test_verifier(expected_count):
        """Custom verifier that captures results"""
        while True:
            file = verify_queue.get()
            if file is DONE:
                break
            verified_files.append(file)

    # Workers
    s3_workers = ["S3-1"]
    api_workers = ["API-1"]

    # Create threads
    dispatcher_thread = threading.Thread(target=dispatcher, args=(test_files,))
    s3_threads = [threading.Thread(target=s3_uploader, args=(name,)) for name in s3_workers]
    api_threads = [threading.Thread(target=api_uploader, args=(name,)) for name in api_workers]
    verifier_thread = threading.Thread(target=test_verifier, args=(len(test_files),))

    # Start all
    for t in [dispatcher_thread] + s3_threads + api_threads + [verifier_thread]:
        t.start()

    # Shutdown sequence
    dispatcher_thread.join()
    for _ in s3_workers:
        s3_queue.put(DONE)

    for t in s3_threads:
        t.join()
    for _ in api_workers:
        api_queue.put(DONE)

    for t in api_threads:
        t.join()
    verify_queue.put(DONE)

    verifier_thread.join()

    # Assertions
    assert len(verified_files) == 2

    for file in verified_files:
        assert file["status"] == "API_UPLOADED"

        assert file["dest_key"] != ""  # S3 key was set
        assert __is_valid_s3_dest_key(file["dest_key"]), f"Invalid dest_key: {file['dest_key']}"

        assert file["upload_id"] != ""  # API returned UUID
        assert __is_valid_uuid(file["upload_id"]), f"Invalid upload_id: {file['upload_id']}"


def __is_valid_uuid(value) -> bool:
    """Check if value is a valid UUID (string or UUID object)"""
    try:
        if isinstance(value, UUID):
            return True
        UUID(str(value))
        return True
    except (ValueError, AttributeError):
        return False


def __is_valid_s3_dest_key(dest_key: str) -> bool:
    """
    Check if dest_key has format: YYYY/MM/DD/HH/MM/SS/uuid
    """
    if not isinstance(dest_key, str):
        return False

    parts = dest_key.split("/")
    if len(parts) != 7:
        return False

    # Validate timestamp
    timestamp_str = "/".join(parts[:6])
    try:
        datetime.strptime(timestamp_str, "%Y/%m/%d/%H/%M/%S")
    except ValueError:
        return False

    # Validate UUID
    return __is_valid_uuid(parts[-1])
