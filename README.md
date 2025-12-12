# Python Concurrency: Multi-Stage Pipeline with Producer-Consumer Pattern - incl. "Poison Pill" Pattern 💀

A hands-on tutorial code demonstrating Python threading, queues, and the Producer-Consumer pattern through a realistic file processing pipeline.

---

## What is Poison Pill Pattern ?

The Poison Pill pattern is a concurrency and messaging design pattern used to gracefully stop consumers 
(threads, workers, or processes) that are waiting for work from a queue or stream. A special sentinel 
value - the “poison pill” - is inserted into the queue to signal that no more work will arrive.


When a consumer receives the poison pill, it recognizes it as a shutdown signal, finishes any necessary
cleanup, and exits normally. This approach avoids forcefully interrupting threads, 
works well with blocking queues, and allows clean, predictable shutdown of producer–consumer pipelines.

---

## 🎯 What You'll Learn ... or basically what I am doing here

- **Producer-Consumer Pattern** — the foundational concurrency pattern used in most distributed systems
- **Thread-Safe Queues** — how `queue.Queue` enables safe communication between threads
- **Poison Pill Pattern** — clean shutdown signaling for worker threads
- **Multi-Stage Pipelines** — chaining multiple processing stages with queues
- **Coordinator Pattern** — orchestrating shutdown across pipeline stages

---

## 🏗️ Architecture Overview

This project simulates a file migration pipeline with 4 stages:

```
┌────────────┐     ┌─────────┐     ┌─────────────┐     ┌─────────┐     ┌─────────────┐     ┌──────────┐     ┌──────────┐
│ Dispatcher │────▶│s3_queue │────▶│ S3 Uploaders│────▶│api_queue│────▶│API Uploaders│────▶│verify_que│────▶│ Verifier │
│    (1)     │     │         │     │     (3)     │     │         │     │     (2)     │     │          │     │   (1)    │
└────────────┘     └─────────┘     └─────────────┘     └─────────┘     └─────────────┘     └──────────┘     └──────────┘
   Producer                         Consumer/Producer                   Consumer/Producer                     Consumer
                                        FAST                                SLOW                              AGGREGATOR
                                      (0.2-0.5s)                          (1-2s)
```

### Stage Descriptions

| Stage | Workers | Role | Speed | Input | Output |
|-------|---------|------|-------|-------|--------|
| **Dispatcher** | 1 | Reads files from "database", dispatches to pipeline | Fast | FILES list | s3_queue |
| **S3 Uploader** | 3 | Copies files between S3 buckets | Fast (0.2-0.5s) | s3_queue | api_queue |
| **API Uploader** | 2 | Uploads metadata to REST API | Slow (1-2s) | api_queue | verify_queue |
| **Verifier** | 1 | Aggregates results, produces final report | N/A | verify_queue | Report |

---

## 🔑 Key Concepts

### 1. Producer-Consumer Pattern

The fundamental pattern where:
- **Producers** create work items and put them in a queue
- **Consumers** take work items from the queue and process them
- **Queue** decouples producers from consumers — they work at their own pace

```python
# Producer
def dispatcher(files):
    for file in files:
        s3_queue.put(file)  # produce work

# Consumer
def s3_uploader(name):
    while True:
        file = s3_queue.get()  # consume work
        # ... process file
```

### 2. Thread-Safe Queue (`queue.Queue`)

Python's `queue.Queue` handles all synchronization automatically:

| Method | Behavior |
|--------|----------|
| `q.put(item)` | Add item. Blocks if queue is full (when maxsize set) |
| `q.get()` | Remove and return item. Blocks if queue is empty |

No locks needed — the queue handles thread safety internally.

### 3. Poison Pill Pattern

A special sentinel value signals workers to shut down:

```python
DONE = None  # The "poison pill"

def worker():
    while True:
        item = queue.get()
        if item is DONE:      # Check for poison pill
            break             # Exit cleanly
        process(item)
```

**Critical Rule:** One poison pill per consumer. Each `.get()` removes the item — other workers won't see it.

```
3 workers need 3 poison pills:

Queue: [DONE] [DONE] [DONE]
          │      │      │
          ▼      ▼      ▼
      Worker1 Worker2 Worker3
       exits   exits   exits
```

### 4. Coordinator Pattern

The main thread orchestrates shutdown by:
1. Waiting for each stage to complete (`.join()`)
2. Sending poison pills to the next stage
3. Repeating until pipeline is fully drained

```python
# Wait for dispatcher
dispatcher_thread.join()

# Signal S3 workers (dispatcher done, no more files coming)
for _ in range(num_s3_workers):
    s3_queue.put(DONE)

# Wait for S3 workers  
for t in s3_threads:
    t.join()

# Signal API workers (S3 workers done)
for _ in range(num_api_workers):
    api_queue.put(DONE)

# ... and so on
```

---

## 📊 Data Flow

Each file travels through the pipeline, getting enriched at each stage:

```
Stage 1 - Dispatcher:
  file = {"id": 1, "name": "file_100.pdf", "status": "READY", ...}

Stage 2 - S3 Uploader:
  file["dest_key"] = "2025/01/15/14/30/45/uuid-here"
  file["status"] = "S3_COPIED"

Stage 3 - API Uploader:
  file["status"] = "API_UPLOADED"

Stage 4 - Verifier:
  Collects all files, produces summary report
```

---

## 🚀 Running the Code

### Prerequisites

- Python 3.7+
- No external dependencies (uses only standard library)

### Execute

```bash
python pipeline.py
```

### Expected Output

```
🔖 Dispatcher: dispatching file file_100.pdf
🔖 Dispatcher: dispatching file file_101.pdf
    Uploader S3-Worker-1 copy file file_100.pdf S3 -> S3
💾 Uploader S3-Worker-1 stored file [file_100.pdf] in S3 bucket=[dest-bucket-proj] and key=[2025/01/15/...]
🔖 Dispatcher: dispatching file file_102.pdf
    Uploader S3-Worker-2 copy file file_101.pdf S3 -> S3
...
🔐 API Uploader: file [file_100.pdf] uploaded ! API_UPLOADED
✓ Verified: file_100.pdf - API_UPLOADED
...
☑️ Uploader S3-Worker-1 is DONE. Finishing work ... Finished
☑️ Uploader S3-Worker-2 is DONE. Finishing work ... Finished
☑️ Uploader S3-Worker-3 is DONE. Finishing work ... Finished
☑️  - 🔐 API Uploader API-Worker-1 got DONE signal. Finishing ... Finished
☑️  - 🔐 API Uploader API-Worker-2 got DONE signal. Finishing ... Finished

=== VERIFICATION REPORT ===
Expected: 8
Processed: 8
Success: 8
Failed: 0

=== Pipeline complete ===
```

---

## 🧠 Why This Pattern Matters

### Real-World Applications

This exact pattern is used in:

| Application | Dispatcher | Workers | Aggregator |
|-------------|-----------|---------|------------|
| **Web Scraper** | URL list | Page fetchers | Data store |
| **Image Processor** | File list | Resize/compress workers | Output folder |
| **Log Aggregator** | Log sources | Parsers | Analytics DB |
| **ETL Pipeline** | Data source | Transformers | Data warehouse |
| **AWS Lambda** | SQS queue | Lambda instances | Results DB |

### Scaling Up

The same pattern scales from threads to distributed systems:

| Scope | Queue Technology |
|-------|------------------|
| Threads in one process | `queue.Queue` |
| Processes on one machine | `multiprocessing.Queue` |
| Services across network | Kafka, SQS, RabbitMQ |

---

## ⚠️ Common Mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Wrong poison pill count | Workers hang forever | One DONE per consumer |
| Send DONE before `.join()` | Files not processed | Always join, then send DONE |
| Forget to start threads | Nothing happens | Check all threads started |
| Multiple aggregators | Incomplete reports | Use single aggregator |

---

## 🔄 Shutdown Sequence Diagram

```
Time
  │
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ dispatcher_thread.join()                                    │
  │   │ (wait for all files to be dispatched)                       │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ s3_queue.put(DONE) × 3                                      │
  │   │ (one per S3 worker)                                         │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ s3_uploader_threads.join()                                  │
  │   │ (wait for all S3 copies to complete)                        │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ api_queue.put(DONE) × 2                                     │
  │   │ (one per API worker)                                        │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ api_uploader_threads.join()                                 │
  │   │ (wait for all API uploads to complete)                      │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ verify_queue.put(DONE) × 1                                  │
  │   │ (only one verifier)                                         │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  │   ┌─────────────────────────────────────────────────────────────┐
  │   │ verifier_thread.join()                                      │
  │   │ (wait for final report)                                     │
  │   └─────────────────────────────────────────────────────────────┘
  │                              │
  │                              ▼
  ▼                        Pipeline Complete
```

---

## Testing code

Install dependencies: `pip install pytest`


Run `pytest test_pipeline.py -v`


---

## 📄 License

MIT License - feel free to use this code for learning and teaching.

---

## 🙏 Acknowledgments

This tutorial was developed as a hands-on learning exercise for Python concurrency patterns.

---

**Happy Threading! 🧵**
