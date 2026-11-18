# Design and Implementation of a Persistent Dynamic Record Storage Engine Using Extendible Hashing

## Abstract
This project implements a generic key-value record storage engine using extendible hashing. A directory of bucket pointers is indexed by the leading bits of a SHA-256 key hash. Buckets split locally when full; the directory doubles only when a split reaches the current global depth. Records and structural metadata are stored in custom JSON files, with an append-only write-ahead operation log supporting recovery after restart. A responsive browser interface exposes record operations, structure inspection, statistics, integrity validation, backup and restore.

## Features
- Manual extendible hashing: global depth, local depth, directory pointers, buckets, splits, doubling and redistribution.
- Generic records with key, name, category, description, value, created date and updated date.
- Exact-key search, duplicate-key rejection, insert, update and delete.
- Buddy-bucket merging and directory shrinking when safe.
- SHA-256 key hashing and bounded-depth overflow handling for pathological hash prefixes.
- Persistent JSON snapshot, append-only operation log, startup recovery and recovery endpoint.
- ZIP backups, restore, JSON snapshot export and structural integrity checks.
- Dashboard, directory and bucket visualizations, operation counters and fictional student demo dataset.
- Automated unit tests for core operations, structural growth, persistence, backup, integrity and log recovery.

## Technology Stack
- Backend: Python 3.10+, FastAPI, Uvicorn, Pydantic.
- Frontend: HTML5, CSS3, vanilla JavaScript.
- Storage: custom JSON snapshot (`data/storage.json`) and JSON Lines write-ahead log (`data/operations.jsonl`).
- Tests: Python `unittest`.

## Installation
```bash
cd extendible-record-engine
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## Running
```bash
uvicorn main:app --host 127.0.0.1 --port 8000
```
Open `http://127.0.0.1:8000`. The `data/` directory is created automatically. Do not delete it if records must persist.

## System Architecture
1. Browser UI calls JSON endpoints under `/api`.
2. FastAPI validates request payloads and delegates operations to `ExtendibleHashStore`.
3. The storage engine hashes a key, selects a directory entry, and scans the pointed bucket.
4. A committed mutation writes a complete state image to the operation log and atomically replaces the JSON snapshot.
5. On startup, the latest snapshot is loaded and any newer committed log state is replayed.

## Extendible Hashing Explanation
Let `d` be the global depth. The directory contains `2^d` pointers. Each bucket stores a local depth `d'`, a prefix, and up to the configured primary bucket capacity. The first `d` bits of the SHA-256 hash select the directory entry. Multiple entries may point to the same bucket; a bucket of local depth `d'` is referenced by `2^(d-d')` entries.

When a bucket overflows, it is split by increasing its local depth and distributing its records using the next hash bit. If its local depth equals global depth, the directory is doubled first. The split is local: unrelated buckets and records are not rehashed. On deletion, buddy buckets are merged when they have matching local depth and their combined records fit; the directory is halved when all local depths permit it.

## Persistence Explanation
`data/storage.json` stores global depth, directory entries, bucket IDs, local depths, bucket prefixes, records, overflow records, counters, sequence number and metadata. `data/operations.jsonl` stores committed operation details and a full recoverable state image for each sequence. Each commit appends and flushes the log entry before atomically replacing the snapshot. This ordering means a crash after the log append but before the snapshot replacement can be recovered from the newer log state.

## Recovery Explanation
At startup, the engine loads the snapshot if valid, scans the operation log, and applies the newest valid state with a sequence greater than the snapshot sequence. The recovery section of the interface can trigger the same recovery procedure manually. A corrupt or missing snapshot can be reconstructed from a valid committed log entry. This is a single-process demonstration design, not a multi-writer transactional database.

## API Documentation
| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Health and storage summary |
| GET | `/api/dashboard` | Dashboard statistics and integrity summary |
| POST | `/api/records` | Insert a record |
| GET | `/api/records/{key}` | Search for an exact key |
| GET | `/api/records` | List primary bucket records |
| PUT | `/api/records/{key}` | Update record fields |
| DELETE | `/api/records/{key}` | Delete a record |
| GET | `/api/directory` | Directory pointers and local depths |
| GET | `/api/buckets` | Bucket contents and overflow records |
| GET | `/api/statistics` | Structural and operation statistics |
| GET | `/api/integrity` | Validate pointers, depths, placement and counts |
| POST | `/api/demo` | Insert demo records and run sample operations |
| POST | `/api/backup` | Create a ZIP backup |
| GET | `/api/backups` | List backups |
| POST | `/api/restore` | Restore `{ "filename": "backup-name.zip" }` |
| GET | `/api/logs` | Read recent operation log entries |
| POST | `/api/recover` | Replay newer committed log state |
| GET | `/api/export` | Download current JSON snapshot |

Insert JSON example:
```json
{
  "key": "STU-101",
  "name": "Aarav Sharma",
  "category": "Computer Science",
  "description": "Fictional student record",
  "value": "CGPA: 8.4"
}
```

## Testing Instructions
```bash
python -m unittest discover -s tests -v
```
The suite covers insertion, search, missing search, duplicate rejection, update, delete, bucket splitting, directory doubling, redistribution, restart persistence, backup/restore, integrity validation and recovery from the operation log.

## Known Limitations
- The application is designed for a single local process; concurrent multi-worker writes are not coordinated.
- Each WAL entry contains a full state image, which is intentionally simple for recovery demonstration but causes log growth. Periodic log compaction is not implemented.
- Directory storage is an in-memory Python list and can become large at high global depths.
- SHA-256 makes accidental hash-prefix collisions extremely unlikely; overflow handling exists as a bounded-depth safeguard, but collision-heavy workloads are not benchmarked.
- This is an academic storage engine, not a production database: it has no authentication, authorization, encryption-at-rest, replication or crash-consistency testing across all filesystems.
- Cloud persistence depends on the hosting provider's attached persistent disk. Ephemeral container filesystems must not be treated as durable storage.

## Future Enhancements
- Page-oriented binary bucket files and an on-disk directory.
- WAL checkpointing, compaction and checksums.
- Concurrent access control and transactional multi-record operations.
- Configurable bucket capacity and alternate hash functions.
- Performance benchmarking against linear hashing and B+-trees.
- Cloud deployment with a managed persistent volume and automated external backups.
