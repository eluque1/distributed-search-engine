# Distributed Search Engine

A distributed web search engine built in Java by a four-person team, with a custom key-value store and distributed processing framework. The system crawls web pages, builds an inverted index, computes PageRank, and serves ranked results through a web search interface. Deployed across AWS EC2 instances.

## Architecture

- **Key-Value Store:** Distributed storage for crawled pages, indexes, and ranking data. Buffered batch writes reduce HTTP request overhead, while an in-memory cache reduces repeated disk reads.
- **Flame:** An RDD-based distributed processing framework used to run crawling, indexing, and ranking jobs across workers.
- **Web Crawler:** Collects web content and extracts links, with URL normalization and workload distribution across workers.
- **Indexer:** Cleans HTML, tokenizes and stems text, and computes term frequencies. Processes documents in batches of 2,000 to limit memory usage.
- **Index Merge:** Combines partial indexes through a custom distributed join operation and supports incorporating newly crawled pages.
- **GraphBuilder and PageRank:** Extracts a compact link graph from crawled pages and iteratively computes PageRank until convergence. Uses batched writes and disk-backed intermediate tables to manage memory.
- **Search Server:** Combines TF-IDF and PageRank with boosts for exact phrases and title/URL matches. Includes spelling suggestions, query-relevant snippets, and pagination.

## Design Decisions

- Reduced storage request overhead by buffering writes and sending them in batches.
- Restructured indexing and PageRank to process datasets that exceeded available JVM memory.
- Improved crawler workload distribution by revising URL hashing strategies.
- Separated graph construction from PageRank to avoid repeatedly processing full page content.
- Implemented parallel index lookups and merging operations.

## Technologies

Java, HTTP, AWS EC2, custom distributed key-value storage, RDD-based processing, TF-IDF, and PageRank.

## Contributors

Anushka Sheoran, Anjana Begur, Cameron Bosio-Kim, and Emma Luque.

## Local Setup (Windows, PowerShell)

### 1) Clone
```powershell
git clone https://github.com/USERNAME/distributed-search-engine.git
cd distributed-search-engine
```

### 2) Build
```powershell
./run-job.ps1 -buildOnly
```
(compiles Java into `bin` and builds jars)

### 3) Ports/Processes (defaults in `run-job.ps1`)
- KVS coordinator: `8000`
- KVS workers: `8001`, `8002` (dirs `worker1/`, `worker2/`)
- Flame coordinator: `9000`
- Flame workers: `9001`, `9002`
- JVM heap: `-Xmx2g` on all processes

### 4) Run jobs
- Crawler:
  ```powershell
  ./run-job.ps1 -job Crawler -jobArgs "<seed-url>"
  ```
- Indexer:
  ```powershell
  ./run-job.ps1 -job Indexer
  ```
- IndexerMerge:
  ```powershell
  ./run-job.ps1 -job IndexerMerge
  ```
- PageRank (example):
  ```powershell
  ./run-job.ps1 -job PageRank -jobArgs "0.001 100 pt-graph 2000"
  ```

Flags:
- `-keep` (if present) leaves processes running for log inspection.
- `-buildOnly` just compiles.

### 5) Logs & data
- Job output: `jobs.log` (repo root)
- Worker data: `worker1/`, `worker2/` (gitignored)
- Add per-process redirection in `run-job.ps1` if needed.

### 6) Frontend (search server)
1) Start KVS: run the KVS coordinator and workers (kvs).
2) Start the search server (Java) pointing at that KVS.
   - Example:
     ```powershell
     java -cp bin cis5550.frontend.SearchServer <kvsHost:port> <frontendPort>
     ```
3) Build/compile the frontend (if applicable) and access it via the search server port.

### 7) Stop locally
```powershell
Get-Process java | Stop-Process
```
(or use `./stop-local.ps1` if available)

### 8) AWS (optional)
Use `run-job-aws.ps1` (background) or after setting `$JobType` and host/IP vars inside the script.
