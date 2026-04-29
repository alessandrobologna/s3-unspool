# Benchmark Results

This file contains generated benchmark output from `scripts/benchmark.py`.
For fixture generation, Lambda setup, and reproduction commands, see
[Benchmark Methodology](benchmark-methodology.md). The checked-in table below
is a post source-scheduler checkpoint from the 2026-04-28 rewrite branch. It is
kept as a comparison baseline until the next benchmark refresh.

<!-- s3-unspool-benchmark-results:start -->
## Automated Benchmark Results

Run id: `20260428T-source-scheduler-w5`
Function: `<lambda-function-name>`
Started: `2026-04-28T14:37:50.777642+00:00`
Elapsed: `32m 19s`

| Memory | Fixture | Scenario | Samples | Duration min | Duration median | Duration max | Max memory median | GET attempts median | Fetched blocks median | Block waits median | PUT failures median | PUT retries median | PUT throttles median | Errors |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MB | small | full | 5/5 | 5.78s | 5.93s | 6.28s | 55 MB | 3 | 1 | 6 | n/a | n/a | n/a | 0 |
| 256 MB | small | update with catalog | 5/5 | 1.12s | 1.17s | 1.44s | 54 MB | 8 | 6 | 6 | n/a | n/a | n/a | 0 |
| 256 MB | small | update with no catalog | 5/5 | 1.27s | 1.31s | 1.36s | 54 MB | 8 | 7 | 11 | n/a | n/a | n/a | 0 |
| 256 MB | medium | full | 5/5 | 65.67s | 66.57s | 66.90s | 197 MB | 32 | 30 | 6 | n/a | n/a | n/a | 0 |
| 256 MB | medium | update with catalog | 5/5 | 8.64s | 10.44s | 12.13s | 198 MB | 232 | 230 | 371 | n/a | n/a | n/a | 0 |
| 256 MB | medium | update with no catalog | 5/5 | 21.40s | 21.68s | 22.22s | 202 MB | 261 | 260 | 387 | n/a | n/a | n/a | 0 |
| 256 MB | large | full | 5/5 | 391.99s | 400.28s | 417.07s | 219 MB | 183 | 179 | 6 | n/a | n/a | n/a | 0 |
| 256 MB | large | update with catalog | 5/5 | 47.78s | 58.16s | 72.63s | 217 MB | 1,246 | 1,242 | 2,019 | n/a | n/a | n/a | 0 |
| 256 MB | large | update with no catalog | 5/5 | 124.67s | 131.34s | 140.16s | 228 MB | 1,424 | 1,421 | 2,265 | n/a | n/a | n/a | 0 |
| 1024 MB | small | full | 5/5 | 2.72s | 2.89s | 2.98s | 55 MB | 3 | 1 | 11 | n/a | n/a | n/a | 0 |
| 1024 MB | small | update with catalog | 5/5 | 0.86s | 0.97s | 1.04s | 57 MB | 8 | 6 | 11 | n/a | n/a | n/a | 0 |
| 1024 MB | small | update with no catalog | 5/5 | 0.57s | 0.70s | 1.07s | 56 MB | 8 | 7 | 22 | n/a | n/a | n/a | 0 |
| 1024 MB | medium | full | 5/5 | 34.02s | 34.32s | 34.77s | 344 MB | 32 | 30 | 11 | n/a | n/a | n/a | 0 |
| 1024 MB | medium | update with catalog | 5/5 | 3.35s | 4.57s | 5.54s | 344 MB | 232 | 230 | 206 | n/a | n/a | n/a | 0 |
| 1024 MB | medium | update with no catalog | 5/5 | 6.31s | 6.45s | 6.68s | 351 MB | 261 | 260 | 25 | n/a | n/a | n/a | 0 |
| 1024 MB | large | full | 5/5 | 200.57s | 200.69s | 202.23s | 906 MB | 183 | 179 | 11 | n/a | n/a | n/a | 0 |
| 1024 MB | large | update with catalog | 5/5 | 18.01s | 23.41s | 27.50s | 917 MB | 1,246 | 1,242 | 1,029 | n/a | n/a | n/a | 0 |
| 1024 MB | large | update with no catalog | 5/5 | 35.91s | 39.56s | 40.39s | 902 MB | 1,424 | 1,421 | 24 | n/a | n/a | n/a | 0 |
| 2048 MB | small | full | 5/5 | 2.11s | 2.19s | 2.29s | 53 MB | 3 | 1 | 16 | n/a | n/a | n/a | 0 |
| 2048 MB | small | update with catalog | 5/5 | 0.53s | 0.62s | 0.67s | 56 MB | 8 | 6 | 16 | n/a | n/a | n/a | 0 |
| 2048 MB | small | update with no catalog | 5/5 | 0.53s | 0.67s | 0.80s | 55 MB | 8 | 7 | 32 | n/a | n/a | n/a | 0 |
| 2048 MB | medium | full | 5/5 | 24.85s | 25.60s | 27.45s | 352 MB | 75 | 73 | 59 | n/a | n/a | n/a | 0 |
| 2048 MB | medium | update with catalog | 5/5 | 4.13s | 4.48s | 6.63s | 378 MB | 256 | 254 | 200 | n/a | n/a | n/a | 0 |
| 2048 MB | medium | update with no catalog | 5/5 | 6.40s | 6.63s | 9.51s | 393 MB | 291 | 289 | 85 | n/a | n/a | n/a | 0 |
| 2048 MB | large | full | 5/5 | 201.34s | 202.63s | 204.05s | 1623 MB | 2,320 | 2,316 | 2,153 | n/a | n/a | n/a | 0 |
| 2048 MB | large | update with catalog | 5/5 | 14.59s | 30.27s | 31.73s | 1629 MB | 1,333 | 1,310 | 756 | n/a | n/a | n/a | 0 |
| 2048 MB | large | update with no catalog | 5/5 | 29.66s | 31.42s | 32.68s | 1696 MB | 1,424 | 1,421 | 208 | n/a | n/a | n/a | 0 |
<!-- s3-unspool-benchmark-results:end -->
