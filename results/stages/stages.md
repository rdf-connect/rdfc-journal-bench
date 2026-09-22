# Stage costs (anchor workload, size=0, reps=3, medians)

| case | n | wall ms | first out ms | lines | peak RSS MB | ms/record (first 10 %) | ms/record (last 10 %) |
|---|---:|---:|---:|---:|---:|---:|---:|
| rml-stream | 1 | 1025 | 1003 | 1 | 186 | – | – |
| rml-stream | 100 | 1413 | 1008 | 100 | 205 | 5.733 | 2.219 |
| rml-stream | 1000 | 2420 | 927 | 1000 | 303 | 3.406 | 0.892 |
| rml-stream | 10000 | 7049 | 1010 | 10000 | 629 | 1.615 | 0.355 |
| rml-batch | 1 | 1059 | 1040 | 1 | 188 | – | – |
| rml-batch | 100 | 1343 | 1308 | 1 | 176 | – | – |
| rml-batch | 1000 | 1804 | 1753 | 1 | 253 | – | – |
| rml-batch | 10000 | 3922 | 3914 | 1 | 688 | – | – |
| shacl-stream | 1 | 278 | 272 | 1 | 81 | – | – |
| shacl-stream | 100 | 382 | 279 | 90 | 93 | 1.946 | 0.641 |
| shacl-stream | 1000 | 904 | 279 | 952 | 166 | 0.918 | 0.488 |
| shacl-stream | 10000 | 4188 | 226 | 9507 | 262 | 0.583 | 0.399 |
| shacl-batch | 1 | 256 | 249 | 1 | 81 | – | – |
| shacl-batch | 100 | 366 | 354 | 90 | 117 | 0.013 | 0.007 |
| shacl-batch | 1000 | 836 | 810 | 952 | 230 | 0.009 | 0.005 |
| shacl-batch | 10000 | 4555 | 4429 | 9507 | 719 | 0.015 | 0.006 |
| pipe-stream | 1 | 1068 | 1055 | 1 | 273 | – | – |
| pipe-stream | 100 | 1570 | 1086 | 90 | 299 | 6.946 | 3.355 |
| pipe-stream | 1000 | 2755 | 1038 | 952 | 463 | 4.372 | 1.136 |
| pipe-stream | 10000 | 8208 | 1094 | 9507 | 892 | 1.719 | 0.462 |
| pipe-batch | 1 | 1125 | 1118 | 1 | 275 | – | – |
| pipe-batch | 100 | 1755 | 1738 | 1 | 327 | – | – |
| pipe-batch | 1000 | 2310 | 2287 | 1 | 499 | – | – |
| pipe-batch | 10000 | 8512 | 8466 | 1 | 1395 | – | – |

Derived from n=1 and n=10000. "cold / warm" is how many warm streamed records one extra process start costs.

| stage | cold ms (n=1) | warm ms/record | batch ms/record | cold / warm |
|---|---:|---:|---:|---:|
| rml | 1025 | 0.602 | 0.286 | 1702 |
| shacl | 278 | 0.391 | 0.430 | 711 |
| pipe | 1068 | 0.714 | 0.739 | 1496 |

## Streaming vs batch: same quads?

| n | stage | same | quads (stream) | quads (batch) |
|---:|---|---|---:|---:|
| 1 | rml | yes | 10 | 10 |
| 1 | shacl | yes | 10 | 10 |
| 1 | pipe | yes | 10 | 10 |
| 100 | rml | yes | 997 | 997 |
| 100 | shacl | yes | 900 | 900 |
| 100 | pipe | yes | 900 | 900 |
| 1000 | rml | yes | 9982 | 9982 |
| 1000 | shacl | yes | 9520 | 9520 |
| 1000 | pipe | yes | 9520 | 9520 |
| 10000 | rml | yes | 99833 | 99833 |
| 10000 | shacl | yes | 95070 | 95070 |
| 10000 | pipe | yes | 95070 | 95070 |
