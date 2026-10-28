# Latency and completeness observations

Correctness takes priority over speculative connection tuning. No connection spamming, TCP source-port experiments, rate-limit probing or trading was performed.

The live coordinator records the maximum observed `local_receive_ms - exchange_timestamp_ms` across accepted book events. Local receipt is captured in each socket task before channel enqueue/JSON normalization. This is a data-age proxy that includes cached snapshot age, feed batching and clock skew; it does not isolate transport latency; the exchange timestamp has only millisecond precision while the supplied SBE schema stores microseconds. Added zero microseconds do not create additional timestamp precision. Queue depth and ready/discovered coverage are reported alongside the delay.

Redundant feeds are independent replicas, not a per-message fastest-packet merger. Such merging would require a reliable sequence/deduplication identity that the documented raw Polymarket feed does not supply. Choosing the fastest-looking delta independently can corrupt a book.

The 2,000-symbol stress test verifies final state after 40,000 replica updates and 2,000 failovers. Its debug-mode wall time is functional test evidence, not a network or production throughput benchmark. The submission does not claim a measured HFT latency improvement.

Live probes and their actual scope/results are recorded in `evidence/VERIFICATION.md`. Warm promotions have been observed in real public runs; sustained all-market coverage remains unproven. No controlled exchange failover or one-way network-latency benchmark is claimed.
