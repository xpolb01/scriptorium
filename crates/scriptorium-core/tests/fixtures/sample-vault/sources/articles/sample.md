# Sample source article

This is a source document the vault owner might drop into `sources/`. The
ingest pipeline reads it, prompts the LLM, and writes summary pages into
`wiki/`. Tests never actually call a real LLM; the mock provider returns a
pre-baked ingest plan.

Key claim: attention mechanisms allow transformers to process sequences in
parallel, in contrast to RNNs which process tokens sequentially.
