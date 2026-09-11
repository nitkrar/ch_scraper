# Scripts

## Requirements

The Python packages these scripts need (`openpyxl`, `pdf2image`, `pytesseract`)
are part of the base install, so `./setup.sh` covers them.

`ocr_staged_pdfs.py` additionally needs two **system** binaries that pip cannot
install. Without them it fails at runtime, not at import:

```bash
brew install tesseract poppler    # provides `tesseract` and `pdftoppm`
```

Verify with `which tesseract pdftoppm`.

## Maintained helpers

- `annotate_pl_exemption.py` — annotate extraction JSONL rows with a `profit_loss_exempt` flag.
- `build_opus_batch.py` — build an Opus extraction batch JSON file.
- `classify_writer.py` — helper for inline classifier sessions.
- `fetch_for_classify.py` — fetch-only helper for ad hoc classifier sessions.
- `financial_page_filter.py` — score and filter financial filing pages before extraction.
- `ocr_staged_pdfs.py` — produce OCR + filtering sidecar data for staged Companies House PDFs.
- `qwen_ocr_extract.py` — run the Qwen-based OCR extraction worker.
- `reparse_saved_financials.py` — re-parse saved filing artifacts into a fresh staging batch.
- `reparse_staged_financials.py` — rebuild a financials staging JSONL from saved raw filings.
- `validate_extractions.py` — sense-check extraction JSONLs with outlier detection.

## Frozen one-shots

- `adhoc/batch_commit.py` — commit verdict JSON output back into the classifications JSONL.
- `adhoc/batch_prep.py` — dump the next unclassified rows for inline classification work.
- `adhoc/classify_shard.py` — fetch-only helper for ad hoc shard classification.
- `adhoc/fetch_batch.py` — batch page fetcher for ad hoc classification.
- `adhoc/fetch_shard_0.py` — fetch-only script for shard 0 without LLM calls.
- `adhoc/salvage_classification_parse_errors.py` — recover classifier `parse_error` rows from staged raw responses.
