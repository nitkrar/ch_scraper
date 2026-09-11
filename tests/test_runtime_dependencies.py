"""Guards against under-declared runtime dependencies.

Some dependencies are never imported by our source, so an import scan of the
codebase cannot find them. They are pulled in by a library only when a specific
feature flag is used at runtime:

  * httpx needs 'h2' when a client is built with http2=True
  * pandas needs 'odfpy' when read_excel is called with engine="odf"

A missing one of these installs cleanly and then raises on first use, deep in a
long-running enrichment job. These tests exercise the same feature flags the
production code uses, so the failure surfaces here instead.
"""

import unittest


class RuntimeDependencyTests(unittest.TestCase):
    def test_httpx_client_supports_http2(self):
        """classifier_pipeline builds httpx.Client(http2=True); h2 must be installed."""
        import httpx

        # Mirrors ch_bulk/web/classifier_pipeline.py. Raises ImportError
        # ("Using http2=True, but the 'h2' package is not installed") when the
        # httpx[http2] extra is missing.
        client = httpx.Client(base_url="http://localhost:9741/v1/", http2=True)
        client.close()

    def test_pandas_can_read_ods(self):
        """cqc/processor.py calls pd.read_excel(engine="odf"); odfpy must be installed."""
        import io

        import pandas as pd

        # Constructing the reader is what pulls in odfpy; an empty buffer fails
        # on parsing rather than on the import, so only assert we got past the
        # dependency check.
        with self.assertRaises(Exception) as ctx:
            pd.read_excel(io.BytesIO(b""), engine="odf")
        self.assertNotIsInstance(ctx.exception, ImportError)


if __name__ == "__main__":
    unittest.main()
