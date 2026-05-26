"""ch_bulk — Download and query UK Companies House bulk data by SIC code."""

from ch_bulk.bootstrap import ensure_pipeline_schema

__all__ = [
    "ChBulk",
    "SanityCheckError",
    "SanityCheckResult",
    "ensure_pipeline_schema",
]


def __getattr__(name: str):
    if name == "ChBulk":
        from ch_bulk.api import ChBulk

        return ChBulk
    if name in {"SanityCheckError", "SanityCheckResult"}:
        from ch_bulk.processor import SanityCheckError, SanityCheckResult

        return {
            "SanityCheckError": SanityCheckError,
            "SanityCheckResult": SanityCheckResult,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
