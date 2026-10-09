"""ocidi2aidp: migrate OCI Data Integration workloads onto Oracle AI Data Platform.

Data flows and Integration / Data Loader tasks become PySpark notebooks,
pipelines and task schedules become AIDP jobs, and targets land as managed
Delta tables. A deterministic compiler does the work; what it cannot convert
safely becomes an LLM work order (see ``ocidi2aidp.fallback``), never a guess.
"""

__version__ = "0.1.0"
