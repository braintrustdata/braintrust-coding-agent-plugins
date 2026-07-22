# Braintrust Skill Evals

Evaluation suite for Braintrust skills.

## Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
cd evals
uv sync
```

## Running Evals

```bash
cd evals

# Run a specific eval
uv run python eval_docs_search.py

# Run all evals
uv run python eval_datasets.py
uv run python eval_docs_search.py
uv run python eval_experiments.py
uv run python eval_log_querying.py
```
