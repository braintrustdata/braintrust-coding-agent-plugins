# Braintrust Skill Evaluation Results

This document summarizes the impact of the `using-braintrust` skill on Claude's ability to use the Braintrust platform.

## Overview

We created two end-to-end evaluations that test Claude's ability to:
1. **Log Fetch**: Log data to Braintrust and verify it exists via BTQL queries
2. **Experiment**: Create and run evaluations with scorers

Both evals use the Claude Agent SDK to have Claude actually execute Python code, then verify the results by querying Braintrust directly (not trusting Claude's text output).

## Results summary

| Eval | Metric | Before Skill | After Skill | Improvement |
|------|--------|--------------|-------------|-------------|
| Log Fetch | Logs Created | 67% | 100% | +33% |
| Log Fetch | Correct Count | 67% | 100% | +33% |
| Log Fetch | Task Completed | 67% | 100% | +33% |
| Experiment | Experiments Created | 100% | 100% | — |
| Experiment | Eval Ran | 100% | 100% | — |
| Experiment | Task Completed | 0% | 100% | **+100%** |

## Key error fixed by the skill

Without the skill, Claude consistently made this error:

```python
# What Claude tried (WRONG)
braintrust.Eval(project_name="My Project", data=..., task=..., scores=...)

# Error: TypeError: Eval() got an unexpected keyword argument 'project_name'
```

The skill documentation explicitly shows the correct usage:

```python
# Correct usage from SKILL.md
braintrust.Eval(
    "My Project",  # Project name is FIRST POSITIONAL argument
    data=lambda: [...],
    task=lambda input: ...,
    scores=[my_scorer],
)
```

## Detailed results

### Log Fetch Eval (`eval_e2e_log_fetch.py`)

Tests Claude's ability to:
- Initialize a Braintrust logger
- Log entries with metadata
- Call `flush()` to ensure data is sent

**Baseline (without skill):**
- Logs Created: 67% — Claude sometimes forgot `flush()` or used wrong API
- Correct Count: 67% — Not always logging the expected number of entries
- Task Completed: 67% — Some executions had errors

**With skill:**
- Logs Created: 100% ✅
- Correct Count: 100% ✅
- Task Completed: 100% ✅

### Experiment Eval (`eval_e2e_eval_improve.py`)

Tests Claude's ability to:
- Create test data
- Write a task function
- Use autoevals scorers
- Run `braintrust.Eval()`

**Baseline (without skill):**
- Experiments Created: 100% — Claude did create experiments
- Eval Ran: 100% — The evals did execute
- Task Completed: 0% — But every execution had the `project_name` TypeError

**With skill:**
- Experiments Created: 100% ✅
- Eval Ran: 100% ✅
- Task Completed: 100% ✅

## What the skill provides

The `SKILL.md` file includes:

1. **Correct API signatures** — Shows that `Eval()` takes a positional project name
2. **Working examples** — Copy-paste code that actually works
3. **Common pitfalls** — Explicitly warns about the `project_name` mistake
4. **API reference** — Documents `Eval()`, `init_logger()`, `Score`, etc.

## Verification method

Both evals verify results by querying Braintrust directly:

- **Log Fetch**: Uses BTQL to count logs with a specific `test_id` in metadata
- **Experiment**: Uses the API to count experiments created in the test project

This ensures we're measuring actual success, not just Claude claiming success.

## Experiment links

- Log Fetch (with skill): https://www.braintrust.dev/app/braintrustdata.com/p/Braintrust%20Skill%20-%20E2E%20Log%20Fetch/experiments/add-evals-1766272101
- Experiment (with skill): https://www.braintrust.dev/app/braintrustdata.com/p/Braintrust%20Skill%20-%20E2E%20Experiment/experiments/add-evals-1766271997

## Conclusion

The skill improved Claude's success rate from **67-0%** to **100%** on critical Braintrust operations. The key insight is that LLMs need explicit documentation of API patterns, especially when:

1. The API uses positional arguments in unexpected ways
2. There are required cleanup steps (like `flush()`)
3. The error messages don't clearly indicate the fix

This validates the evaluation-driven development approach: we identified real gaps by running evals first, then created targeted documentation to address those specific failures.
