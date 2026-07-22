"""
Shared scorers for Braintrust skill evaluations.

This module provides reusable scoring functions that use Claude
to evaluate whether outputs meet specified criteria.
"""

import anthropic
from autoevals import Score
from braintrust import wrap_anthropic

# Use Anthropic client wrapped with Braintrust for tracing
_client = wrap_anthropic(anthropic.Anthropic())


def check_criterion(question: str, answer: str, criterion: str) -> bool:
    """Check if a single criterion is met by the answer using structured output."""
    response = _client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1024,
        tools=[
            {
                "name": "submit_judgment",
                "description": "Submit your judgment on whether the criterion is satisfied",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "satisfied": {
                            "type": "boolean",
                            "description": "True if the answer satisfies the criterion, False otherwise",
                        },
                    },
                    "required": ["satisfied"],
                },
            }
        ],
        tool_choice={"type": "tool", "name": "submit_judgment"},
        messages=[
            {
                "role": "user",
                "content": f"""Evaluate whether this answer satisfies the criterion. Use the submit_judgment tool with your decision.

Question: {question}

Answer: {answer}

Criterion: {criterion}""",
            }
        ],
    )
    # Get the tool use block
    for block in response.content:
        if block.type == "tool_use" and block.name == "submit_judgment":
            return block.input.get("satisfied", False)
    return False


def criteria_scorer(output, expected, input, **kwargs):
    """
    Scorer that checks if the output meets each criterion in the expected list.
    Runs a separate LLM call for each criterion and aggregates results.

    Args:
        output: The model output to evaluate
        expected: List of natural language criteria the output should satisfy
        input: The original question/prompt
        **kwargs: Additional arguments (metadata, etc.)

    Returns:
        Score with name "Criteria" and metadata containing pass/fail for each criterion
    """
    if not output:
        return Score(name="Criteria", score=0, metadata={"reason": "empty output"})

    criteria = expected if isinstance(expected, list) else []
    if not criteria:
        return Score(name="Criteria", score=1, metadata={"reason": "no criteria"})

    # Check each criterion
    results = {}
    for criterion in criteria:
        results[criterion] = check_criterion(input, output, criterion)

    passed = sum(1 for v in results.values() if v)
    score = passed / len(criteria)

    return Score(
        name="Criteria",
        score=score,
        metadata={
            "passed": passed,
            "total": len(criteria),
            "results": results,
        },
    )


def get_anthropic_client():
    """Get the shared Anthropic client for use in task functions."""
    return _client
