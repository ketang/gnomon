# Opportunity Stability Policy

## Purpose

Opportunity annotations should stay intelligible across model, pricing, and
harness changes. This policy separates the stable taxonomy from the advice layer
that may need to evolve over time.

## Stable Taxonomy

The following opportunity categories are stable mechanism labels for `v1`:

- `session setup`
- `task setup`
- `history drag`
- `delegation`
- `model mismatch`
- `prompt yield`
- `search churn`
- `tool-result bloat`

These names describe the kind of overhead that was observed. They are not tied
to one specific recommendation, one model family, or one moment in the product.

## Time-Sensitive Recommendations

Recommendations derived from a category are expected to change over time.
Examples:

- `model mismatch` advice may change as model pricing or relative capability changes
- `delegation` advice may change as harness workflows change
- `tool-result bloat` advice may change as tool limits or rendering behavior change

Recommendations should therefore be treated as versioned guidance attached to a
stable category, not as part of the taxonomy itself.

## Revision Rules

When advice changes:

- keep the category label stable unless the underlying mechanism has changed
- update recommendation text, thresholds, and examples without renaming the category
- document materially changed advice in release notes or adjacent docs when user expectations may shift
- only create a new category when the observed overhead mechanism is genuinely different, not because guidance changed

## Confidence Expectations

Confidence applies to the observed mechanism first and to the recommendation
second.

- Stable mechanism categories should only be shown when the observed evidence is
  at least `medium` confidence.
- Time-sensitive recommendations may be more conservative than the category that
  triggered them.
- If the mechanism is credible but the recommendation is not, keep the category
  and downgrade or omit the recommendation text.
- If both mechanism and recommendation are weak, suppress the annotation.

## Reviewer Guidance

Review changes to opportunity heuristics with two questions:

1. Is the taxonomy label still naming the same underlying mechanism?
2. Is the recommendation the part that changed, rather than the mechanism?

If the answer to the first question is yes, preserve taxonomy continuity and
revise only the recommendation layer.
