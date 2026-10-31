# ADR 0002: Evidence-aware entity resolution

## Status

Accepted for the first resolution implementation.

## Decision

Entity resolution is tenant-scoped and multi-signal. The resolver uses exact identifier matching first, followed by conservative similarity across identifiers, name tokens, and optional Bangladesh location attributes.

The resolver returns a decision with:

- selected entity, if safe enough;
- confidence score;
- ranked candidates;
- individual matching signals;
- explicit state;
- review requirement and rationale.

`VERIFIED` is reserved for a unique exact identifier match within an organization. Fuzzy matches are never silently promoted to verified identity.

## Consequences

- OCR and legacy identifier errors can be investigated without overwriting the original value.
- Identical identifiers in different tenants cannot cross-link.
- Ambiguous matches remain reviewable instead of becoming false graph edges.
- The resolver is deterministic and can be evaluated against the generator's ground truth.
