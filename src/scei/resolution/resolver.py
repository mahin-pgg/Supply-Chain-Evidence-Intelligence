from __future__ import annotations

from difflib import SequenceMatcher
from uuid import UUID

from scei.resolution.models import (
    EntityProfile,
    ResolutionCandidate,
    ResolutionDecision,
    ResolutionSignal,
    ResolutionState,
)
from scei.resolution.normalization import (
    identifier_variants,
    name_tokens,
    normalize_identifier,
    normalize_name,
)


class EntityResolver:
    """Deterministic first-pass resolver with explicit uncertainty."""

    def __init__(self, profiles: list[EntityProfile]) -> None:
        self._profiles = profiles
        self._identifiers: dict[tuple[UUID, str, str], list[EntityProfile]] = {}
        for profile in profiles:
            for identifier in profile.identifiers:
                for variant in identifier_variants(identifier):
                    self._identifiers.setdefault(
                        (profile.organization_id, profile.entity_type, variant), []
                    ).append(profile)

    def resolve(
        self,
        organization_id: UUID,
        entity_type: str,
        observed_value: str,
        *,
        district: str | None = None,
        country_code: str | None = None,
        limit: int = 5,
    ) -> ResolutionDecision:
        exact_profiles = self._exact_candidates(organization_id, entity_type, observed_value)
        if len(exact_profiles) == 1:
            selected = exact_profiles[0]
            candidate = ResolutionCandidate(
                entity_id=selected.entity_id,
                score=1.0,
                state=ResolutionState.VERIFIED,
                signals=[
                    ResolutionSignal(
                        name="identifier_exact",
                        score=1.0,
                        explanation="Observed identifier matches one tenant-scoped identifier.",
                    )
                ],
            )
            return ResolutionDecision(
                organization_id=organization_id,
                entity_type=entity_type,
                observed_value=observed_value,
                selected_entity_id=selected.entity_id,
                state=ResolutionState.VERIFIED,
                score=1.0,
                candidates=[candidate],
                review_required=False,
                rationale="Unique exact identifier match within the organization.",
            )

        candidates = [
            self._score(profile, observed_value, district, country_code)
            for profile in self._profiles
            if profile.organization_id == organization_id and profile.entity_type == entity_type
        ]
        candidates.sort(key=lambda candidate: candidate.score, reverse=True)
        candidates = candidates[:limit]
        if not candidates:
            return self._unknown(
                organization_id,
                entity_type,
                observed_value,
                "No tenant-scoped candidates were available.",
            )

        top = candidates[0]
        runner_up = candidates[1].score if len(candidates) > 1 else 0.0
        ambiguous = top.score - runner_up < 0.08
        if top.score >= 0.90 and not ambiguous:
            state = ResolutionState.PROBABLE
        elif top.score >= 0.72 and not ambiguous:
            state = ResolutionState.POSSIBLE
        else:
            state = (
                ResolutionState.CONTRADICTED
                if top.score >= 0.55 and ambiguous
                else ResolutionState.UNKNOWN
            )
        selected_id = (
            top.entity_id if state in {ResolutionState.PROBABLE, ResolutionState.POSSIBLE} else None
        )
        return ResolutionDecision(
            organization_id=organization_id,
            entity_type=entity_type,
            observed_value=observed_value,
            selected_entity_id=selected_id,
            state=state,
            score=top.score,
            candidates=[
                candidate.model_copy(
                    update={"state": state if candidate is top else ResolutionState.POSSIBLE}
                )
                for candidate in candidates
            ],
            review_required=state not in {ResolutionState.PROBABLE, ResolutionState.VERIFIED},
            rationale=self._rationale(state, top.score, runner_up),
        )

    def _exact_candidates(
        self, organization_id: UUID, entity_type: str, observed_value: str
    ) -> list[EntityProfile]:
        matches: dict[UUID, EntityProfile] = {}
        for variant in identifier_variants(observed_value):
            for profile in self._identifiers.get((organization_id, entity_type, variant), []):
                matches[profile.entity_id] = profile
        return list(matches.values())

    def _score(
        self,
        profile: EntityProfile,
        observed_value: str,
        district: str | None,
        country_code: str | None,
    ) -> ResolutionCandidate:
        observed_identifier = normalize_identifier(observed_value)
        identifier_score = max(
            (
                SequenceMatcher(None, observed_identifier, normalize_identifier(identifier)).ratio()
                for identifier in profile.identifiers
            ),
            default=0.0,
        )
        observed_tokens = name_tokens(observed_value)
        profile_tokens = name_tokens(profile.canonical_name)
        token_score = len(observed_tokens & profile_tokens) / max(
            len(observed_tokens | profile_tokens), 1
        )
        signals = [
            ResolutionSignal(
                name="identifier_similarity",
                score=identifier_score,
                explanation="Edit similarity between observed and known identifiers.",
            ),
            ResolutionSignal(
                name="token_overlap",
                score=token_score,
                explanation="Overlap between normalized observed and canonical name tokens.",
            ),
        ]
        score = 0.75 * identifier_score + 0.25 * token_score
        if district and profile.district:
            location_score = (
                1.0 if normalize_name(district) == normalize_name(profile.district) else 0.0
            )
            signals.append(
                ResolutionSignal(
                    name="district_match",
                    score=location_score,
                    explanation="Observed district compared with the profile district.",
                )
            )
            score = 0.7 * score + 0.3 * location_score
        if country_code and profile.country_code:
            country_score = (
                1.0 if country_code.casefold() == profile.country_code.casefold() else 0.0
            )
            signals.append(
                ResolutionSignal(
                    name="country_match",
                    score=country_score,
                    explanation="Observed country compared with the profile country.",
                )
            )
            score = 0.85 * score + 0.15 * country_score
        return ResolutionCandidate(
            entity_id=profile.entity_id,
            score=round(min(score, 1.0), 6),
            state=ResolutionState.UNKNOWN,
            signals=signals,
        )

    @staticmethod
    def _unknown(
        organization_id: UUID, entity_type: str, observed_value: str, rationale: str
    ) -> ResolutionDecision:
        return ResolutionDecision(
            organization_id=organization_id,
            entity_type=entity_type,
            observed_value=observed_value,
            selected_entity_id=None,
            state=ResolutionState.UNKNOWN,
            score=0.0,
            candidates=[],
            review_required=True,
            rationale=rationale,
        )

    @staticmethod
    def _rationale(state: ResolutionState, score: float, runner_up: float) -> str:
        if state == ResolutionState.CONTRADICTED:
            return f"Top candidates are too close to merge safely (top={score:.3f}, runner_up={runner_up:.3f})."
        if state == ResolutionState.UNKNOWN:
            return f"No candidate exceeded the safe threshold (top={score:.3f})."
        return (
            f"Best candidate scored {score:.3f}; retain the signals and evidence for review policy."
        )
