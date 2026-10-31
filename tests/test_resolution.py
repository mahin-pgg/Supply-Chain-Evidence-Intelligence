from uuid import uuid4

from scei.resolution.models import EntityProfile, ResolutionState
from scei.resolution.normalization import normalize_identifier, normalize_name
from scei.resolution.resolver import EntityResolver


def test_normalization_keeps_bengali_text_and_compacts_identifiers():
    assert normalize_identifier("LOT-A-102") == "lota102"
    assert "ঢাকা" in normalize_name("ঢাকা Pharma Ltd.")


def test_exact_identifier_is_verified_within_tenant():
    tenant = uuid4()
    profile = EntityProfile(
        entity_id=uuid4(),
        organization_id=tenant,
        entity_type="batch",
        canonical_name="API batch",
        identifiers=["LOT-A-102"],
        district="Gazipur",
        country_code="BD",
    )
    decision = EntityResolver([profile]).resolve(tenant, "batch", "LOT-A-102")
    assert decision.state == ResolutionState.VERIFIED
    assert decision.selected_entity_id == profile.entity_id
    assert decision.review_required is False


def test_same_identifier_in_another_tenant_is_not_visible():
    tenant_a, tenant_b = uuid4(), uuid4()
    profile = EntityProfile(
        entity_id=uuid4(),
        organization_id=tenant_a,
        entity_type="batch",
        canonical_name="API batch",
        identifiers=["LOT-A-102"],
    )
    decision = EntityResolver([profile]).resolve(tenant_b, "batch", "LOT-A-102")
    assert decision.state == ResolutionState.UNKNOWN
    assert decision.selected_entity_id is None


def test_ambiguous_fuzzy_match_requires_review():
    tenant = uuid4()
    profiles = [
        EntityProfile(
            entity_id=uuid4(),
            organization_id=tenant,
            entity_type="supplier",
            canonical_name="ABC Chemicals Limited",
            identifiers=["ABC-100"],
            district="Dhaka",
            country_code="BD",
        ),
        EntityProfile(
            entity_id=uuid4(),
            organization_id=tenant,
            entity_type="supplier",
            canonical_name="ABC Chemical Limited",
            identifiers=["ABC-101"],
            district="Dhaka",
            country_code="BD",
        ),
    ]
    decision = EntityResolver(profiles).resolve(
        tenant, "supplier", "ABC-10", district="Dhaka", country_code="BD"
    )
    assert decision.state in {ResolutionState.CONTRADICTED, ResolutionState.UNKNOWN}
    assert decision.review_required is True
