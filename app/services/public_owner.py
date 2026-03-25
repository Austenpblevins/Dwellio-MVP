from __future__ import annotations

from app.models.parcel import ParcelOwnerSummary

ENTITY_TOKENS = {
    "ASSOCIATES",
    "ASSOCIATION",
    "BANK",
    "CHURCH",
    "CITY",
    "COMPANY",
    "CORP",
    "CORPORATION",
    "COUNTY",
    "DISTRICT",
    "ESTATE",
    "HOLDINGS",
    "INC",
    "INCORPORATED",
    "ISD",
    "LLC",
    "LLP",
    "LTD",
    "LP",
    "PARTNERS",
    "PARTNERSHIP",
    "PROPERTIES",
    "PROPERTY",
    "SCHOOL",
    "TRUST",
}
NAME_SUFFIXES = {"II", "III", "IV", "JR", "JR.", "SR", "SR."}


def build_public_owner_summary(
    owner_name: str | None,
    *,
    confidence_score: float | None = None,
) -> ParcelOwnerSummary:
    normalized_owner_name = " ".join((owner_name or "").split())
    if not normalized_owner_name:
        return ParcelOwnerSummary()

    if _looks_like_entity_owner(normalized_owner_name):
        return ParcelOwnerSummary(
            display_name=normalized_owner_name,
            owner_type="entity",
            privacy_mode="public_entity_name",
            confidence_label=_confidence_label(confidence_score),
        )

    return ParcelOwnerSummary(
        display_name=_mask_individual_owner_name(normalized_owner_name),
        owner_type="individual",
        privacy_mode="masked_individual_name",
        confidence_label=_confidence_label(confidence_score),
    )


def _looks_like_entity_owner(owner_name: str) -> bool:
    normalized = owner_name.upper().replace(",", " ")
    tokens = {token for token in normalized.split() if token}
    return bool(tokens & ENTITY_TOKENS)


def _mask_individual_owner_name(owner_name: str) -> str:
    original_tokens = [token for token in owner_name.replace(",", " ").split() if token]
    meaningful_tokens = [token for token in original_tokens if token.upper() not in NAME_SUFFIXES]

    if not meaningful_tokens:
        return owner_name
    if len(meaningful_tokens) == 1:
        return f"{meaningful_tokens[0][0]}."

    leading_initials = " ".join(f"{token[0]}." for token in meaningful_tokens[:-1])
    return f"{leading_initials} {meaningful_tokens[-1]}"


def _confidence_label(confidence_score: float | None) -> str:
    if confidence_score is None:
        return "limited"
    if confidence_score >= 0.8:
        return "high"
    if confidence_score >= 0.55:
        return "medium"
    return "limited"
