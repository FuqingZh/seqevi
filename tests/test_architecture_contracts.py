from pathlib import Path


def test_claim_session_protocol_publishes_authority_route() -> None:
    contract = Path(
        "docs/architecture/20260813-v1.4-sequence-evidence-contract.md"
    ).read_text()
    assert (
        "/v1/internal/claim-sessions/"
        "{capabilities,open,renew,acquire,authority,finalize,close}" in contract
    )
