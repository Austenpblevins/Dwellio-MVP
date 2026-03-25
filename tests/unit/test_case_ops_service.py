from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

from app.models.case import (
    EvidenceCompSetCreate,
    EvidenceCompSetItemCreate,
    EvidencePacketCreate,
    EvidencePacketItemCreate,
    ProtestCaseCreate,
    ProtestCaseStatusUpdate,
)
from app.services.case_ops import CaseOpsService


class SequenceCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[dict[str, object] | None] | None = None,
        fetchall_results: list[list[dict[str, object]]] | None = None,
    ) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])

    def __enter__(self) -> SequenceCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        self.execute_calls.append((query, params))

    def fetchone(self) -> dict[str, object] | None:
        if not self.fetchone_results:
            return None
        return self.fetchone_results.pop(0)

    def fetchall(self) -> list[dict[str, object]]:
        if not self.fetchall_results:
            return []
        return self.fetchall_results.pop(0)


class SequenceConnection:
    def __init__(
        self,
        *,
        fetchone_results: list[dict[str, object] | None] | None = None,
        fetchall_results: list[list[dict[str, object]]] | None = None,
    ) -> None:
        self.cursor_instance = SequenceCursor(
            fetchone_results=fetchone_results,
            fetchall_results=fetchall_results,
        )
        self.commit_calls = 0

    def cursor(self) -> SequenceCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1


def connection_factory(connection: SequenceConnection):
    @contextmanager
    def _connection():
        yield connection

    return _connection


def test_create_case_links_client_parcel_and_status_history(monkeypatch) -> None:
    parcel_id = uuid4()
    client_id = uuid4()
    valuation_run_id = uuid4()
    connection = SequenceConnection(
        fetchone_results=[
            {"county_id": "harris"},
            {"protest_case_id": uuid4()},
        ]
    )
    monkeypatch.setattr("app.services.case_ops.get_connection", connection_factory(connection))

    result = CaseOpsService().create_case(
        request=ProtestCaseCreate(
            client_id=client_id,
            parcel_id=parcel_id,
            tax_year=2026,
            valuation_run_id=valuation_run_id,
            case_status="created",
            workflow_status_code="active",
        )
    )

    assert result.action == "create_protest_case"
    assert result.protest_case_id is not None
    assert connection.commit_calls == 1
    sql_text = "\n".join(query for query, _ in connection.cursor_instance.execute_calls)
    assert "INSERT INTO protest_cases" in sql_text
    assert "INSERT INTO client_parcels" in sql_text
    assert "INSERT INTO case_status_history" in sql_text


def test_update_case_status_appends_history(monkeypatch) -> None:
    protest_case_id = str(uuid4())
    connection = SequenceConnection(
        fetchone_results=[
            {"protest_case_id": uuid4(), "workflow_status_code": "packet_review"},
        ]
    )
    monkeypatch.setattr("app.services.case_ops.get_connection", connection_factory(connection))

    result = CaseOpsService().update_case_status(
        protest_case_id=protest_case_id,
        request=ProtestCaseStatusUpdate(
            case_status="packet_ready",
            workflow_status_code="packet_review",
            reason_text="packet rows assembled",
            changed_by="analyst@example.test",
        ),
    )

    assert result.action == "update_case_status"
    assert connection.commit_calls == 1
    sql_text = "\n".join(query for query, _ in connection.cursor_instance.execute_calls)
    assert "UPDATE protest_cases" in sql_text
    assert "INSERT INTO case_status_history" in sql_text


def test_create_packet_persists_items_and_comp_sets(monkeypatch) -> None:
    connection = SequenceConnection(
        fetchone_results=[
            {
                "parcel_id": uuid4(),
                "tax_year": 2026,
                "valuation_run_id": uuid4(),
            },
            {"evidence_packet_id": uuid4()},
            {"evidence_comp_set_id": uuid4()},
        ]
    )
    monkeypatch.setattr("app.services.case_ops.get_connection", connection_factory(connection))

    result = CaseOpsService().create_packet(
        request=EvidencePacketCreate(
            protest_case_id=uuid4(),
            packet_type="informal",
            packet_status="draft",
            items=[
                EvidencePacketItemCreate(
                    section_code="value_summary",
                    title="Value summary",
                    body_text="Market and equity support are ready for review.",
                    source_basis="quote_read_model",
                    display_order=10,
                )
            ],
            comp_sets=[
                EvidenceCompSetCreate(
                    basis_type="equity",
                    set_label="Equity support set",
                    items=[
                        EvidenceCompSetItemCreate(
                            parcel_sale_id=uuid4(),
                            comp_role="supporting",
                            comp_rank=1,
                            rationale_text="Closest neighborhood peer.",
                        )
                    ],
                )
            ],
        )
    )

    assert result.action == "create_evidence_packet"
    assert result.evidence_packet_id is not None
    assert connection.commit_calls == 1
    sql_text = "\n".join(query for query, _ in connection.cursor_instance.execute_calls)
    assert "INSERT INTO evidence_packets" in sql_text
    assert "INSERT INTO evidence_packet_items" in sql_text
    assert "INSERT INTO evidence_comp_sets" in sql_text
    assert "INSERT INTO evidence_comp_set_items" in sql_text


def test_list_cases_maps_internal_summary(monkeypatch) -> None:
    protest_case_id = uuid4()
    parcel_id = uuid4()
    connection = SequenceConnection(
        fetchall_results=[
            [
                {
                    "protest_case_id": protest_case_id,
                    "county_id": "harris",
                    "parcel_id": parcel_id,
                    "account_number": "1001001001001",
                    "tax_year": 2026,
                    "case_status": "active",
                    "workflow_status_code": "packet_review",
                    "address": "101 Main St, Houston, TX 77002",
                    "owner_name": "A. Example",
                    "client_id": uuid4(),
                    "client_name": "Alex Example",
                    "representation_agreement_id": uuid4(),
                    "valuation_run_id": uuid4(),
                    "packet_count": 1,
                    "note_count": 2,
                    "hearing_count": 0,
                    "latest_outcome_code": None,
                    "outcome_date": None,
                    "recommendation_code": "file_protest",
                    "expected_tax_savings_point": 975.0,
                    "created_at": None,
                    "updated_at": None,
                }
            ]
        ]
    )
    monkeypatch.setattr("app.services.case_ops.get_connection", connection_factory(connection))

    response = CaseOpsService().list_cases(county_id="harris", tax_year=2026)

    assert response.access_scope == "internal"
    assert len(response.cases) == 1
    assert response.cases[0].protest_case_id == str(protest_case_id)
    assert response.cases[0].packet_count == 1
    assert response.cases[0].recommendation_code == "file_protest"
