from __future__ import annotations

import sys

import pytest

from app.jobs import cli
from app.jobs.cli import _load_account_numbers, build_parser


def test_cli_loads_account_numbers_from_file(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.txt"
    accounts_path.write_text("1001\n\n1002\n1001\n", encoding="utf-8")

    assert _load_account_numbers(str(accounts_path)) == ["1001", "1002", "1001"]


def test_cli_parser_accepts_repeated_account_number_arguments() -> None:
    parser = build_parser()

    args = parser.parse_args(
        [
            "job_score_models",
            "--county-id",
            "harris",
            "--tax-year",
            "2025",
            "--account-number",
            "1001",
            "--account-number",
            "1002",
        ]
    )

    assert args.job_name == "job_score_models"
    assert args.account_numbers == ["1001", "1002"]


@pytest.mark.parametrize("job_name", sorted(cli.JOB_REGISTRY))
def test_cli_parser_accepts_every_registered_job(job_name: str) -> None:
    args = build_parser().parse_args([job_name])

    assert args.job_name == job_name
    assert args.account_numbers is None
    assert args.account_numbers_file is None
    assert args.dry_run is False


def test_cli_help_exits_successfully(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "--account-number" in output
    assert "--account-numbers-file" in output


@pytest.mark.parametrize(
    ("inline_accounts", "file_contents", "expected_accounts"),
    [
        ([], None, None),
        (["1001", "1002"], None, ("1001", "1002")),
        ([], " 1002 \n\n1003\n1002\n", ("1002", "1003")),
        ([" 1001 ", "1002", "1001", " "], "1002\n1003\n", ("1001", "1002", "1003")),
        ([], " \n\n", None),
    ],
)
def test_cli_dispatches_account_inputs_without_running_job(
    monkeypatch, tmp_path, inline_accounts, file_contents, expected_accounts
) -> None:
    argv = ["dwellio-job", "job_score_models", "--county-id", "harris", "--tax-year", "2025"]
    for account in inline_accounts:
        argv.extend(["--account-number", account])
    if file_contents is not None:
        accounts_path = tmp_path / "accounts.txt"
        accounts_path.write_text(file_contents, encoding="utf-8")
        argv.extend(["--account-numbers-file", str(accounts_path)])
    calls = []
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(cli, "execute_job", lambda *args, **kwargs: calls.append((args, kwargs)))

    cli.main()

    expected_kwargs = {"county_id": "harris", "tax_year": 2025}
    if expected_accounts is not None:
        expected_kwargs["account_numbers"] = expected_accounts
    assert calls == [(("job_score_models", cli.JOB_REGISTRY["job_score_models"]), expected_kwargs)]


def test_cli_preserves_optional_job_arguments(monkeypatch) -> None:
    status = sorted(cli.TAX_RATE_BASIS_STATUSES)[0]
    source = sorted(cli.TAX_RATE_ADOPTION_STATUS_SOURCES)[0]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dwellio-job",
            "job_set_tax_rate_adoption_status",
            "--dataset-type",
            "tax_rates",
            "--import-batch-id",
            "batch-1",
            "--tax-rate-basis-status",
            status,
            "--tax-rate-basis-status-reason",
            "reviewed",
            "--tax-rate-basis-status-note",
            "test note",
            "--tax-rate-basis-status-source",
            source,
            "--dry-run",
        ],
    )
    calls = []
    monkeypatch.setattr(cli, "execute_job", lambda *args, **kwargs: calls.append((args, kwargs)))

    cli.main()

    assert calls[0][1] == {
        "county_id": None,
        "tax_year": None,
        "dataset_type": "tax_rates",
        "import_batch_id": "batch-1",
        "tax_rate_basis_status": status,
        "tax_rate_basis_status_reason": "reviewed",
        "tax_rate_basis_status_note": "test note",
        "tax_rate_basis_status_source": source,
        "dry_run": True,
    }
