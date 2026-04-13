from __future__ import annotations

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
