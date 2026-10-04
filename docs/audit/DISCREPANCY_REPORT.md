# Discrepancy Report

## Confirmed discrepancies

### 1. Job CLI parser failure — resolved October 3, 2026

`app/jobs/cli.py` registers `--account-number` at lines 70 and 84 and registers
`--account-numbers-file` at lines 71 and 85. Running
`python3 -m app.jobs.cli --help` fails while building the parser with:

```text
argparse.ArgumentError: argument --account-number: conflicting option string: --account-number
```

The preceding reproduction describes the audit baseline. The separately
approved repair removed duplicate account flag registration and the obsolete
`args.account_number` dispatch block, retaining the `account_numbers` path.
Verification: 26 CLI tests passed and module `--help` succeeded. Tests cover all
17 registered job names, repeated/file inputs, ordered deduplication, absent
inputs, help and optional argument forwarding. Job dispatch was mocked; no
database job ran and job implementations were unchanged.

### 2. Architecture status ledger predates current migrations

`docs/architecture-state.md` says it was verified against migration `0056`.
The repository contains 77 migrations through `0079`. The ledger must be
re-verified before its implementation claims are treated as authoritative.

### 3. Documentation authority is distributed

The source-of-truth, architecture, runbook, and final-summary collections have
overlapping descriptions of what is canonical, final, or authoritative. The
documents need an approved hierarchy and ownership/refresh policy.

## Important non-discrepancies / bounded conclusions

- There are exactly 48 operational script files under the defined baseline
  counting rule: script modules and launchers, excluding package/readme files.
- Packet review and schema foundations exist, but the generator and refresh job
  remain stubs; no final packet/PDF workflow was established.
- Instant-quote backend capability exists, but a complete customer-facing web
  integration was not established.
- Schema concepts for agreements and invoices do not establish onboarding,
  e-sign, billing, payment, or filing workflows.
