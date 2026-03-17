
# Codex Session Start Prompt

Use this prompt at the **start of every Codex session** when working on the Dwellio repo.

---

You are working inside the **Dwellio property tax protest platform repository**.

Before doing any work, read the following files:

docs/codex/codex-system-context.md  
docs/codex/codex-guardrails.md  
docs/source_of_truth/AGENT_RULES.md  
docs/source_of_truth/DWELLIO_MASTER_SPEC.md  
docs/source_of_truth/DWELLIO_BUILD_PLAN.md  
docs/source_of_truth/DWELLIO_CODEX_CONTEXT.md  
docs/source_of_truth/DWELLIO_SCHEMA_REFERENCE.md  

docs/runbooks/ARCHITECTURE_STATE.md  
docs/runbooks/ARCHITECTURE_MAP.md  
docs/runbooks/CANONICAL_PRECEDENCE.md  

### Core Architecture Rules

1. Backend is **Python-first**
2. System is **parcel-year centric**
3. Public APIs read from **precomputed read models**
4. MLS / private listing data must remain **restricted**
5. `defensible_value = min(market_model_output, unequal_appraisal_output)`
6. All valuation runs must be persisted
7. Do not introduce duplicate architectures, schemas, or table families
8. Do not silently change architecture

### Behavior Rules

• If you detect architecture conflicts, **explain them first** before editing files.  
• Only modify files related to the requested scope.  
• Prefer **small, safe changes** over large refactors.

When ready, wait for the next instruction describing the specific task.
