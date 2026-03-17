
# Dwellio Build Order Checklist

This checklist provides the **exact build order for the Dwellio platform**.
It breaks the system into phases and individual tasks so nothing is missed.

Use this alongside:

- docs/codex-build-playbook.md
- docs/codex-session-start.md
- docs/codex-task-prompts.md
- docs/codex-architecture-map.md
- docs/architecture-state.md

Mark items complete as you build.

---

# Phase 1 — Repository Setup

[ ] Initialize Git repository  
[ ] Connect repository to GitHub  
[ ] Verify folder structure

Required directories:

app/
docs/
sql/
tests/

---

# Phase 2 — Architecture Documents

[ ] Add source_of_truth documentation  
[ ] Add architecture docs  
[ ] Add Codex control documents

Verify presence of:

docs/source_of_truth/  
docs/architecture/  
docs/runbooks/  
docs/codex/

---

# Phase 3 — Database Layer

[ ] Create migration framework  
[ ] Implement base schema tables  
[ ] Implement sales tables  
[ ] Implement neighborhood analytics tables  
[ ] Implement comparable sales tables  
[ ] Implement valuation tables  
[ ] Implement protest tables

Key tables:

parcels  
parcel_improvements  
parcel_assessments  
parcel_sales  
sales_reconstruction  
neighborhood_stats  
market_stats  
comp_candidates  
valuation_runs  
parcel_savings_estimates  
decision_tree_results  
protest_recommendations  
protest_cases  
case_outcomes  

---

# Phase 4 — Read Models

[ ] Implement search read model view  
[ ] Implement quote read model view  
[ ] Validate read model performance  

Views:

v_search_read_model  
v_quote_read_model  

---

# Phase 5 — Domain Models

[ ] Create Parcel model  
[ ] Create Quote model  
[ ] Create Savings model  
[ ] Create DecisionTreeResult model  
[ ] Create Lead model  

Location:

app/models/

---

# Phase 6 — Domain Services

[ ] address_resolver service  
[ ] comp_scoring service  
[ ] market_model service  
[ ] equity_model service  
[ ] decision_tree service  
[ ] arb_probability service  
[ ] savings_engine service  
[ ] explanation_builder service  
[ ] packet_generator service  

Location:

app/services/

---

# Phase 7 — ETL Pipeline

[ ] Build ETL runner  
[ ] Implement data fetch job  
[ ] Implement staging loader  
[ ] Implement normalization job  
[ ] Implement geocode repair job  
[ ] Implement sales ingestion job  
[ ] Implement feature engineering job  
[ ] Implement comp candidate generation  
[ ] Implement valuation scoring job  
[ ] Implement savings scoring job  
[ ] Implement quote cache refresh  
[ ] Implement packet refresh job  

Location:

app/jobs/

---

# Phase 8 — County Data Adapters

[ ] Harris County adapter  
[ ] Fort Bend County adapter  

Responsibilities:

- download appraisal roll data
- normalize parcel identifiers
- extract exemption data
- load staging tables

Location:

app/county_adapters/

---

# Phase 9 — API Layer

[ ] Implement search endpoint  
[ ] Implement quote endpoint  
[ ] Implement quote explanation endpoint  
[ ] Implement lead submission endpoint  

Public endpoints:

GET /search  
GET /quote/{county}/{year}/{account}  
GET /quote/{county}/{year}/{account}/explanation  
POST /lead  

Location:

app/api/

---

# Phase 10 — Internal Operations APIs

[ ] protest case management endpoint  
[ ] admin management endpoint  
[ ] case outcome recording  

Location:

app/api/

---

# Phase 11 — Testing

[ ] address normalization unit tests  
[ ] comp scoring unit tests  
[ ] market model tests  
[ ] equity model tests  
[ ] decision tree tests  
[ ] savings engine tests  

Integration tests:

[ ] search endpoint  
[ ] quote endpoint  
[ ] lead submission  

Location:

tests/

---

# Phase 12 — Observability

[ ] logging framework  
[ ] ETL job logging  
[ ] API request logging  
[ ] error monitoring  

---

# Phase 13 — Documentation Sync

[ ] verify architecture docs match code  
[ ] verify API documentation  
[ ] verify schema documentation  
[ ] update architecture-state.md  

---

# Phase 14 — Final Audit

[ ] repository architecture audit  
[ ] schema consistency check  
[ ] API contract verification  
[ ] documentation reconciliation  

---

# Phase 15 — Production Preparation

[ ] environment configuration  
[ ] database backups  
[ ] deployment pipeline setup  
[ ] performance tuning  
[ ] security checks

---

# Completion Criteria

Dwellio MVP is complete when:

• Property search works  
• Quote generation works  
• Savings estimate is calculated  
• Leads can be captured  
• Protest recommendation is generated  
• ETL pipeline updates data nightly
