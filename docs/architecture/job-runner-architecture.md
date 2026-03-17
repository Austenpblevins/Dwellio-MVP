# Job Runner Architecture (Final Reconciled)

Nightly order:
1. job_fetch_sources
2. job_load_staging
3. job_normalize
4. job_geocode_repair
5. job_sales_ingestion
6. job_features
7. job_comp_candidates
8. job_score_models
9. job_score_savings
10. job_refresh_quote_cache
11. job_packet_refresh
