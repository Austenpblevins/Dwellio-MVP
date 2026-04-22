ALTER TABLE instant_quote_request_logs
  ADD COLUMN IF NOT EXISTS shadow_profile_version text,
  ADD COLUMN IF NOT EXISTS shadow_savings_estimate_raw numeric,
  ADD COLUMN IF NOT EXISTS shadow_savings_delta_raw numeric,
  ADD COLUMN IF NOT EXISTS shadow_tax_profile_status text,
  ADD COLUMN IF NOT EXISTS shadow_tax_profile_quality_score integer,
  ADD COLUMN IF NOT EXISTS shadow_marginal_model_type text,
  ADD COLUMN IF NOT EXISTS shadow_marginal_tax_rate_total numeric,
  ADD COLUMN IF NOT EXISTS shadow_opportunity_vs_savings_state text,
  ADD COLUMN IF NOT EXISTS shadow_limiting_reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS shadow_fallback_tax_profile_used_flag boolean;

COMMENT ON COLUMN instant_quote_request_logs.shadow_profile_version IS 'Internal tax-profile version used for the Stage 5 shadow savings comparison payload.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_savings_estimate_raw IS 'Internal V5 shadow savings estimate computed from the materialized tax profile without changing public quote behavior.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_savings_delta_raw IS 'Internal difference between the Stage 5 shadow savings estimate and the current public savings estimate raw value.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_tax_profile_status IS 'Materialized tax-profile status used by the Stage 5 shadow comparison path.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_tax_profile_quality_score IS 'Materialized tax-profile quality score used by the Stage 5 shadow comparison path.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_marginal_model_type IS 'Materialized marginal-model classification used by the Stage 5 shadow comparison path.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_marginal_tax_rate_total IS 'Materialized total marginal rate used by the Stage 5 shadow comparison path when a quoteable shadow estimate is available.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_opportunity_vs_savings_state IS 'Materialized product-state classification used by the Stage 5 shadow comparison path.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_limiting_reason_codes IS 'Internal limiting reason codes carried from the materialized tax profile into the Stage 5 shadow comparison payload.';
COMMENT ON COLUMN instant_quote_request_logs.shadow_fallback_tax_profile_used_flag IS 'Internal flag indicating whether the Stage 5 shadow comparison relied on a fallback tax profile or prior-year tax basis.';
