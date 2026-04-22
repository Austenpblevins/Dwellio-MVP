ALTER TABLE instant_quote_request_logs
  ADD COLUMN IF NOT EXISTS warning_action_classes text[] NOT NULL DEFAULT ARRAY[]::text[],
  ADD COLUMN IF NOT EXISTS dominant_warning_action_class text,
  ADD COLUMN IF NOT EXISTS warning_taxonomy_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS opportunity_vs_savings_state text,
  ADD COLUMN IF NOT EXISTS product_state_reason_code text;

COMMENT ON COLUMN instant_quote_request_logs.warning_action_classes IS 'Distinct internal warning action classes derived from current blockers, warning codes, and runtime guardrails.';
COMMENT ON COLUMN instant_quote_request_logs.dominant_warning_action_class IS 'Highest-priority internal warning action class for the served request, ordered as suppress > constrain > disclose > QA_only.';
COMMENT ON COLUMN instant_quote_request_logs.warning_taxonomy_json IS 'Internal warning taxonomy payload for the served request, including warning code, action class, severity, subsystem, unit mask, and disclosure metadata.';
COMMENT ON COLUMN instant_quote_request_logs.opportunity_vs_savings_state IS 'Internal Stage 2 product-state classification for the served request.';
COMMENT ON COLUMN instant_quote_request_logs.product_state_reason_code IS 'Deterministic internal reason code explaining the Stage 2 product-state classification.';
