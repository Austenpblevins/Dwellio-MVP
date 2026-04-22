ALTER TABLE instant_quote_request_logs
  ADD COLUMN IF NOT EXISTS savings_translation_mode text,
  ADD COLUMN IF NOT EXISTS savings_translation_reason_code text,
  ADD COLUMN IF NOT EXISTS savings_translation_applied_flag boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS selected_public_savings_estimate_raw numeric;

COMMENT ON COLUMN instant_quote_request_logs.savings_translation_mode IS
  'Stage 7 selected public savings translation path, such as current formula or v5 shadow rollout.';

COMMENT ON COLUMN instant_quote_request_logs.savings_translation_reason_code IS
  'Reason code explaining why the Stage 7 savings translation path was selected.';

COMMENT ON COLUMN instant_quote_request_logs.savings_translation_applied_flag IS
  'True when Stage 7 changed the public savings estimate away from the default current formula.';

COMMENT ON COLUMN instant_quote_request_logs.selected_public_savings_estimate_raw IS
  'Raw savings estimate that backed the public response after Stage 7 savings translation gating.';
