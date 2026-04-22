ALTER TABLE instant_quote_request_logs
  ADD COLUMN IF NOT EXISTS public_rollout_state text,
  ADD COLUMN IF NOT EXISTS public_rollout_reason_code text,
  ADD COLUMN IF NOT EXISTS public_rollout_applied_flag boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN instant_quote_request_logs.public_rollout_state IS
  'Stage 6 public-facing product-state rollout classification kept additive to the request log.';

COMMENT ON COLUMN instant_quote_request_logs.public_rollout_reason_code IS
  'Reason code explaining why the Stage 6 product-state rollout mapping was selected.';

COMMENT ON COLUMN instant_quote_request_logs.public_rollout_applied_flag IS
  'True when Stage 6 materially changed public presentation fields without changing public savings math.';
