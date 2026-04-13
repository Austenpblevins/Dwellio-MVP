DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_type
    WHERE typname = 'validation_scope_enum'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t
      ON t.oid = e.enumtypid
    WHERE t.typname = 'validation_scope_enum'
      AND e.enumlabel = 'publish_control'
  ) THEN
    ALTER TYPE validation_scope_enum ADD VALUE 'publish_control';
  END IF;
END
$$;
