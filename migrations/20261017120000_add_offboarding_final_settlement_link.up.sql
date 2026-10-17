-- Migration: an offboarding names its final settlement
--
-- One settlement may be drafted per offboarding, and a second draft is
-- refused. The offboarding row now carries the id of the settlement drafted
-- for it, so a reader can tell the settlement is already worked out without
-- a second query. The draft verb sets it in the same transaction as the
-- insert; NULL means none has been drafted yet.

ALTER TABLE lifecycle.offboardings
    ADD COLUMN IF NOT EXISTS final_settlement_id uuid;

-- Link the settlements drafted before the column existed.
UPDATE lifecycle.offboardings o
   SET final_settlement_id = s.id
  FROM lifecycle.final_settlements s
 WHERE s.offboarding_id = o.id
   AND (s.metadata->>'deleted_at') IS NULL
   AND o.final_settlement_id IS NULL;
