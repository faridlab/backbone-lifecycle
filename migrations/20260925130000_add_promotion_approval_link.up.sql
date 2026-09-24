-- Migration: promotions gain the approvals-engine link
--
-- The engine-gated lane: create files into the approvals engine (the
-- `promotion` resource type) and stamps the link; approve fails closed
-- unless the engine says Approved. NULL keeps the bespoke approve verb for
-- unwired deployments and rows created before the link existed.

ALTER TABLE lifecycle.promotions
    ADD COLUMN IF NOT EXISTS approval_request_id uuid;
