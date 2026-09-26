-- Migration: promotions gain the appraisal rating snapshot
--
-- The rating at FILING time, stamped from the validated appraisal: what a
-- later audit reads (the weeks-long window between filing and effective can
-- never audit against a different number). NULL keeps rows without an
-- appraisal reference unchanged.

ALTER TABLE lifecycle.promotions
    ADD COLUMN IF NOT EXISTS appraisal_rating numeric(3,1);
