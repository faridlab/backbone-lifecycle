-- Migration: offboarding reasons name the PP 35/2021 termination case
--
-- The severance multipliers depend on WHY employment ended, at the
-- granularity of PP 35/2021 Pasal 41-57 — a merger, an efficiency to prevent
-- losses and an efficiency because of losses pay differently. The nine
-- original labels were too coarse to select them, so the enum gains one value
-- per case. The original labels stay valid (existing rows keep reading); the
-- settlement maps each of them onto one case, and refuses the unspecific
-- `termination`.
--
-- ADD VALUE only appends; nothing in this migration uses the new values, so
-- it is safe inside the migration transaction.

ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'merger_consolidation_split';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'takeover';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'takeover_changed_terms';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'efficiency_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'efficiency_prevent_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'closure_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'closure_not_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'closure_force_majeure';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'force_majeure_no_closure';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'pkpu_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'pkpu_not_losses';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'bankruptcy';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'employer_violation_request';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'worker_request_rejected';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'absence_without_notice';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'violation_after_warnings';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'urgent_violation';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'detained_company_loss';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'detained_no_company_loss';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'convicted_company_loss';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'convicted_no_company_loss';
ALTER TYPE offboarding_reason ADD VALUE IF NOT EXISTS 'prolonged_illness';
