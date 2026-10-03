-- Migration: the final settlement is itemised per PP 35/2021
--
-- pesangon_amount used to be one lump; the items it is made of, and the
-- inputs they were computed from, now ride their own columns so a settlement
-- reads (and audits) without re-running the calculation:
--
--   uang_pesangon  Pasal 40(2) months x the reason's pesangon multiplier x wage
--   upmk           Pasal 40(3) months x the reason's UPMK multiplier x wage
--   uang_pisah     company separation pay, for the reasons that earn it
--   pesangon_amount  stays the severance total = the three above
--
-- plus the wage base, the daily wage and work pattern it came from, the
-- leave days paid, the years of service, the legal basis and the effective
-- date of the statutory set applied.
--
-- last_pay_via_payroll states what net_payable means: when true (the only
-- mode today) the last pay in base_pay is paid by the final payroll run and is
-- NOT part of net_payable. Existing rows were drafted under the same rule, so
-- the default is true for them as well. Existing rows keep NULL items: they
-- were computed before the itemisation and are not recomputed.

ALTER TABLE lifecycle.final_settlements
    ADD COLUMN IF NOT EXISTS uang_pesangon            numeric(18,2) CHECK (uang_pesangon >= 0),
    ADD COLUMN IF NOT EXISTS upmk                     numeric(18,2) CHECK (upmk >= 0),
    ADD COLUMN IF NOT EXISTS uang_pisah               numeric(18,2) CHECK (uang_pisah >= 0),
    ADD COLUMN IF NOT EXISTS monthly_wage             numeric(18,2) CHECK (monthly_wage >= 0),
    ADD COLUMN IF NOT EXISTS daily_wage               numeric(18,2) CHECK (daily_wage >= 0),
    ADD COLUMN IF NOT EXISTS work_days_per_week       int CHECK (work_days_per_week IN (5, 6)),
    ADD COLUMN IF NOT EXISTS unused_leave_days        numeric(8,2),
    ADD COLUMN IF NOT EXISTS tenure_years             numeric(8,4),
    ADD COLUMN IF NOT EXISTS legal_basis              varchar(120),
    ADD COLUMN IF NOT EXISTS statutory_effective_from date,
    ADD COLUMN IF NOT EXISTS last_pay_via_payroll     boolean NOT NULL DEFAULT true;
