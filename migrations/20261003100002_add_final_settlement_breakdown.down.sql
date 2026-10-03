ALTER TABLE lifecycle.final_settlements
    DROP COLUMN IF EXISTS last_pay_via_payroll,
    DROP COLUMN IF EXISTS statutory_effective_from,
    DROP COLUMN IF EXISTS legal_basis,
    DROP COLUMN IF EXISTS tenure_years,
    DROP COLUMN IF EXISTS unused_leave_days,
    DROP COLUMN IF EXISTS work_days_per_week,
    DROP COLUMN IF EXISTS daily_wage,
    DROP COLUMN IF EXISTS monthly_wage,
    DROP COLUMN IF EXISTS uang_pisah,
    DROP COLUMN IF EXISTS upmk,
    DROP COLUMN IF EXISTS uang_pesangon;
