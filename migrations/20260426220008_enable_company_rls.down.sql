-- Down: remove the company RLS fence for lifecycle module

-- Reverse the company RLS fence for lifecycle.clearance_items
DROP POLICY IF EXISTS clearance_items_company_isolation ON lifecycle.clearance_items;
ALTER TABLE lifecycle.clearance_items NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.clearance_items DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.exit_interviews
DROP POLICY IF EXISTS exit_interviews_company_isolation ON lifecycle.exit_interviews;
ALTER TABLE lifecycle.exit_interviews NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.exit_interviews DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.final_settlements
DROP POLICY IF EXISTS final_settlements_company_isolation ON lifecycle.final_settlements;
ALTER TABLE lifecycle.final_settlements NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.final_settlements DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.offboardings
DROP POLICY IF EXISTS offboardings_company_isolation ON lifecycle.offboardings;
ALTER TABLE lifecycle.offboardings NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.offboardings DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.onboardings
DROP POLICY IF EXISTS onboardings_company_isolation ON lifecycle.onboardings;
ALTER TABLE lifecycle.onboardings NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.onboardings DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.onboarding_tasks
DROP POLICY IF EXISTS onboarding_tasks_company_isolation ON lifecycle.onboarding_tasks;
ALTER TABLE lifecycle.onboarding_tasks NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.onboarding_tasks DISABLE ROW LEVEL SECURITY;

-- Reverse the company RLS fence for lifecycle.promotions
DROP POLICY IF EXISTS promotions_company_isolation ON lifecycle.promotions;
ALTER TABLE lifecycle.promotions NO FORCE ROW LEVEL SECURITY;
ALTER TABLE lifecycle.promotions DISABLE ROW LEVEL SECURITY;

