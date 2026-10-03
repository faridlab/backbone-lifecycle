-- Postgres cannot drop a value from an enum type without rebuilding the type
-- and every column that uses it, and rows recorded with a PP 35/2021 case
-- would have no earlier label to fall back to. The widening is therefore kept
-- on rollback.
SELECT 1;
