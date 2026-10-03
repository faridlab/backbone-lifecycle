-- Migration: PP 35/2021 severance parameters as effective-dated rows
--
-- The severance a leaver is owed is national law, not a company setting:
-- PP 35/2021 Pasal 40(2) (uang pesangon by years of service), Pasal 40(3)
-- (uang penghargaan masa kerja, UPMK) and Pasal 41-57 (the multipliers per
-- termination case). They live here as versioned rows, resolved AS OF the
-- leaver's last working day: for each table the row set with the greatest
-- effective_from <= that day applies, as a whole.
--
-- Global masters on purpose: no company or org-unit column and no CRUD
-- surface — the law is country-wide, and a per-company edit of it must not be
-- possible. When the law changes, the correction is a NEW COMPLETE set at the
-- new effective_from (every row restated, unchanged ones copied verbatim),
-- never an edit of live rows and never a code change. The resolver refuses a
-- set that is incomplete (a scale that does not open at zero years, a reason
-- table missing a case it knows), so a lone correction row reads as a broken
-- set and the settlement is refused rather than computed from half a law.
--
-- Values: PP 35/2021 as promulgated (in force 2021-02-02).

CREATE SCHEMA IF NOT EXISTS lifecycle;

-- Pasal 40(2): months of wage by completed years of service. A band opens at
-- min_service_years; the next band's min_service_years closes it.
CREATE TABLE IF NOT EXISTS lifecycle.severance_pesangon_scale (
  country_code      text          NOT NULL,
  effective_from    date          NOT NULL,
  min_service_years int           NOT NULL,
  months            numeric(5,2)  NOT NULL,
  PRIMARY KEY (country_code, effective_from, min_service_years),
  CHECK (min_service_years >= 0),
  CHECK (months >= 0)
);

-- Pasal 40(3): UPMK months of wage by completed years of service (nothing
-- below three years — the zero band is stated explicitly so a set is complete
-- only when it opens at zero).
CREATE TABLE IF NOT EXISTS lifecycle.severance_upmk_scale (
  country_code      text          NOT NULL,
  effective_from    date          NOT NULL,
  min_service_years int           NOT NULL,
  months            numeric(5,2)  NOT NULL,
  PRIMARY KEY (country_code, effective_from, min_service_years),
  CHECK (min_service_years >= 0),
  CHECK (months >= 0)
);

-- Pasal 41-57: per termination case, the multiplier on the Pasal 40(2) and
-- Pasal 40(3) amounts, and whether the case earns uang pisah (whose amount
-- is company policy — a setting, not law). reason_code is the
-- offboarding_reason value naming the case.
CREATE TABLE IF NOT EXISTS lifecycle.severance_reason_params (
  country_code         text          NOT NULL,
  effective_from       date          NOT NULL,
  reason_code          text          NOT NULL,
  pesangon_multiplier  numeric(5,2)  NOT NULL,
  upmk_multiplier      numeric(5,2)  NOT NULL,
  uang_pisah_eligible  boolean       NOT NULL,
  article              text          NOT NULL,
  PRIMARY KEY (country_code, effective_from, reason_code),
  CHECK (pesangon_multiplier >= 0),
  CHECK (upmk_multiplier >= 0)
);

INSERT INTO lifecycle.severance_pesangon_scale (country_code, effective_from, min_service_years, months) VALUES
  ('ID','2021-02-02',0,1),
  ('ID','2021-02-02',1,2),
  ('ID','2021-02-02',2,3),
  ('ID','2021-02-02',3,4),
  ('ID','2021-02-02',4,5),
  ('ID','2021-02-02',5,6),
  ('ID','2021-02-02',6,7),
  ('ID','2021-02-02',7,8),
  ('ID','2021-02-02',8,9)
ON CONFLICT DO NOTHING;

INSERT INTO lifecycle.severance_upmk_scale (country_code, effective_from, min_service_years, months) VALUES
  ('ID','2021-02-02',0,0),
  ('ID','2021-02-02',3,2),
  ('ID','2021-02-02',6,3),
  ('ID','2021-02-02',9,4),
  ('ID','2021-02-02',12,5),
  ('ID','2021-02-02',15,6),
  ('ID','2021-02-02',18,7),
  ('ID','2021-02-02',21,8),
  ('ID','2021-02-02',24,10)
ON CONFLICT DO NOTHING;

INSERT INTO lifecycle.severance_reason_params
  (country_code, effective_from, reason_code, pesangon_multiplier, upmk_multiplier, uang_pisah_eligible, article) VALUES
  ('ID','2021-02-02','merger_consolidation_split', 1.00, 1, false, 'PP 35/2021 Pasal 41'),
  ('ID','2021-02-02','takeover',                   1.00, 1, false, 'PP 35/2021 Pasal 42(1)'),
  ('ID','2021-02-02','takeover_changed_terms',     0.50, 1, false, 'PP 35/2021 Pasal 42(2)'),
  ('ID','2021-02-02','efficiency_losses',          0.50, 1, false, 'PP 35/2021 Pasal 43(1)'),
  ('ID','2021-02-02','efficiency_prevent_losses',  1.00, 1, false, 'PP 35/2021 Pasal 43(2)'),
  ('ID','2021-02-02','closure_losses',             0.50, 1, false, 'PP 35/2021 Pasal 44(1)'),
  ('ID','2021-02-02','closure_not_losses',         1.00, 1, false, 'PP 35/2021 Pasal 44(2)'),
  ('ID','2021-02-02','closure_force_majeure',      0.50, 1, false, 'PP 35/2021 Pasal 45(1)'),
  ('ID','2021-02-02','force_majeure_no_closure',   0.75, 1, false, 'PP 35/2021 Pasal 45(2)'),
  ('ID','2021-02-02','pkpu_losses',                0.50, 1, false, 'PP 35/2021 Pasal 46(1)'),
  ('ID','2021-02-02','pkpu_not_losses',            1.00, 1, false, 'PP 35/2021 Pasal 46(2)'),
  ('ID','2021-02-02','bankruptcy',                 0.50, 1, false, 'PP 35/2021 Pasal 47'),
  ('ID','2021-02-02','employer_violation_request', 1.00, 1, false, 'PP 35/2021 Pasal 48'),
  ('ID','2021-02-02','worker_request_rejected',    0.00, 0, true,  'PP 35/2021 Pasal 49'),
  ('ID','2021-02-02','resignation',                0.00, 0, true,  'PP 35/2021 Pasal 50'),
  ('ID','2021-02-02','absence_without_notice',     0.00, 0, true,  'PP 35/2021 Pasal 51'),
  ('ID','2021-02-02','violation_after_warnings',   0.50, 1, false, 'PP 35/2021 Pasal 52(1)'),
  ('ID','2021-02-02','urgent_violation',           0.00, 0, true,  'PP 35/2021 Pasal 52(2)'),
  ('ID','2021-02-02','detained_company_loss',      0.00, 0, true,  'PP 35/2021 Pasal 54(1)'),
  ('ID','2021-02-02','detained_no_company_loss',   0.00, 1, false, 'PP 35/2021 Pasal 54(2)'),
  ('ID','2021-02-02','convicted_company_loss',     0.00, 0, true,  'PP 35/2021 Pasal 54(4)'),
  ('ID','2021-02-02','convicted_no_company_loss',  0.00, 1, false, 'PP 35/2021 Pasal 54(5)'),
  ('ID','2021-02-02','prolonged_illness',          2.00, 1, false, 'PP 35/2021 Pasal 55'),
  ('ID','2021-02-02','retirement',                 1.75, 1, false, 'PP 35/2021 Pasal 56'),
  ('ID','2021-02-02','death',                      2.00, 1, false, 'PP 35/2021 Pasal 57')
ON CONFLICT DO NOTHING;
