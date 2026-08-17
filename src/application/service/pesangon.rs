//! 🇮🇩 Pesangon (severance) calculation — pure, config-driven functions.
//!
//! The Indonesian end-of-employment severance package, computed at offboarding.
//! Four components (all in IDR, rounded to 2 dp):
//!
//!   - **UPMK** (*Uang Penghargaan Masa Kerja* — service appreciation):
//!     `upmk_scale(tenure) × monthly_salary`. The scale graduates with tenure
//!     years: `<2→1, <3→2, <4→3, <5→4, <6→5, <7→6, <8→7, ≥8→8` months of salary.
//!   - **Pesangon** (severance, reason-dependent):
//!     `min(months_per_year × tenure × salary, cap_months × salary)` for
//!     config-keyed reasons; special-cased for `resignation` (tenure-scaled) and
//!     `misconduct`/`end_of_contract`/`retirement` (zero via a `{0,0}` rule).
//!   - **UPM** (*Uang Penggantian Hak* — rights replacement):
//!     `upm_rate × (pesangon + upmk)` when `tenure ≥ 1`, else `0`.
//!   - **unused-leave payout**: `unused_leave_days × (monthly_salary / working_days_per_month)`.
//!
//! Design rules (mirrors `backbone_payroll::statutory_calcs`):
//!   - **Pure math only.** No DB, no ports, no async. Every rate/scale/rule lives in
//!     [`PesangonConfig`] (loaded from `config/application.yml` or built via
//!     [`PesangonConfig::default`], which bakes current-law values). The calc bodies
//!     hardcode *the formula structure*, never the numeric rates.
//!   - **Same crate as the enum.** Unlike payroll's `PtkpTier` mirror, this calc lives
//!     inside `backbone-lifecycle`, so it references [`OffboardingReason`] directly —
//!     no Cargo edge, no mirror needed.
//!   - **Rounding.** Every monetary output is rounded to 2 dp (rupiah + sen) with
//!     `RoundingStrategy::MidpointAwayFromZero`, applied once per component.
//!   - **Fallibility.** A reason absent from `reason_rules` returns
//!     [`PesangonError::UnknownReason`] — fail closed (a malformed config must never
//!     silently produce a wrong severance). `Resignation` is the only special-cased
//!     reason (its pesangon follows the UPMK scale, not a linear `months_per_year`
//!     rule), so it is intentionally NOT in `reason_rules`.

use crate::domain::entity::OffboardingReason;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Two-decimal rounding used for every pesangon money output.
const MONEY_DP: u32 = 2;
/// Round a Decimal to ledger precision (2 dp, half-up). Shared with the final-settlement
/// draft verb, which prorates base pay with the same convention.
pub(crate) fn money(d: Decimal) -> Decimal {
    d.round_dp_with_strategy(MONEY_DP, RoundingStrategy::MidpointAwayFromZero)
}

// ============================================================================
// Errors
// ============================================================================

/// Errors raised by the pesangon calculator. Only the reason-rule lookup and the
/// YAML loader are fallible; the math itself is total.
#[derive(Debug, thiserror::Error)]
pub enum PesangonError {
    /// The offboarding reason is not present in `reason_rules` — the config is
    /// incomplete. (`Resignation` is special-cased and never looked up, so it does
    /// not need a rule entry.)
    #[error("unknown offboarding reason '{0}' — add it to pesangon.reason_rules in config/application.yml")]
    UnknownReason(String),
    /// A pesangon config YAML could not be parsed.
    #[error("invalid pesangon config YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// A config file could not be read.
    #[error("pesangon config I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ============================================================================
// Config
// ============================================================================

/// Top-level pesangon configuration — the `pesangon:` block of `config/application.yml`.
///
/// Construct with [`PesangonConfig::default`] (current-law values baked in) or load
/// from YAML via [`PesangonConfig::from_yaml_str`] /
/// [`PesangonConfig::load_from_config_dir`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PesangonConfig {
    /// UPM rate — rights replacement fraction of (pesangon + upmk). Default `0.15`.
    #[serde(default = "default_upm_rate")]
    pub upm_rate: Decimal,
    /// Working days per month — divisor for the unused-leave daily-rate payout.
    /// Default `22`.
    #[serde(default = "default_working_days_per_month")]
    pub working_days_per_month: Decimal,
    /// UPMK scale steps (service-appreciation months by tenure), sorted ascending by
    /// `min_tenure_years`. The largest step with `min_tenure_years <= tenure` wins.
    #[serde(default = "default_upmk_scale")]
    pub upmk_scale: Vec<UpmkScaleStep>,
    /// Reason → pesangon rule map, keyed by the lowercase snake_case reason label
    /// (`"efficiency"`, `"merger_acquisition"`, …). `resignation` is special-cased in
    /// the calc and deliberately absent here.
    #[serde(default = "default_reason_rules")]
    pub reason_rules: HashMap<String, ReasonRule>,
}

/// One step of the UPMK (service appreciation) scale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpmkScaleStep {
    /// Inclusive lower tenure bound (years) at which `months` applies.
    pub min_tenure_years: Decimal,
    /// Months of salary awarded at this tenure band.
    pub months: u32,
}

/// Pesangon rule for one reason: `months_per_year × tenure × salary`, capped at
/// `cap_months × salary`. A `{0, 0}` rule yields zero pesangon (misconduct,
/// end_of_contract, retirement).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasonRule {
    /// Months of salary per year of service.
    pub months_per_year: Decimal,
    /// Hard cap in months of salary.
    pub cap_months: Decimal,
}

fn default_upm_rate() -> Decimal {
    Decimal::new(15, 2) // 0.15
}
fn default_working_days_per_month() -> Decimal {
    Decimal::new(22, 0)
}
fn default_upmk_scale() -> Vec<UpmkScaleStep> {
    vec![
        UpmkScaleStep { min_tenure_years: Decimal::ZERO, months: 1 },
        UpmkScaleStep { min_tenure_years: Decimal::new(2, 0), months: 2 },
        UpmkScaleStep { min_tenure_years: Decimal::new(3, 0), months: 3 },
        UpmkScaleStep { min_tenure_years: Decimal::new(4, 0), months: 4 },
        UpmkScaleStep { min_tenure_years: Decimal::new(5, 0), months: 5 },
        UpmkScaleStep { min_tenure_years: Decimal::new(6, 0), months: 6 },
        UpmkScaleStep { min_tenure_years: Decimal::new(7, 0), months: 7 },
        UpmkScaleStep { min_tenure_years: Decimal::new(8, 0), months: 8 },
    ]
}
fn default_reason_rules() -> HashMap<String, ReasonRule> {
    let one = Decimal::new(1, 0);
    let half = Decimal::new(5, 1); // 0.5
    let eight = Decimal::new(8, 0);
    let four = Decimal::new(4, 0);
    let zero = Decimal::ZERO;
    [
        // Merger / acquisition — 1 mo/yr, cap 8 (PP 35/2021).
        ("merger_acquisition", ReasonRule { months_per_year: one, cap_months: eight }),
        // Efficiency / restructuring — 1 mo/yr, cap 8.
        ("efficiency", ReasonRule { months_per_year: one, cap_months: eight }),
        // Force majeure — 0.5 mo/yr, cap 4.
        ("force_majeure", ReasonRule { months_per_year: half, cap_months: four }),
        // Employer-initiated termination — 1 mo/yr, cap 8 (configurable).
        ("termination", ReasonRule { months_per_year: one, cap_months: eight }),
        // Fixed-term contract ended naturally — 0 (separate regime; configurable).
        ("end_of_contract", ReasonRule { months_per_year: zero, cap_months: zero }),
        // Retirement — 0 pesangon (pension/JHT rules apply; configurable).
        ("retirement", ReasonRule { months_per_year: zero, cap_months: zero }),
        // Death — per UU Manpower: heirs receive the package (configurable; JKM
        // insurance is booked separately). Default mirrors a standard severance.
        ("death", ReasonRule { months_per_year: one, cap_months: eight }),
        // Gross misconduct — forfeits severance (configurable; UPMK/UPM still computed
        // per the literal formula, which keys only the pesangon component on reason).
        ("misconduct", ReasonRule { months_per_year: zero, cap_months: zero }),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

impl Default for PesangonConfig {
    /// Current-law (UU 13/2003 + PP 35/2021) pesangon values. Kept in sync with the
    /// `pesangon:` block of `config/application.yml`; when the law changes, update
    /// both (or just the YAML).
    fn default() -> Self {
        PesangonConfig {
            upm_rate: default_upm_rate(),
            working_days_per_month: default_working_days_per_month(),
            upmk_scale: default_upmk_scale(),
            reason_rules: default_reason_rules(),
        }
    }
}

impl PesangonConfig {
    /// Parse the `pesangon:` block out of a full `application.yml` document string.
    /// If the block is absent, the current-law [`PesangonConfig::default`] is returned
    /// so a lifecycle node that has not yet added the block still boots with correct
    /// values.
    pub fn from_yaml_str(application_yml: &str) -> Result<Self, PesangonError> {
        let root: serde_yaml::Value = serde_yaml::from_str(application_yml)?;
        match root.get("pesangon") {
            Some(block) => Ok(serde_yaml::from_value(block.clone())?),
            None => Ok(Self::default()),
        }
    }

    /// Load from a config directory containing `application.yml` (+ optional
    /// `application-{env}.yml` override). The env file's **whole** `pesangon:` block
    /// replaces the base block when present (shallow override — you tune rules by
    /// overriding the entire section).
    pub fn load_from_config_dir(dir: &Path, environment: &str) -> Result<Self, PesangonError> {
        let base = std::fs::read_to_string(dir.join("application.yml"))?;
        let mut root: serde_yaml::Value = serde_yaml::from_str(&base)?;

        let env_path = dir.join(format!("application-{}.yml", environment));
        if env_path.exists() {
            let env_str = std::fs::read_to_string(&env_path)?;
            if let Ok(env_root) = serde_yaml::from_str::<serde_yaml::Value>(&env_str) {
                if let Some(env_block) = env_root.get("pesangon") {
                    match root.get_mut("pesangon") {
                        Some(slot) => *slot = env_block.clone(),
                        None => {
                            if let serde_yaml::Value::Mapping(ref mut m) = root {
                                m.insert(
                                    serde_yaml::Value::String("pesangon".into()),
                                    env_block.clone(),
                                );
                            }
                        }
                    }
                }
            }
        }

        match root.get("pesangon") {
            Some(block) => Ok(serde_yaml::from_value(block.clone())?),
            None => Ok(Self::default()),
        }
    }

    /// UPMK months of salary for `tenure_years`: the largest scale step whose
    /// `min_tenure_years <= tenure`. Returns the `min_tenure=0` step (1 month) if no
    /// step matches (e.g. negative tenure clamped conceptually — the scale's first
    /// step always matches a non-negative tenure).
    fn upmk_months(&self, tenure_years: Decimal) -> u32 {
        let mut months = self
            .upmk_scale
            .first()
            .map(|s| s.months)
            .unwrap_or(0);
        for step in &self.upmk_scale {
            if tenure_years >= step.min_tenure_years {
                months = step.months;
            } else {
                break;
            }
        }
        months
    }
}

// ============================================================================
// Calculation
// ============================================================================

/// Full pesangon breakdown — one computed component per statutory line, all in IDR
/// at 2 dp, plus the `total`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PesangonBreakdown {
    /// Pesangon (severance) — reason-dependent.
    pub pesangon: Decimal,
    /// UPMK — service appreciation (`upmk_scale × salary`).
    pub upmk: Decimal,
    /// UPM — rights replacement (`upm_rate × (pesangon + upmk)` when tenure ≥ 1).
    pub upm: Decimal,
    /// Payout for unused leave days.
    pub unused_leave_payout: Decimal,
    /// pesangon + upmk + upm + unused_leave_payout.
    pub total: Decimal,
}

/// **Pesangon** — the 🇮🇩 severance package for one leaver.
///
/// Inputs:
/// - `reason` — selects the pesangon formula (config-keyed, except `Resignation`).
/// - `tenure_years` — years of service (Decimal so fractional years from day-level
///   `join_date` math are honoured; clamped to `>= 0`).
/// - `monthly_salary` — last gross monthly salary (the base for every component).
/// - `unused_leave_days` — remaining leave paid out (clamped to `>= 0`).
/// - `cfg` — the [`PesangonConfig`] (rates/scale/rules).
///
/// Returns the full [`PesangonBreakdown`] with each component rounded to 2 dp.
pub fn pesangon(
    reason: OffboardingReason,
    tenure_years: Decimal,
    monthly_salary: Decimal,
    unused_leave_days: Decimal,
    cfg: &PesangonConfig,
) -> Result<PesangonBreakdown, PesangonError> {
    // Clamp inputs to a non-negative domain.
    let tenure = if tenure_years < Decimal::ZERO {
        Decimal::ZERO
    } else {
        tenure_years
    };
    let unused_leave = if unused_leave_days < Decimal::ZERO {
        Decimal::ZERO
    } else {
        unused_leave_days
    };

    // ---- UPMK — always upmk_scale(tenure) × salary, independent of reason. -------
    let upmk = money(Decimal::from(cfg.upmk_months(tenure)) * monthly_salary);

    // ---- Pesangon — reason-dependent. -------------------------------------------
    let pesangon = if reason == OffboardingReason::Resignation {
        // Voluntary resignation: pesangon scaled by tenure per the UPMK scale table.
        //   <2yr: 0  ·  [2,8)yr: upmk_scale(tenure) × salary  ·  ≥8yr: 0
        // (≥8yr resignation receives UPMK + UPM only, no severance.)
        if tenure < Decimal::new(2, 0) || tenure >= Decimal::new(8, 0) {
            Decimal::ZERO
        } else {
            money(Decimal::from(cfg.upmk_months(tenure)) * monthly_salary)
        }
    } else {
        let rule = cfg
            .reason_rules
            .get(&reason.to_string())
            .ok_or_else(|| PesangonError::UnknownReason(reason.to_string()))?;
        let raw = rule.months_per_year * tenure * monthly_salary;
        let cap = rule.cap_months * monthly_salary;
        let amount = if raw < cap { raw } else { cap };
        money(amount)
    };

    // ---- UPM — rights replacement, only when tenure ≥ 1. ------------------------
    let upm = if tenure >= Decimal::new(1, 0) {
        money(cfg.upm_rate * (pesangon + upmk))
    } else {
        Decimal::ZERO
    };

    // ---- Unused-leave payout ----------------------------------------------------
    let daily_rate = if cfg.working_days_per_month.is_zero() {
        Decimal::ZERO
    } else {
        monthly_salary / cfg.working_days_per_month
    };
    let unused_leave_payout = money(unused_leave * daily_rate);

    let total = money(pesangon + upmk + upm + unused_leave_payout);

    Ok(PesangonBreakdown {
        pesangon,
        upmk,
        upm,
        unused_leave_payout,
        total,
    })
}

// ============================================================================
// Tests — the gate. Hand-computed expected values.
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Convenience: the default (current-law) config.
    fn cfg() -> PesangonConfig {
        PesangonConfig::default()
    }

    // ---- The two anchor cases from the spec --------------------------------

    #[test]
    fn efficiency_3yr_12m_salary_is_82_8m() {
        // efficiency, 3yr tenure, 12M salary, 0 unused leave.
        //   UPMK  = upmk_scale(3) × 12M         = 3 × 12M = 36,000,000
        //   pesangon = min(1×3×12M, 8×12M=96M)   = 36,000,000
        //   UPM   = 0.15 × (36M + 36M)          = 10,800,000
        //   leave = 0
        //   total = 36M + 36M + 10.8M           = 82,800,000
        let b = pesangon(
            OffboardingReason::Efficiency,
            Decimal::new(3, 0),
            Decimal::new(12_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .expect("efficiency is in the default reason_rules");
        assert_eq!(b.upmk, Decimal::new(36_000_000, 0));
        assert_eq!(b.pesangon, Decimal::new(36_000_000, 0));
        assert_eq!(b.upm, Decimal::new(10_800_000, 0));
        assert_eq!(b.unused_leave_payout, Decimal::ZERO);
        assert_eq!(b.total, Decimal::new(82_800_000, 0));
    }

    #[test]
    fn resignation_8yr_plus_pays_upmk_upm_only() {
        // ≥8yr resignation → pesangon = 0, UPMK = 8 × salary, UPM = 0.15 × UPMK.
        //   UPMK  = 8 × 10M = 80,000,000
        //   pesangon = 0
        //   UPM   = 0.15 × (0 + 80M) = 12,000,000
        //   total = 0 + 80M + 12M = 92,000,000
        let b = pesangon(
            OffboardingReason::Resignation,
            Decimal::new(8, 0),
            Decimal::new(10_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.pesangon, Decimal::ZERO);
        assert_eq!(b.upmk, Decimal::new(80_000_000, 0));
        assert_eq!(b.upm, Decimal::new(12_000_000, 0));
        assert_eq!(b.total, Decimal::new(92_000_000, 0));
    }

    // ---- Force majeure exercises the 0.5 mo/yr rate + the 4-month cap ------

    #[test]
    fn force_majeure_caps_at_4_months() {
        // force_majeure, 10yr tenure, 10M salary, 0 unused leave.
        //   UPMK  = upmk_scale(10) × 10M = 8 × 10M = 80,000,000
        //   pesangon raw = 0.5 × 10 × 10M = 50,000,000 ; cap = 4 × 10M = 40,000,000
        //          → capped to 40,000,000
        //   UPM   = 0.15 × (40M + 80M) = 0.15 × 120M = 18,000,000
        //   total = 40M + 80M + 18M = 138,000,000
        let b = pesangon(
            OffboardingReason::ForceMajeure,
            Decimal::new(10, 0),
            Decimal::new(10_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.upmk, Decimal::new(80_000_000, 0));
        assert_eq!(b.pesangon, Decimal::new(40_000_000, 0));
        assert_eq!(b.upm, Decimal::new(18_000_000, 0));
        assert_eq!(b.total, Decimal::new(138_000_000, 0));
    }

    // ---- Resignation in the [2,8) band uses the UPMK scale ----------------

    #[test]
    fn resignation_3yr_partial_per_scale() {
        // resignation, 3yr tenure (in [2,8)), 10M salary, 0 unused leave.
        //   UPMK    = 3 × 10M = 30,000,000
        //   pesangon = upmk_scale(3) × salary = 3 × 10M = 30,000,000
        //   UPM     = 0.15 × (30M + 30M) = 9,000,000
        //   total   = 30M + 30M + 9M = 69,000,000
        let b = pesangon(
            OffboardingReason::Resignation,
            Decimal::new(3, 0),
            Decimal::new(10_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.upmk, Decimal::new(30_000_000, 0));
        assert_eq!(b.pesangon, Decimal::new(30_000_000, 0));
        assert_eq!(b.upm, Decimal::new(9_000_000, 0));
        assert_eq!(b.total, Decimal::new(69_000_000, 0));
    }

    #[test]
    fn resignation_under_2yr_gets_no_pesangon() {
        // <2yr resignation → pesangon = 0 (and UPM = 0 since tenure < 1 here).
        //   UPMK    = upmk_scale(1) × 5M = 1 × 5M = 5,000,000
        //   pesangon = 0
        //   UPM     = 0  (tenure 1 < … actually tenure=1 ≥1 → UPM = 0.15 × 5M = 750,000)
        let b = pesangon(
            OffboardingReason::Resignation,
            Decimal::new(1, 0),
            Decimal::new(5_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.pesangon, Decimal::ZERO);
        assert_eq!(b.upmk, Decimal::new(5_000_000, 0));
        // tenure 1 ≥ 1, so UPM applies on (0 + 5M).
        assert_eq!(b.upm, Decimal::new(750_000, 0));
        assert_eq!(b.total, Decimal::new(5_750_000, 0));
    }

    // ---- Unused-leave payout path ------------------------------------------

    #[test]
    fn unused_leave_payout_uses_daily_rate() {
        // efficiency, 3yr, 12M salary, 5 unused leave days, 22 working days/month.
        //   daily = 12M / 22 = 545,454.5454…  →  × 5 = 2,727,272.7272… → 2,727,272.73
        //   (UPMK/pesangon/UPM identical to efficiency_3yr_12m_salary_is_82_8m)
        //   total = 36M + 36M + 10.8M + 2,727,272.73 = 85,527,272.73
        let b = pesangon(
            OffboardingReason::Efficiency,
            Decimal::new(3, 0),
            Decimal::new(12_000_000, 0),
            Decimal::new(5, 0),
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.unused_leave_payout, Decimal::from_str("2727272.73").unwrap());
        assert_eq!(b.total, Decimal::from_str("85527272.73").unwrap());
    }

    // ---- Misconduct → pesangon = 0 (config rule {0,0}) --------------------

    #[test]
    fn misconduct_pays_zero_pesangon_but_keeps_upmk() {
        // misconduct, 4yr, 8M salary, 0 unused leave.
        //   UPMK    = upmk_scale(4) × 8M = 4 × 8M = 32,000,000
        //   pesangon = 0 (rule {0,0})
        //   UPM     = 0.15 × (0 + 32M) = 4,800,000
        //   total   = 0 + 32M + 4.8M = 36,800,000
        let b = pesangon(
            OffboardingReason::Misconduct,
            Decimal::new(4, 0),
            Decimal::new(8_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        assert_eq!(b.pesangon, Decimal::ZERO);
        assert_eq!(b.upmk, Decimal::new(32_000_000, 0));
        assert_eq!(b.upm, Decimal::new(4_800_000, 0));
        assert_eq!(b.total, Decimal::new(36_800_000, 0));
    }

    // ---- UPMK scale boundary checks ---------------------------------------

    #[test]
    fn upmk_scale_boundaries() {
        let c = cfg();
        // <2 → 1, exactly 2 → 2, 2.9 → 2, 3 → 3, 7.999 → 7, 8 → 8, 20 → 8.
        assert_eq!(c.upmk_months(Decimal::new(1, 0)), 1);
        assert_eq!(c.upmk_months(Decimal::from_str("1.99").unwrap()), 1);
        assert_eq!(c.upmk_months(Decimal::new(2, 0)), 2);
        assert_eq!(c.upmk_months(Decimal::from_str("2.9").unwrap()), 2);
        assert_eq!(c.upmk_months(Decimal::new(3, 0)), 3);
        assert_eq!(c.upmk_months(Decimal::from_str("7.999").unwrap()), 7);
        assert_eq!(c.upmk_months(Decimal::new(8, 0)), 8);
        assert_eq!(c.upmk_months(Decimal::new(20, 0)), 8);
    }

    // ---- Config loading ----------------------------------------------------

    #[test]
    fn config_from_yaml_drives_same_calc_as_default() {
        // The shipped config/application.yml `pesangon:` block must produce identical
        // results to the baked-in Default — proving the calc is config-driven.
        let yaml = include_str!("../../../config/application.yml");
        let loaded = PesangonConfig::from_yaml_str(yaml).expect("application.yml parses");

        let via_default = pesangon(
            OffboardingReason::Efficiency,
            Decimal::new(3, 0),
            Decimal::new(12_000_000, 0),
            Decimal::ZERO,
            &cfg(),
        )
        .unwrap();
        let via_loaded = pesangon(
            OffboardingReason::Efficiency,
            Decimal::new(3, 0),
            Decimal::new(12_000_000, 0),
            Decimal::ZERO,
            &loaded,
        )
        .unwrap();
        assert_eq!(via_default, via_loaded);
        assert_eq!(via_loaded.total, Decimal::new(82_800_000, 0));
    }

    #[test]
    fn config_missing_pesangon_block_falls_back_to_default() {
        // A YAML with no `pesangon:` key still boots with the current-law defaults.
        let yaml = "server:\n  port: 8080\n";
        let loaded = PesangonConfig::from_yaml_str(yaml).unwrap();
        let b = pesangon(
            OffboardingReason::Efficiency,
            Decimal::new(3, 0),
            Decimal::new(12_000_000, 0),
            Decimal::ZERO,
            &loaded,
        )
        .unwrap();
        assert_eq!(b.total, Decimal::new(82_800_000, 0));
    }

    #[test]
    fn reason_to_string_keys_match_config() {
        // Every non-resignation variant must resolve against the default config map.
        for reason in [
            OffboardingReason::Termination,
            OffboardingReason::EndOfContract,
            OffboardingReason::Retirement,
            OffboardingReason::Death,
            OffboardingReason::MergerAcquisition,
            OffboardingReason::Efficiency,
            OffboardingReason::ForceMajeure,
            OffboardingReason::Misconduct,
        ] {
            assert!(
                cfg().reason_rules.contains_key(&reason.to_string()),
                "default config missing reason_rules entry for '{reason}'"
            );
        }
    }
}
