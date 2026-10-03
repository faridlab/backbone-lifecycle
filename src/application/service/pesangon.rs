//! Final-settlement calculation under PP 35/2021 — pure functions, no I/O.
//!
//! What a leaver is owed when employment ends, itemised the way the regulation
//! itemises it:
//!
//! - **Uang pesangon** (Pasal 40(2)): months of wage by completed years of
//!   service, times the pesangon multiplier of the termination case.
//! - **UPMK** — uang penghargaan masa kerja (Pasal 40(3)): months of wage by
//!   completed years of service (nothing below three years), times the case's
//!   UPMK multiplier.
//! - **Uang pisah**: separation pay for the cases that earn it (voluntary
//!   resignation, urgent violation, …). Its amount is company policy (the
//!   employment agreement, company regulation or collective agreement), so it
//!   comes from a setting, never from law data.
//! - **Uang penggantian hak** (Pasal 40(4)(a)): unused, unexpired annual leave,
//!   paid at the daily wage. Owed in every case.
//! - **Last pay**: the final month's wage for the days actually worked in it.
//!   Paid through payroll, so it is reported but not part of the settlement's
//!   payable (see [`SettlementBreakdown::last_pay_via_payroll`]).
//!
//! There is no 15% "uang penggantian perumahan serta pengobatan": that item
//! belonged to UU 13/2003 Pasal 156(4)(c) and PP 35/2021 does not carry it.
//!
//! ## Where the numbers come from
//!
//! The statutory numbers (both scales and the per-case multipliers) are NOT in
//! this file. They are effective-dated rows in the `lifecycle.severance_*`
//! tables, resolved as of the last working day into a [`SeveranceParams`] by
//! the persistence layer, which refuses an incomplete set. This file holds only
//! the structure of the formula. The company settings (uang pisah, the work
//! week, which leave types are paid out) arrive as [`SeveranceSettings`].
//!
//! ## Conventions
//!
//! - **Wage base.** `monthly_wage` is the wage the items are computed from: per
//!   UU 13/2003 Pasal 157 (as amended) that is base wage plus fixed allowances.
//!   The data model records one monthly salary figure per employee and no
//!   separate fixed-allowance line, so that figure is the base.
//! - **Daily wage.** PP 36/2021 Pasal 17: monthly wage / 21 for a five-day week,
//!   / 25 for a six-day week. One daily wage serves the last pay and the leave
//!   payout. It is rounded to the sen first and the items are multiplied from
//!   the rounded figure, so `daily_wage × days` on the stored row reproduces
//!   the stored amount exactly.
//! - **Years of service** run from the join date through the last working day
//!   inclusive, counted by anniversary: someone who joined on 3 October 2023
//!   and whose last day is 2 October 2026 has completed three years. The
//!   scales are keyed on completed years.
//! - **Never started.** A join date after the last working day means the
//!   employee never started: every item is zero.
//! - **Rounding.** Each money item is rounded once to 2 dp, half away from zero.

use crate::domain::entity::OffboardingReason;
use chrono::{Datelike, NaiveDate, Weekday};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The country whose statutory set the settlement reads.
pub const SEVERANCE_COUNTRY: &str = "ID";

/// Two-decimal rounding used for every money output.
const MONEY_DP: u32 = 2;

/// Round a Decimal to ledger precision (2 dp, half away from zero).
pub(crate) fn money(d: Decimal) -> Decimal {
    d.round_dp_with_strategy(MONEY_DP, RoundingStrategy::MidpointAwayFromZero)
}

// ============================================================================
// Errors
// ============================================================================

/// Why a settlement could not be computed. Every variant refuses: the
/// calculation never falls back to a guessed number.
#[derive(Debug, thiserror::Error)]
pub enum PesangonError {
    /// The offboarding reason does not say which PP 35/2021 case applies, so
    /// the multipliers cannot be chosen.
    #[error("offboarding reason '{0}' does not name a PP 35/2021 termination case; record the specific case (for example efficiency_prevent_losses, violation_after_warnings or closure_not_losses) before computing the settlement")]
    UnspecificReason(String),
    /// No statutory severance set is in force on the given day.
    #[error("no PP 35/2021 severance parameters are in force for country {country} on {as_of} ({table})")]
    NoStatutoryParams {
        country: String,
        as_of: NaiveDate,
        table: &'static str,
    },
    /// A statutory set exists but is not complete enough to compute from.
    #[error("the severance parameters in force for country {country} on {as_of} are incomplete: {detail}")]
    IncompleteStatutoryParams {
        country: String,
        as_of: NaiveDate,
        detail: String,
    },
    /// A company setting holds a value the calculation cannot use.
    #[error("setting {group}/{key} = '{value}' is invalid: {detail}")]
    InvalidSetting {
        group: &'static str,
        key: &'static str,
        value: String,
        detail: &'static str,
    },
    /// A database failure while reading the parameters or settings.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl PesangonError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            PesangonError::UnspecificReason(_) => "unspecific_offboarding_reason",
            PesangonError::NoStatutoryParams { .. } => "no_severance_params",
            PesangonError::IncompleteStatutoryParams { .. } => "incomplete_severance_params",
            PesangonError::InvalidSetting { .. } => "invalid_offboarding_setting",
            PesangonError::Db(_) => "internal_error",
        }
    }

    /// HTTP status for the HTTP surface: the caller can act on a reason, and an
    /// operator on missing parameters or a bad setting — none is a server fault
    /// except the database.
    pub fn http_status(&self) -> u16 {
        match self {
            PesangonError::Db(_) => 500,
            _ => 422,
        }
    }
}

// ============================================================================
// Statutory parameters (resolved from the effective-dated tables)
// ============================================================================

/// One band of a months-by-years-of-service scale: from `min_service_years`
/// completed years (inclusive) up to the next band's bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenureBand {
    pub min_service_years: u32,
    pub months: Decimal,
}

/// The statutory parameters of one termination case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReasonParams {
    /// Multiplier on the Pasal 40(2) amount.
    pub pesangon_multiplier: Decimal,
    /// Multiplier on the Pasal 40(3) amount.
    pub upmk_multiplier: Decimal,
    /// Whether the case earns uang pisah.
    pub uang_pisah_eligible: bool,
    /// The provision, e.g. "PP 35/2021 Pasal 50".
    pub article: String,
}

/// The statutory severance set in force on one day: both scales and the
/// per-case parameters, each with the `effective_from` of the row set it came
/// from. Built by the persistence layer and checked by [`SeveranceParams::check`].
#[derive(Debug, Clone, PartialEq)]
pub struct SeveranceParams {
    pub pesangon_scale: Vec<TenureBand>,
    pub pesangon_effective_from: NaiveDate,
    pub upmk_scale: Vec<TenureBand>,
    pub upmk_effective_from: NaiveDate,
    /// Keyed by the case code (an `offboarding_reason` value).
    pub reasons: HashMap<String, ReasonParams>,
    pub reasons_effective_from: NaiveDate,
}

impl SeveranceParams {
    /// The latest `effective_from` among the three tables — the date the
    /// combined set took the shape it has. Stamped on the settlement.
    pub fn effective_from(&self) -> NaiveDate {
        self.pesangon_effective_from
            .max(self.upmk_effective_from)
            .max(self.reasons_effective_from)
    }

    /// Refuse a set that cannot be computed from: a scale that is empty, does
    /// not open at zero years or is not strictly ascending, or a case table
    /// missing any case this build knows. Cases in the table that this build
    /// does not know are ignored (a newer set may add one).
    pub fn check(self, as_of: NaiveDate) -> Result<Self, PesangonError> {
        let incomplete = |detail: String| PesangonError::IncompleteStatutoryParams {
            country: SEVERANCE_COUNTRY.to_string(),
            as_of,
            detail,
        };
        for (name, scale) in [("pesangon scale", &self.pesangon_scale), ("UPMK scale", &self.upmk_scale)] {
            if scale.first().map(|b| b.min_service_years) != Some(0) {
                return Err(incomplete(format!("the {name} does not open at zero years of service")));
            }
            if scale.windows(2).any(|w| w[0].min_service_years >= w[1].min_service_years) {
                return Err(incomplete(format!("the {name} bands are not strictly ascending")));
            }
        }
        for code in STATUTORY_CASES {
            if !self.reasons.contains_key(*code) {
                return Err(incomplete(format!("no parameters for the case '{code}'")));
            }
        }
        Ok(self)
    }

    fn scale_months(scale: &[TenureBand], service_years: u32) -> Decimal {
        scale
            .iter()
            .take_while(|b| b.min_service_years <= service_years)
            .last()
            .map(|b| b.months)
            .unwrap_or(Decimal::ZERO)
    }

    /// Pasal 40(2) months for `service_years` completed years.
    pub fn pesangon_months(&self, service_years: u32) -> Decimal {
        Self::scale_months(&self.pesangon_scale, service_years)
    }

    /// Pasal 40(3) months for `service_years` completed years.
    pub fn upmk_months(&self, service_years: u32) -> Decimal {
        Self::scale_months(&self.upmk_scale, service_years)
    }
}

// ============================================================================
// Termination case
// ============================================================================

/// Every PP 35/2021 case this build computes; a statutory set must carry all
/// of them. Each is an `offboarding_reason` value.
pub const STATUTORY_CASES: &[&str] = &[
    "merger_consolidation_split",
    "takeover",
    "takeover_changed_terms",
    "efficiency_losses",
    "efficiency_prevent_losses",
    "closure_losses",
    "closure_not_losses",
    "closure_force_majeure",
    "force_majeure_no_closure",
    "pkpu_losses",
    "pkpu_not_losses",
    "bankruptcy",
    "employer_violation_request",
    "worker_request_rejected",
    "resignation",
    "absence_without_notice",
    "violation_after_warnings",
    "urgent_violation",
    "detained_company_loss",
    "detained_no_company_loss",
    "convicted_company_loss",
    "convicted_no_company_loss",
    "prolonged_illness",
    "retirement",
    "death",
];

/// How an offboarding reason is settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeveranceBasis {
    /// A PP 35/2021 case — the code of its row in the case table.
    Case(&'static str),
    /// A fixed-term (PKWT) contract that reached its end. It earns no pesangon,
    /// UPMK or uang pisah — only the unused-leave payout. The PKWT
    /// compensation of PP 35/2021 Pasal 15-16 (one month per twelve months of
    /// contract) is a separate entitlement that this calculation does not
    /// produce yet; it must be settled outside this module until it does.
    FixedTermEnd,
}

/// Legal-basis text recorded for a fixed-term contract end.
pub const FIXED_TERM_END_BASIS: &str =
    "PKWT ended — no pesangon or UPMK; PKWT compensation (PP 35/2021 Pasal 15-16) not included";

/// Map an offboarding reason onto the case it is settled under.
///
/// The PP 35/2021 values map to themselves. The earlier coarse labels map onto
/// one case each: `merger_acquisition` → Pasal 41, `efficiency` → Pasal 43(2)
/// (efficiency to prevent losses — the employee-favourable reading, since the
/// label does not say the company was losing money), `force_majeure` →
/// Pasal 45(2), `misconduct` → Pasal 52(2). `termination` names no case and is
/// refused.
pub fn severance_basis(reason: OffboardingReason) -> Result<SeveranceBasis, PesangonError> {
    use OffboardingReason as R;
    let case = match reason {
        R::Termination => return Err(PesangonError::UnspecificReason(reason.to_string())),
        R::EndOfContract => return Ok(SeveranceBasis::FixedTermEnd),
        R::MergerAcquisition => "merger_consolidation_split",
        R::Efficiency => "efficiency_prevent_losses",
        R::ForceMajeure => "force_majeure_no_closure",
        R::Misconduct => "urgent_violation",
        R::Resignation => "resignation",
        R::MergerConsolidationSplit => "merger_consolidation_split",
        R::Takeover => "takeover",
        R::TakeoverChangedTerms => "takeover_changed_terms",
        R::EfficiencyLosses => "efficiency_losses",
        R::EfficiencyPreventLosses => "efficiency_prevent_losses",
        R::ClosureLosses => "closure_losses",
        R::ClosureNotLosses => "closure_not_losses",
        R::ClosureForceMajeure => "closure_force_majeure",
        R::ForceMajeureNoClosure => "force_majeure_no_closure",
        R::PkpuLosses => "pkpu_losses",
        R::PkpuNotLosses => "pkpu_not_losses",
        R::Bankruptcy => "bankruptcy",
        R::EmployerViolationRequest => "employer_violation_request",
        R::WorkerRequestRejected => "worker_request_rejected",
        R::AbsenceWithoutNotice => "absence_without_notice",
        R::ViolationAfterWarnings => "violation_after_warnings",
        R::UrgentViolation => "urgent_violation",
        R::DetainedCompanyLoss => "detained_company_loss",
        R::DetainedNoCompanyLoss => "detained_no_company_loss",
        R::ConvictedCompanyLoss => "convicted_company_loss",
        R::ConvictedNoCompanyLoss => "convicted_no_company_loss",
        R::ProlongedIllness => "prolonged_illness",
        R::Retirement => "retirement",
        R::Death => "death",
    };
    Ok(SeveranceBasis::Case(case))
}

// ============================================================================
// Company settings
// ============================================================================

/// The work pattern the daily wage is derived from (PP 36/2021 Pasal 17).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkWeek {
    /// Monday to Friday; daily wage = monthly / 21.
    FiveDay,
    /// Monday to Saturday; daily wage = monthly / 25.
    SixDay,
}

impl WorkWeek {
    /// Working days per week (5 or 6).
    pub fn days(self) -> u8 {
        match self {
            WorkWeek::FiveDay => 5,
            WorkWeek::SixDay => 6,
        }
    }

    /// The PP 36/2021 Pasal 17 divisor from monthly to daily wage.
    pub fn daily_divisor(self) -> Decimal {
        match self {
            WorkWeek::FiveDay => Decimal::new(21, 0),
            WorkWeek::SixDay => Decimal::new(25, 0),
        }
    }

    /// Whether `day` is a scheduled working day. Public holidays falling on
    /// such a day count: they are paid days.
    pub fn is_working_day(self, day: Weekday) -> bool {
        match day {
            Weekday::Sun => false,
            Weekday::Sat => self == WorkWeek::SixDay,
            _ => true,
        }
    }

    /// From a days-per-week count; `None` for anything but 5 or 6.
    pub fn from_days(days: u8) -> Option<Self> {
        match days {
            5 => Some(WorkWeek::FiveDay),
            6 => Some(WorkWeek::SixDay),
            _ => None,
        }
    }
}

/// The company's settlement settings (from `platform.sysparams`, group
/// `lifecycle.offboarding`). The defaults below are what applies when a
/// setting is absent, and match the seeded defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct SeveranceSettings {
    /// Uang pisah, in months of `monthly_wage`, for the cases that earn it.
    /// Default 0 until the company records its policy.
    pub uang_pisah_months: Decimal,
    /// The work pattern of the daily wage. Default five-day: the data model
    /// records no per-employee work pattern.
    pub work_week: WorkWeek,
    /// Codes of the leave types whose unused balance is paid out (annual leave).
    /// Default `["ANNUAL"]`.
    pub leave_type_codes: Vec<String>,
}

impl Default for SeveranceSettings {
    fn default() -> Self {
        SeveranceSettings {
            uang_pisah_months: Decimal::ZERO,
            work_week: WorkWeek::FiveDay,
            leave_type_codes: vec!["ANNUAL".to_string()],
        }
    }
}

// ============================================================================
// Years of service
// ============================================================================

/// The day `years` anniversaries after `start`; a 29 February start falls on
/// 1 March in a year without one.
fn anniversary(start: NaiveDate, years: i32) -> NaiveDate {
    let year = start.year() + years;
    NaiveDate::from_ymd_opt(year, start.month(), start.day())
        .or_else(|| NaiveDate::from_ymd_opt(year, 3, 1))
        .expect("a valid calendar date")
}

/// Years of service from `join_date` through `last_working_day` inclusive.
///
/// The whole part is the number of completed anniversaries (what the statutory
/// scales are keyed on); the fraction is the share of the current service year
/// worked, truncated to 4 dp so it can never round up into the next band.
/// Zero when the employee never started.
pub fn tenure_years(join_date: NaiveDate, last_working_day: NaiveDate) -> Decimal {
    if join_date > last_working_day {
        return Decimal::ZERO;
    }
    // Service ends at the close of the last working day.
    let end = last_working_day.succ_opt().expect("a date before the calendar's end");
    let mut years = end.year() - join_date.year();
    if anniversary(join_date, years) > end {
        years -= 1;
    }
    let from = anniversary(join_date, years);
    let to = anniversary(join_date, years + 1);
    let fraction = Decimal::from((end - from).num_days()) / Decimal::from((to - from).num_days());
    (Decimal::from(years) + fraction).round_dp_with_strategy(4, RoundingStrategy::ToZero)
}

/// Completed years of service from a tenure figure (its whole part).
fn completed_years(tenure_years: Decimal) -> u32 {
    use rust_decimal::prelude::ToPrimitive;
    tenure_years.max(Decimal::ZERO).trunc().to_u32().unwrap_or(u32::MAX)
}

// ============================================================================
// Last pay
// ============================================================================

/// The final month's pay: from the later of the join date and the first day of
/// the last working day's month, through the last working day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastPay {
    pub amount: Decimal,
    /// First paid day; `None` when the employee never started.
    pub from: Option<NaiveDate>,
    /// Scheduled working days paid.
    pub working_days: u32,
}

/// Prorate the final month. A full calendar month pays the full monthly wage;
/// a part month pays `daily_wage × working days`, never more than the monthly
/// wage. Zero when the employee never started.
pub fn last_pay(
    join_date: NaiveDate,
    last_working_day: NaiveDate,
    monthly_wage: Decimal,
    work_week: WorkWeek,
) -> LastPay {
    if join_date > last_working_day {
        return LastPay { amount: Decimal::ZERO, from: None, working_days: 0 };
    }
    let month_start = last_working_day.with_day(1).expect("day 1 exists");
    let from = join_date.max(month_start);
    let working_days = from
        .iter_days()
        .take_while(|d| *d <= last_working_day)
        .filter(|d| work_week.is_working_day(d.weekday()))
        .count() as u32;
    let month_end = month_start
        .checked_add_months(chrono::Months::new(1))
        .and_then(|d| d.pred_opt())
        .expect("a month end");
    let amount = if from == month_start && last_working_day == month_end {
        money(monthly_wage)
    } else {
        let daily = daily_wage(monthly_wage, work_week);
        money(daily * Decimal::from(working_days)).min(money(monthly_wage))
    };
    LastPay { amount, from: Some(from), working_days }
}

/// The daily wage (PP 36/2021 Pasal 17), rounded to the sen.
pub fn daily_wage(monthly_wage: Decimal, work_week: WorkWeek) -> Decimal {
    money(monthly_wage / work_week.daily_divisor())
}

// ============================================================================
// The settlement
// ============================================================================

/// What the calculation is given.
#[derive(Debug, Clone, PartialEq)]
pub struct SettlementInputs {
    pub reason: OffboardingReason,
    pub join_date: NaiveDate,
    pub last_working_day: NaiveDate,
    /// The wage base (see the module notes).
    pub monthly_wage: Decimal,
    /// Unused, unexpired annual leave days.
    pub unused_leave_days: Decimal,
}

/// The itemised settlement. Field names are the ones the settlement row and
/// the `offboarding.closed` payload carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementBreakdown {
    /// The PP 35/2021 case settled under; `None` for a fixed-term contract end.
    pub severance_case: Option<String>,
    /// The provision, e.g. "PP 35/2021 Pasal 50".
    pub legal_basis: String,
    /// `effective_from` of the statutory set applied.
    pub statutory_effective_from: NaiveDate,
    /// False when the join date is after the last working day.
    pub started: bool,
    /// Years of service through the last working day (see [`tenure_years`]).
    pub tenure_years: Decimal,
    /// Completed years — the scale key.
    pub service_years: u32,
    pub monthly_wage: Decimal,
    pub daily_wage: Decimal,
    pub work_days_per_week: u8,
    /// Pasal 40(2) months for the tenure, before the multiplier.
    pub pesangon_months: Decimal,
    pub pesangon_multiplier: Decimal,
    /// Pasal 40(3) months for the tenure, before the multiplier.
    pub upmk_months: Decimal,
    pub upmk_multiplier: Decimal,
    pub uang_pesangon: Decimal,
    pub upmk: Decimal,
    pub uang_pisah: Decimal,
    /// uang_pesangon + upmk + uang_pisah.
    pub severance_total: Decimal,
    pub unused_leave_days: Decimal,
    /// unused_leave_days × daily_wage.
    pub unused_leave_payout: Decimal,
    /// The final month's pay (see [`last_pay`]).
    pub last_pay: Decimal,
    pub last_pay_from: Option<NaiveDate>,
    pub last_pay_working_days: u32,
    /// True: payroll pays the last pay, so it is not in `net_payable`.
    pub last_pay_via_payroll: bool,
    /// What the settlement books and pays: severance_total + unused_leave_payout.
    pub net_payable: Decimal,
}

/// Compute the settlement. Pure and total once the reason maps to a case and
/// the set carries it (both checked before any arithmetic).
pub fn compute_settlement(
    inputs: &SettlementInputs,
    params: &SeveranceParams,
    settings: &SeveranceSettings,
) -> Result<SettlementBreakdown, PesangonError> {
    let tenure = tenure_years(inputs.join_date, inputs.last_working_day);
    compute_with_tenure(inputs, tenure, params, settings)
}

/// [`compute_settlement`] with the years of service given rather than derived
/// from the dates — the seam the band-boundary tests drive.
fn compute_with_tenure(
    inputs: &SettlementInputs,
    tenure: Decimal,
    params: &SeveranceParams,
    settings: &SeveranceSettings,
) -> Result<SettlementBreakdown, PesangonError> {
    let basis = severance_basis(inputs.reason)?;
    let (severance_case, case_params) = match basis {
        SeveranceBasis::Case(code) => {
            let p = params.reasons.get(code).ok_or_else(|| {
                PesangonError::IncompleteStatutoryParams {
                    country: SEVERANCE_COUNTRY.to_string(),
                    as_of: inputs.last_working_day,
                    detail: format!("no parameters for the case '{code}'"),
                }
            })?;
            (Some(code.to_string()), Some(p))
        }
        SeveranceBasis::FixedTermEnd => (None, None),
    };
    let legal_basis = case_params
        .map(|p| p.article.clone())
        .unwrap_or_else(|| FIXED_TERM_END_BASIS.to_string());

    let started = inputs.join_date <= inputs.last_working_day;
    let work_week = settings.work_week;
    let monthly = inputs.monthly_wage.max(Decimal::ZERO);
    let daily = daily_wage(monthly, work_week);
    let tenure = if started { tenure.max(Decimal::ZERO) } else { Decimal::ZERO };
    let service_years = completed_years(tenure);

    let (pesangon_multiplier, upmk_multiplier, pisah_eligible) = match case_params {
        Some(p) => (p.pesangon_multiplier, p.upmk_multiplier, p.uang_pisah_eligible),
        None => (Decimal::ZERO, Decimal::ZERO, false),
    };
    let pesangon_months = params.pesangon_months(service_years);
    let upmk_months = params.upmk_months(service_years);

    let (uang_pesangon, upmk, uang_pisah, leave_days) = if started {
        (
            money(pesangon_months * pesangon_multiplier * monthly),
            money(upmk_months * upmk_multiplier * monthly),
            if pisah_eligible {
                money(settings.uang_pisah_months.max(Decimal::ZERO) * monthly)
            } else {
                Decimal::ZERO
            },
            inputs.unused_leave_days.max(Decimal::ZERO),
        )
    } else {
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    };
    let unused_leave_payout = money(daily * leave_days);
    let severance_total = money(uang_pesangon + upmk + uang_pisah);
    let last = last_pay(inputs.join_date, inputs.last_working_day, monthly, work_week);

    Ok(SettlementBreakdown {
        severance_case,
        legal_basis,
        statutory_effective_from: params.effective_from(),
        started,
        tenure_years: tenure,
        service_years,
        monthly_wage: monthly,
        daily_wage: daily,
        work_days_per_week: work_week.days(),
        pesangon_months,
        pesangon_multiplier,
        upmk_months,
        upmk_multiplier,
        uang_pesangon,
        upmk,
        uang_pisah,
        severance_total,
        unused_leave_days: leave_days,
        unused_leave_payout,
        last_pay: last.amount,
        last_pay_from: last.from,
        last_pay_working_days: last.working_days,
        last_pay_via_payroll: true,
        net_payable: money(severance_total + unused_leave_payout),
    })
}

impl SettlementBreakdown {
    /// The `pesangon_breakdown` object of the `offboarding.closed` payload: the
    /// itemised fields, plus the four names earlier consumers read
    /// (`pesangon` = uang pesangon, `upm` = always 0 since PP 35/2021 has no
    /// such item, `total` = net_payable) so a consumer built against the
    /// earlier payload keeps recording the right total.
    pub fn to_event_payload(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(self).expect("a plain struct serialises");
        if let serde_json::Value::Object(m) = &mut v {
            m.insert("pesangon".into(), serde_json::json!(self.uang_pesangon));
            m.insert("upm".into(), serde_json::json!(Decimal::ZERO));
            m.insert("total".into(), serde_json::json!(self.net_payable));
        }
        v
    }
}

// ============================================================================
// Tests — hand-computed expectations against the seeded statutory set.
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
    fn date(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    /// The statutory set exactly as the migration seeds it — parsed from the
    /// migration file, so these tests check the shipped law data, not a copy.
    fn seeded() -> SeveranceParams {
        let sql = include_str!(
            "../../../migrations/20261003100001_create_severance_statutory_tables.up.sql"
        );
        let mut pesangon = Vec::new();
        let mut upmk = Vec::new();
        let mut reasons = HashMap::new();
        let mut table = "";
        for line in sql.lines() {
            let t = line.trim();
            if t.starts_with("INSERT INTO lifecycle.severance_pesangon_scale") {
                table = "pesangon";
            } else if t.starts_with("INSERT INTO lifecycle.severance_upmk_scale") {
                table = "upmk";
            } else if t.starts_with("INSERT INTO lifecycle.severance_reason_params") {
                table = "reasons";
            }
            if !t.starts_with("('ID'") {
                continue;
            }
            let inner = t.trim_start_matches('(').trim_end_matches(',').trim_end_matches(')');
            let cols: Vec<String> = inner
                .split(',')
                .map(|c| c.trim().trim_matches('\'').to_string())
                .collect();
            match table {
                "pesangon" => pesangon.push(TenureBand {
                    min_service_years: cols[2].parse().unwrap(),
                    months: d(&cols[3]),
                }),
                "upmk" => upmk.push(TenureBand {
                    min_service_years: cols[2].parse().unwrap(),
                    months: d(&cols[3]),
                }),
                "reasons" => {
                    reasons.insert(
                        cols[2].clone(),
                        ReasonParams {
                            pesangon_multiplier: d(&cols[3]),
                            upmk_multiplier: d(&cols[4]),
                            uang_pisah_eligible: cols[5] == "true",
                            article: cols[6].clone(),
                        },
                    );
                }
                _ => {}
            }
        }
        SeveranceParams {
            pesangon_scale: pesangon,
            pesangon_effective_from: date("2021-02-02"),
            upmk_scale: upmk,
            upmk_effective_from: date("2021-02-02"),
            reasons,
            reasons_effective_from: date("2021-02-02"),
        }
        .check(date("2026-10-03"))
        .expect("the seeded set is complete")
    }

    const WAGE: &str = "10000000";

    /// The real calculation, with the years of service driven directly (the
    /// dates only need to make the employee started).
    fn calc_at(reason: OffboardingReason, tenure: &str, settings: &SeveranceSettings) -> SettlementBreakdown {
        compute_with_tenure(
            &SettlementInputs {
                reason,
                join_date: date("2000-01-01"),
                last_working_day: date("2026-10-02"),
                monthly_wage: d(WAGE),
                unused_leave_days: Decimal::ZERO,
            },
            d(tenure),
            &seeded(),
            settings,
        )
        .unwrap()
    }

    /// The real entry point, on a join date `years` + `days` before the last
    /// working day (inclusive counting).
    fn settle(reason: OffboardingReason, join: &str, last_day: &str, leave: &str, settings: &SeveranceSettings) -> SettlementBreakdown {
        compute_settlement(
            &SettlementInputs {
                reason,
                join_date: date(join),
                last_working_day: date(last_day),
                monthly_wage: d(WAGE),
                unused_leave_days: d(leave),
            },
            &seeded(),
            settings,
        )
        .unwrap()
    }

    /// (uang_pesangon, upmk) in millions at a 10M wage.
    fn items(b: &SettlementBreakdown) -> (Decimal, Decimal) {
        (b.uang_pesangon / d("1000000"), b.upmk / d("1000000"))
    }

    // ---- Anchor: the voluntary resignation that started this work ----------

    #[test]
    fn resignation_at_a_tenth_of_a_year_pays_nothing_but_the_uang_pisah_setting() {
        // Joined 2026-08-28, last day 2026-10-07: ~0.11 years, 0 completed.
        let b = settle(OffboardingReason::Resignation, "2026-08-28", "2026-10-07", "0", &SeveranceSettings::default());
        assert_eq!(b.service_years, 0);
        assert!(b.tenure_years > d("0.1") && b.tenure_years < d("0.2"), "tenure {}", b.tenure_years);
        assert_eq!(b.uang_pesangon, Decimal::ZERO, "Pasal 50: no pesangon");
        assert_eq!(b.upmk, Decimal::ZERO, "Pasal 50: no UPMK (and none below 3 years anyway)");
        assert_eq!(b.uang_pisah, Decimal::ZERO, "uang pisah defaults to 0 months");
        assert_eq!(b.severance_total, Decimal::ZERO);
        assert_eq!(b.legal_basis, "PP 35/2021 Pasal 50");

        // With the company's policy set to one month, the same leaver gets one month.
        let policy = SeveranceSettings { uang_pisah_months: d("1"), ..SeveranceSettings::default() };
        let b = settle(OffboardingReason::Resignation, "2026-08-28", "2026-10-07", "0", &policy);
        assert_eq!(b.uang_pisah, d(WAGE));
        assert_eq!(b.severance_total, d(WAGE));
    }

    // ---- Reason × tenure boundaries (10M wage, multiplier × Pasal 40 months) --

    #[test]
    fn scales_follow_pasal_40_at_every_boundary() {
        // (tenure, pesangon months, UPMK months) from the reference table.
        let cases = [
            ("0.9", "1", "0"),
            ("1", "2", "0"),
            ("2.99", "3", "0"),
            ("3", "4", "2"),
            ("6", "7", "3"),
            ("8", "9", "3"),
            ("24", "9", "10"),
        ];
        let p = seeded();
        for (t, pm, um) in cases {
            let y = completed_years(d(t));
            assert_eq!(p.pesangon_months(y), d(pm), "pesangon months at {t}y");
            assert_eq!(p.upmk_months(y), d(um), "UPMK months at {t}y");
        }
        // Bands just below each UPMK step.
        assert_eq!(p.upmk_months(completed_years(d("5.99"))), d("2"));
        assert_eq!(p.upmk_months(completed_years(d("8.99"))), d("3"));
        assert_eq!(p.upmk_months(completed_years(d("9"))), d("4"));
        assert_eq!(p.upmk_months(completed_years(d("23.99"))), d("8"));
        assert_eq!(p.pesangon_months(completed_years(d("7.99"))), d("8"));
    }

    #[test]
    fn reason_by_tenure_matrix_matches_hand_computed_values() {
        let s = SeveranceSettings::default();
        // (reason, tenure, uang pesangon M, UPMK M) at a 10M wage.
        let expect: &[(OffboardingReason, &str, &str, &str)] = &[
            // Pasal 50 resignation: nothing at any tenure.
            (OffboardingReason::Resignation, "0.9", "0", "0"),
            (OffboardingReason::Resignation, "3", "0", "0"),
            (OffboardingReason::Resignation, "24", "0", "0"),
            // Pasal 43(1) efficiency (losses): 0.5 × pesangon, 1 × UPMK.
            (OffboardingReason::EfficiencyLosses, "0.9", "5", "0"),
            (OffboardingReason::EfficiencyLosses, "1", "10", "0"),
            (OffboardingReason::EfficiencyLosses, "2.99", "15", "0"),
            (OffboardingReason::EfficiencyLosses, "3", "20", "20"),
            (OffboardingReason::EfficiencyLosses, "6", "35", "30"),
            (OffboardingReason::EfficiencyLosses, "8", "45", "30"),
            (OffboardingReason::EfficiencyLosses, "24", "45", "100"),
            // Pasal 43(2) efficiency (prevent losses): 1 × pesangon, 1 × UPMK.
            (OffboardingReason::EfficiencyPreventLosses, "0.9", "10", "0"),
            (OffboardingReason::EfficiencyPreventLosses, "1", "20", "0"),
            (OffboardingReason::EfficiencyPreventLosses, "2.99", "30", "0"),
            (OffboardingReason::EfficiencyPreventLosses, "3", "40", "20"),
            (OffboardingReason::EfficiencyPreventLosses, "6", "70", "30"),
            (OffboardingReason::EfficiencyPreventLosses, "8", "90", "30"),
            (OffboardingReason::EfficiencyPreventLosses, "24", "90", "100"),
            // Pasal 45(2) force majeure without closure: 0.75 × pesangon.
            (OffboardingReason::ForceMajeureNoClosure, "3", "30", "20"),
            (OffboardingReason::ForceMajeureNoClosure, "8", "67.5", "30"),
            // Pasal 52(1) after three warnings: 0.5 × pesangon, 1 × UPMK.
            (OffboardingReason::ViolationAfterWarnings, "6", "35", "30"),
            // Pasal 54(2) detained, no loss: UPMK only.
            (OffboardingReason::DetainedNoCompanyLoss, "2.99", "0", "0"),
            (OffboardingReason::DetainedNoCompanyLoss, "3", "0", "20"),
            // Pasal 55 prolonged illness: 2 × pesangon, 1 × UPMK.
            (OffboardingReason::ProlongedIllness, "8", "180", "30"),
            // Pasal 56 retirement: 1.75 × pesangon, 1 × UPMK.
            (OffboardingReason::Retirement, "0.9", "17.5", "0"),
            (OffboardingReason::Retirement, "24", "157.5", "100"),
            // Pasal 57 death: 2 × pesangon, 1 × UPMK.
            (OffboardingReason::Death, "6", "140", "30"),
            // Pasal 41 merger: 1 × / 1 ×.
            (OffboardingReason::MergerConsolidationSplit, "3", "40", "20"),
            // Pasal 42(2) takeover with changed terms: 0.5 × / 1 ×.
            (OffboardingReason::TakeoverChangedTerms, "3", "20", "20"),
            // Pasal 47 bankruptcy: 0.5 × / 1 ×.
            (OffboardingReason::Bankruptcy, "6", "35", "30"),
            // Pasal 52(2) urgent violation: nothing statutory.
            (OffboardingReason::UrgentViolation, "24", "0", "0"),
        ];
        for (reason, t, p, u) in expect {
            let b = calc_at(*reason, t, &s);
            assert_eq!(items(&b), (d(p), d(u)), "{reason} at {t} years");
        }
    }

    #[test]
    fn earlier_labels_compute_as_their_pp35_case() {
        let s = SeveranceSettings::default();
        for (legacy, case) in [
            (OffboardingReason::Efficiency, OffboardingReason::EfficiencyPreventLosses),
            (OffboardingReason::MergerAcquisition, OffboardingReason::MergerConsolidationSplit),
            (OffboardingReason::ForceMajeure, OffboardingReason::ForceMajeureNoClosure),
            (OffboardingReason::Misconduct, OffboardingReason::UrgentViolation),
        ] {
            for t in ["0.9", "3", "8", "24"] {
                assert_eq!(items(&calc_at(legacy, t, &s)), items(&calc_at(case, t, &s)), "{legacy} at {t}");
            }
        }
        // The legacy `efficiency` at 3 years: 4 months pesangon + 2 months UPMK, no 15%.
        let b = settle(OffboardingReason::Efficiency, "2023-10-03", "2026-10-02", "0", &s);
        assert_eq!(b.service_years, 3);
        assert_eq!(b.uang_pesangon, d("40000000"));
        assert_eq!(b.upmk, d("20000000"));
        assert_eq!(b.severance_total, d("60000000"));
        assert_eq!(b.legal_basis, "PP 35/2021 Pasal 43(2)");
        assert_eq!(b.severance_case.as_deref(), Some("efficiency_prevent_losses"));
    }

    #[test]
    fn unspecific_termination_is_refused() {
        let err = compute_settlement(
            &SettlementInputs {
                reason: OffboardingReason::Termination,
                join_date: date("2020-01-01"),
                last_working_day: date("2026-10-02"),
                monthly_wage: d(WAGE),
                unused_leave_days: Decimal::ZERO,
            },
            &seeded(),
            &SeveranceSettings::default(),
        )
        .expect_err("termination names no case");
        assert!(matches!(err, PesangonError::UnspecificReason(_)), "{err:?}");
        assert_eq!(err.code(), "unspecific_offboarding_reason");
        assert_eq!(err.http_status(), 422);
    }

    #[test]
    fn fixed_term_end_pays_only_the_leave_payout() {
        let policy = SeveranceSettings { uang_pisah_months: d("1"), ..SeveranceSettings::default() };
        let b = settle(OffboardingReason::EndOfContract, "2020-01-01", "2026-10-02", "4", &policy);
        assert_eq!(b.uang_pesangon, Decimal::ZERO);
        assert_eq!(b.upmk, Decimal::ZERO);
        assert_eq!(b.uang_pisah, Decimal::ZERO, "a contract end earns no uang pisah");
        assert_eq!(b.unused_leave_payout, d("1904761.92"), "4 × round(10M / 21) = 4 × 476,190.48");
        assert_eq!(b.net_payable, d("1904761.92"));
        assert_eq!(b.severance_case, None);
        assert_eq!(b.legal_basis, FIXED_TERM_END_BASIS);
    }

    #[test]
    fn uang_pisah_only_for_the_cases_that_earn_it() {
        let policy = SeveranceSettings { uang_pisah_months: d("1.5"), ..SeveranceSettings::default() };
        for reason in [
            OffboardingReason::Resignation,
            OffboardingReason::WorkerRequestRejected,
            OffboardingReason::AbsenceWithoutNotice,
            OffboardingReason::UrgentViolation,
            OffboardingReason::DetainedCompanyLoss,
            OffboardingReason::ConvictedCompanyLoss,
            OffboardingReason::Misconduct,
        ] {
            assert_eq!(calc_at(reason, "4", &policy).uang_pisah, d("15000000"), "{reason} earns uang pisah");
        }
        for reason in [
            OffboardingReason::EfficiencyPreventLosses,
            OffboardingReason::Retirement,
            OffboardingReason::DetainedNoCompanyLoss,
            OffboardingReason::ViolationAfterWarnings,
        ] {
            assert_eq!(calc_at(reason, "4", &policy).uang_pisah, Decimal::ZERO, "{reason} earns no uang pisah");
        }
    }

    #[test]
    fn every_reason_maps_to_a_seeded_case_or_a_documented_exception() {
        use OffboardingReason as R;
        let params = seeded();
        let all = [
            R::Resignation, R::MergerConsolidationSplit, R::Takeover, R::TakeoverChangedTerms,
            R::EfficiencyLosses, R::EfficiencyPreventLosses, R::ClosureLosses, R::ClosureNotLosses,
            R::ClosureForceMajeure, R::ForceMajeureNoClosure, R::PkpuLosses, R::PkpuNotLosses,
            R::Bankruptcy, R::EmployerViolationRequest, R::WorkerRequestRejected,
            R::AbsenceWithoutNotice, R::ViolationAfterWarnings, R::UrgentViolation,
            R::DetainedCompanyLoss, R::DetainedNoCompanyLoss, R::ConvictedCompanyLoss,
            R::ConvictedNoCompanyLoss, R::ProlongedIllness, R::Retirement, R::Death,
            R::EndOfContract, R::Termination, R::MergerAcquisition, R::Efficiency,
            R::ForceMajeure, R::Misconduct,
        ];
        for r in all {
            match severance_basis(r) {
                Ok(SeveranceBasis::Case(c)) => assert!(params.reasons.contains_key(c), "{r} → {c} not seeded"),
                Ok(SeveranceBasis::FixedTermEnd) => assert_eq!(r, R::EndOfContract),
                Err(_) => assert_eq!(r, R::Termination),
            }
        }
        // A new statutory value maps to itself.
        assert_eq!(severance_basis(R::ClosureLosses).unwrap(), SeveranceBasis::Case("closure_losses"));
    }

    // ---- Statutory set completeness ------------------------------------------

    #[test]
    fn an_incomplete_set_is_refused() {
        let as_of = date("2026-10-03");
        let mut missing_case = seeded();
        missing_case.reasons.remove("death");
        let err = missing_case.check(as_of).expect_err("a case is missing");
        assert!(matches!(err, PesangonError::IncompleteStatutoryParams { .. }), "{err:?}");

        let mut no_zero_band = seeded();
        no_zero_band.upmk_scale.remove(0);
        assert!(no_zero_band.check(as_of).is_err(), "a scale must open at zero years");

        let mut unordered = seeded();
        unordered.pesangon_scale.swap(1, 2);
        assert!(unordered.check(as_of).is_err(), "bands must ascend");

        let mut extra_case = seeded();
        extra_case.reasons.insert(
            "some_future_case".into(),
            ReasonParams { pesangon_multiplier: d("1"), upmk_multiplier: d("1"), uang_pisah_eligible: false, article: "x".into() },
        );
        assert!(extra_case.check(as_of).is_ok(), "an extra case from a newer set is tolerated");
    }

    // ---- Years of service ----------------------------------------------------

    #[test]
    fn tenure_counts_anniversaries_through_the_last_day() {
        // The day before the third anniversary is the last day of year three.
        assert_eq!(completed_years(tenure_years(date("2023-10-03"), date("2026-10-02"))), 3);
        assert_eq!(tenure_years(date("2023-10-03"), date("2026-10-02")), d("3"));
        // One day earlier, still two completed years.
        assert_eq!(completed_years(tenure_years(date("2023-10-03"), date("2026-10-01"))), 2);
        // An anniversary with no leap day in between is still a full year
        // (a 365.25-day year would have counted 2.998).
        assert_eq!(tenure_years(date("2024-03-01"), date("2027-02-28")), d("3"));
        // A 29 February start completes its year on 28 February (inclusive).
        assert_eq!(completed_years(tenure_years(date("2024-02-29"), date("2025-02-28"))), 1);
        assert_eq!(completed_years(tenure_years(date("2024-02-29"), date("2025-02-27"))), 0);
        // Same-day join and exit: a fraction of a year, zero completed.
        let one_day = tenure_years(date("2026-10-01"), date("2026-10-01"));
        assert!(one_day > Decimal::ZERO && one_day < d("0.01"), "{one_day}");
        // Never started: zero.
        assert_eq!(tenure_years(date("2026-11-01"), date("2026-10-31")), Decimal::ZERO);
        // The fraction never rounds up into the next band.
        let almost = tenure_years(date("2023-10-03"), date("2026-09-30"));
        assert!(almost < d("3"), "{almost}");
    }

    // ---- Last pay and the daily wage ----------------------------------------

    #[test]
    fn daily_wage_follows_the_work_week() {
        assert_eq!(daily_wage(d("10500000"), WorkWeek::FiveDay), d("500000"));
        assert_eq!(daily_wage(d("10000000"), WorkWeek::SixDay), d("400000"));
        // The leave payout uses the same daily wage.
        let six = SeveranceSettings { work_week: WorkWeek::SixDay, ..SeveranceSettings::default() };
        let b = settle(OffboardingReason::Resignation, "2020-01-01", "2026-10-02", "5", &six);
        assert_eq!(b.daily_wage, d("400000"));
        assert_eq!(b.unused_leave_payout, d("2000000"));
        assert_eq!(b.work_days_per_week, 6);
        let five = settle(OffboardingReason::Resignation, "2020-01-01", "2026-10-02", "5", &SeveranceSettings::default());
        assert_eq!(five.daily_wage, d("476190.48"));
        assert_eq!(five.unused_leave_payout, d("2380952.40"));
        assert_eq!(five.work_days_per_week, 5);
    }

    #[test]
    fn last_pay_prorates_working_days_from_the_month_start() {
        // October 2026: the 1st is a Thursday. 1-7 Oct holds Thu, Fri, Mon, Tue, Wed
        // = 5 five-day working days; with Saturday 6 six-day working days.
        let five = last_pay(date("2020-01-01"), date("2026-10-07"), d("10500000"), WorkWeek::FiveDay);
        assert_eq!(five.from, Some(date("2026-10-01")));
        assert_eq!(five.working_days, 5);
        assert_eq!(five.amount, d("2500000"), "5 × 500,000");
        let six = last_pay(date("2020-01-01"), date("2026-10-07"), d("10000000"), WorkWeek::SixDay);
        assert_eq!(six.working_days, 6);
        assert_eq!(six.amount, d("2400000"), "6 × 400,000");
    }

    #[test]
    fn last_pay_starts_at_a_join_date_inside_the_month() {
        // Joined Monday 2026-10-12, last day Friday 2026-10-16: 5 working days.
        let p = last_pay(date("2026-10-12"), date("2026-10-16"), d("10500000"), WorkWeek::FiveDay);
        assert_eq!(p.from, Some(date("2026-10-12")));
        assert_eq!(p.working_days, 5);
        assert_eq!(p.amount, d("2500000"));
        // A calendar-day proration would have paid 16/31 of the month from the 1st.
    }

    #[test]
    fn last_pay_is_zero_when_the_employee_never_started() {
        let p = last_pay(date("2026-11-02"), date("2026-10-30"), d("10500000"), WorkWeek::FiveDay);
        assert_eq!(p, LastPay { amount: Decimal::ZERO, from: None, working_days: 0 });
        // And the whole settlement is zero, whatever the reason or leave balance.
        let policy = SeveranceSettings { uang_pisah_months: d("1"), ..SeveranceSettings::default() };
        for reason in [OffboardingReason::Resignation, OffboardingReason::EfficiencyPreventLosses, OffboardingReason::Death] {
            let b = settle(reason, "2026-11-02", "2026-10-30", "12", &policy);
            assert!(!b.started);
            assert_eq!(
                (b.uang_pesangon, b.upmk, b.uang_pisah, b.unused_leave_payout, b.last_pay, b.net_payable),
                (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                "{reason}"
            );
            assert_eq!(b.tenure_years, Decimal::ZERO);
        }
    }

    #[test]
    fn a_full_month_pays_the_full_wage_and_a_part_month_never_more() {
        let full = last_pay(date("2020-01-01"), date("2026-10-31"), d("10000000"), WorkWeek::FiveDay);
        assert_eq!(full.amount, d("10000000"));
        // 1-30 October: 22 weekdays × 476,190.48 = 10,476,190.56 → capped at the month.
        let almost = last_pay(date("2020-01-01"), date("2026-10-30"), d("10000000"), WorkWeek::FiveDay);
        assert_eq!(almost.working_days, 22);
        assert_eq!(almost.amount, d("10000000"));
    }

    #[test]
    fn net_payable_excludes_the_last_pay_and_the_payload_keeps_earlier_names() {
        let policy = SeveranceSettings { uang_pisah_months: d("1"), ..SeveranceSettings::default() };
        let b = settle(OffboardingReason::Resignation, "2020-01-01", "2026-10-07", "2", &policy);
        // uang pisah 10M + leave 2 × 476,190.48.
        assert_eq!(b.net_payable, d("10952380.96"));
        assert!(b.last_pay > Decimal::ZERO && b.last_pay_via_payroll);
        let v = b.to_event_payload();
        assert_eq!(v["total"], serde_json::json!(b.net_payable));
        assert_eq!(v["pesangon"], serde_json::json!(b.uang_pesangon));
        assert_eq!(v["upm"], serde_json::json!(Decimal::ZERO));
        assert_eq!(v["uang_pisah"], serde_json::json!(b.uang_pisah));
        assert_eq!(v["unused_leave_payout"], serde_json::json!(b.unused_leave_payout));
        assert_eq!(v["last_pay_via_payroll"], serde_json::json!(true));
    }
}
