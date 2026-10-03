//! Reads for the final-settlement calculation: the statutory severance set in
//! force on a day, and the company's settlement settings.
//!
//! ## Statutory set (`lifecycle.severance_*`)
//!
//! For each of the three tables, the row set with the greatest
//! `effective_from <= as_of` applies, as a whole (a correction is a new
//! complete set, never an edit of live rows). A table with no set in force, or
//! a set that fails [`SeveranceParams::check`], refuses: the settlement is not
//! computed rather than computed from a partial law. There is no fallback to
//! numbers compiled into the code.
//!
//! The tables are global national-law data — no company column and no row
//! security — so the reads carry no scope.
//!
//! ## Settings (`platform.sysparams`, group `lifecycle.offboarding`)
//!
//! | key | meaning | default when absent |
//! |---|---|---|
//! | `uang_pisah.months` | uang pisah in months of wage, for the cases that earn it | `0` |
//! | `daily_wage.work_days_per_week` | `5` (daily wage = monthly / 21) or `6` (/ 25) | `5` |
//! | `leave_payout.timeoff_type_codes` | comma-separated leave-type codes paid out | `ANNUAL` |
//!
//! `platform.sysparams` belongs to the composing platform; a database without
//! it (a standalone module, a test) reads the defaults. A value that is present
//! but unusable refuses the settlement with the setting named, so a typo can
//! never silently pay the default.

use crate::application::service::pesangon::{
    PesangonError, ReasonParams, SeveranceParams, SeveranceSettings, TenureBand, WorkWeek,
};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgConnection;
use std::collections::HashMap;
use std::str::FromStr;

/// The settings group the settlement reads.
pub const SETTINGS_GROUP: &str = "lifecycle.offboarding";
/// Uang pisah, in months of the monthly wage.
pub const KEY_UANG_PISAH_MONTHS: &str = "uang_pisah.months";
/// Work days per week behind the daily wage (5 or 6).
pub const KEY_WORK_DAYS_PER_WEEK: &str = "daily_wage.work_days_per_week";
/// Leave-type codes whose unused balance is paid out.
pub const KEY_LEAVE_TYPE_CODES: &str = "leave_payout.timeoff_type_codes";

/// Resolve the statutory severance set in force for `country` on `as_of`.
pub async fn severance_params_as_of(
    conn: &mut PgConnection,
    country: &str,
    as_of: NaiveDate,
) -> Result<SeveranceParams, PesangonError> {
    let (pesangon_scale, pesangon_effective_from) =
        scale_as_of(conn, "lifecycle.severance_pesangon_scale", country, as_of).await?;
    let (upmk_scale, upmk_effective_from) =
        scale_as_of(conn, "lifecycle.severance_upmk_scale", country, as_of).await?;

    let reasons_effective_from =
        effective_at(conn, "lifecycle.severance_reason_params", country, as_of).await?;
    let rows: Vec<(String, Decimal, Decimal, bool, String)> = sqlx::query_as(
        r#"SELECT reason_code, pesangon_multiplier, upmk_multiplier, uang_pisah_eligible, article
             FROM lifecycle.severance_reason_params
            WHERE country_code = $1 AND effective_from = $2"#,
    )
    .bind(country)
    .bind(reasons_effective_from)
    .fetch_all(&mut *conn)
    .await?;
    let reasons: HashMap<String, ReasonParams> = rows
        .into_iter()
        .map(|(code, pesangon_multiplier, upmk_multiplier, uang_pisah_eligible, article)| {
            (
                code,
                ReasonParams { pesangon_multiplier, upmk_multiplier, uang_pisah_eligible, article },
            )
        })
        .collect();

    SeveranceParams {
        pesangon_scale,
        pesangon_effective_from,
        upmk_scale,
        upmk_effective_from,
        reasons,
        reasons_effective_from,
    }
    .check(as_of)
}

/// The `effective_from` of the set in force, or the fail-closed error.
async fn effective_at(
    conn: &mut PgConnection,
    table: &'static str,
    country: &str,
    as_of: NaiveDate,
) -> Result<NaiveDate, PesangonError> {
    let effective: Option<NaiveDate> = sqlx::query_scalar(&format!(
        "SELECT MAX(effective_from) FROM {table} WHERE country_code = $1 AND effective_from <= $2"
    ))
    .bind(country)
    .bind(as_of)
    .fetch_one(&mut *conn)
    .await?;
    effective.ok_or_else(|| PesangonError::NoStatutoryParams {
        country: country.to_string(),
        as_of,
        table,
    })
}

async fn scale_as_of(
    conn: &mut PgConnection,
    table: &'static str,
    country: &str,
    as_of: NaiveDate,
) -> Result<(Vec<TenureBand>, NaiveDate), PesangonError> {
    let effective = effective_at(conn, table, country, as_of).await?;
    let rows: Vec<(i32, Decimal)> = sqlx::query_as(&format!(
        "SELECT min_service_years, months FROM {table}
          WHERE country_code = $1 AND effective_from = $2
          ORDER BY min_service_years"
    ))
    .bind(country)
    .bind(effective)
    .fetch_all(&mut *conn)
    .await?;
    let bands = rows
        .into_iter()
        .map(|(years, months)| TenureBand {
            // The column CHECK keeps it non-negative.
            min_service_years: u32::try_from(years).unwrap_or(0),
            months,
        })
        .collect();
    Ok((bands, effective))
}

/// Read the company's settlement settings (defaults when unset).
pub async fn severance_settings(conn: &mut PgConnection) -> Result<SeveranceSettings, PesangonError> {
    let has_store: bool = sqlx::query_scalar("SELECT to_regclass('platform.sysparams') IS NOT NULL")
        .fetch_one(&mut *conn)
        .await?;
    let mut settings = SeveranceSettings::default();
    if !has_store {
        return Ok(settings);
    }
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"SELECT key, value FROM platform.sysparams
            WHERE group_name = $1 AND status::text = 'active'"#,
    )
    .bind(SETTINGS_GROUP)
    .fetch_all(&mut *conn)
    .await?;
    for (key, value) in rows {
        apply_setting(&mut settings, &key, &value)?;
    }
    Ok(settings)
}

/// Apply one stored setting; unknown keys in the group are ignored.
pub fn apply_setting(settings: &mut SeveranceSettings, key: &str, value: &str) -> Result<(), PesangonError> {
    let v = value.trim();
    match key {
        KEY_UANG_PISAH_MONTHS => {
            let months = Decimal::from_str(v).map_err(|_| invalid(KEY_UANG_PISAH_MONTHS, value, "not a number"))?;
            if months < Decimal::ZERO {
                return Err(invalid(KEY_UANG_PISAH_MONTHS, value, "must not be negative"));
            }
            settings.uang_pisah_months = months;
        }
        KEY_WORK_DAYS_PER_WEEK => {
            settings.work_week = v
                .parse::<u8>()
                .ok()
                .and_then(WorkWeek::from_days)
                .ok_or_else(|| invalid(KEY_WORK_DAYS_PER_WEEK, value, "must be 5 or 6"))?;
        }
        KEY_LEAVE_TYPE_CODES => {
            let codes: Vec<String> = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if codes.is_empty() {
                return Err(invalid(KEY_LEAVE_TYPE_CODES, value, "names no leave type"));
            }
            settings.leave_type_codes = codes;
        }
        _ => {}
    }
    Ok(())
}

fn invalid(key: &'static str, value: &str, detail: &'static str) -> PesangonError {
    PesangonError::InvalidSetting { group: SETTINGS_GROUP, key, value: value.to_string(), detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_parse_and_refuse_unusable_values() {
        let mut s = SeveranceSettings::default();
        apply_setting(&mut s, KEY_UANG_PISAH_MONTHS, " 1.5 ").unwrap();
        apply_setting(&mut s, KEY_WORK_DAYS_PER_WEEK, "6").unwrap();
        apply_setting(&mut s, KEY_LEAVE_TYPE_CODES, "ANNUAL, LONG_SERVICE").unwrap();
        apply_setting(&mut s, "some.other_key", "anything").unwrap();
        assert_eq!(s.uang_pisah_months, Decimal::new(15, 1));
        assert_eq!(s.work_week, WorkWeek::SixDay);
        assert_eq!(s.leave_type_codes, vec!["ANNUAL".to_string(), "LONG_SERVICE".to_string()]);

        for (key, value) in [
            (KEY_UANG_PISAH_MONTHS, "one"),
            (KEY_UANG_PISAH_MONTHS, "-1"),
            (KEY_WORK_DAYS_PER_WEEK, "7"),
            (KEY_WORK_DAYS_PER_WEEK, "five"),
            (KEY_LEAVE_TYPE_CODES, " , "),
        ] {
            let err = apply_setting(&mut SeveranceSettings::default(), key, value)
                .expect_err("an unusable value refuses");
            assert_eq!(err.code(), "invalid_offboarding_setting", "{key}={value}");
        }
    }

    #[test]
    fn defaults_are_zero_uang_pisah_a_five_day_week_and_annual_leave() {
        let s = SeveranceSettings::default();
        assert_eq!(s.uang_pisah_months, Decimal::ZERO);
        assert_eq!(s.work_week, WorkWeek::FiveDay);
        assert_eq!(s.leave_type_codes, vec!["ANNUAL".to_string()]);
    }
}
