//! The contract expiry reminder pass and its start point.
//!
//! Provisions its own scratch database on the server named by `DATABASE_URL`
//! (dropped and recreated per run) and applies the module's contract table
//! migration. SKIPS when no server is reachable; set `LIFECYCLE_REQUIRE_DB=1`
//! to turn a skip into a failure.

use backbone_lifecycle::application::service::ContractWriteService;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use sqlx::PgPool;
use uuid::Uuid;

async fn scratch() -> Option<PgPool> {
    let required = std::env::var("LIFECYCLE_REQUIRE_DB").map(|v| v != "0").unwrap_or(false);
    let Some(url) = std::env::var("DATABASE_URL").ok() else {
        assert!(!required, "DATABASE_URL is required");
        eprintln!("skip: DATABASE_URL is not set");
        return None;
    };
    let (prefix, _) = url.trim_end_matches('/').rsplit_once('/')?;
    let admin = match PgPool::connect(&format!("{prefix}/postgres")).await {
        Ok(p) => p,
        Err(e) => {
            assert!(!required, "no admin connection ({e})");
            eprintln!("skip: no admin connection ({e})");
            return None;
        }
    };
    let name = "lifecycle_contract_reminder_start";
    let _ = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#))
        .execute(&admin)
        .await;
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#))
        .execute(&admin)
        .await
        .expect("create scratch database");
    admin.close().await;
    let pool = PgPool::connect(&format!("{prefix}/{name}")).await.expect("connect scratch");
    sqlx::raw_sql(include_str!("../migrations/20260925130002_create_contract_table.up.sql"))
        .execute(&pool)
        .await
        .expect("apply contract table");
    Some(pool)
}

async fn seed_pkwt(pool: &PgPool, end_date: NaiveDate) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO lifecycle.contracts
               (id, employment_id, employee_id, contract_type, start_date, end_date, status)
           VALUES ($1, $2, $3, 'pkwt', $4, $5, 'active')"#,
    )
    .bind(id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(end_date - Duration::days(365))
    .bind(end_date)
    .execute(pool)
    .await
    .unwrap();
    id
}

/// With a 30-day window and a start point, a contract that already ended
/// before the start day is not reminded; every contract still running is
/// reminded once, including one whose window opened before the start day.
#[tokio::test]
async fn only_contracts_ended_before_the_start_point_go_unreminded() {
    let Some(pool) = scratch().await else { return };
    let svc = ContractWriteService::new(pool.clone());

    let start = Utc.with_ymd_and_hms(2026, 10, 4, 6, 0, 0).unwrap();
    let start_day = start.date_naive();
    let now = start;

    // Ended three days before the start, still active on record.
    let ended = seed_pkwt(&pool, start_day - Duration::days(3)).await;
    // Window opened 20 days before the start; ends 10 days after it.
    let window_opened_earlier = seed_pkwt(&pool, start_day + Duration::days(10)).await;
    // Ends on the start day itself: not yet ended.
    let ends_today = seed_pkwt(&pool, start_day).await;
    // Window opens on the start day.
    let due_today = seed_pkwt(&pool, start_day + Duration::days(30)).await;
    // Outside the window.
    let later = seed_pkwt(&pool, start_day + Duration::days(90)).await;

    let mut reminded = svc.remind_due_from(now, 30, Some(start)).await.expect("reminder pass");
    reminded.sort();
    let mut expected = vec![window_opened_earlier, ends_today, due_today];
    expected.sort();
    assert_eq!(reminded, expected);

    // Exactly once: the watermark keeps the next pass silent.
    assert!(svc.remind_due_from(now, 30, Some(start)).await.unwrap().is_empty());
    let stamped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM lifecycle.contracts WHERE reminder_sent_at IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stamped, 3);

    // Without a start point the ended contract is reminded too, as before —
    // the stale notice the start point exists to avoid.
    assert_eq!(svc.remind_due(now, 30).await.unwrap(), vec![ended]);
    let _ = later;
}
