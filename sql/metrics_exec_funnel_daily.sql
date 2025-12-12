-- DEBUG: prove execution
insert into metrics.system_log (level, message)
values ('INFO', 'metrics_exec_funnel_daily.sql executed');

-- ============================================================
-- Phase 6A — Executive Funnel Daily Metrics
-- Event-derived, date-driven, idempotent
-- Guarantees exactly 1 row per run_date
-- ============================================================

with params as (
  select current_setting('run_date')::date
),

-- ------------------------------------------------------------
-- Source tables
-- ------------------------------------------------------------
leads as (
  select
    lead_id,
    rep_id,
    created_at
  from crm_remote.leads
),

activities as (
  select
    lead_id,
    occurred_at
  from crm_remote.activities
),

opps as (
  select
    opportunity_id,
    rep_id,
    amount,
    created_at,
    is_closed,
    is_won,
    closed_at
  from crm_remote.opportunities
),

-- ------------------------------------------------------------
-- First touch per lead (event-derived engagement)
-- ------------------------------------------------------------
first_touch as (
  select
    l.lead_id,
    min(a.occurred_at) as first_touch_at
  from leads l
  left join activities a
    on a.lead_id = l.lead_id
  group by 1
)

-- ============================================================
-- Insert / Upsert (date-driven, scalar subqueries only)
-- ============================================================
insert into metrics.exec_funnel_daily (
  metric_date,
  leads_created,
  leads_engaged,
  opps_created,
  opps_won,
  opps_lost,
  pipeline_created,
  revenue_won,
  lead_to_opp_conv_pct,
  avg_days_to_first_touch,
  avg_days_lead_to_opp,
  updated_at
)
select
  p.run_date as metric_date,

  -- -------------------------
  -- Funnel volume
  -- -------------------------
  (select count(*)
   from leads l
   where l.created_at::date = p.run_date
  ) as leads_created,

  (select count(*)
   from first_touch ft
   where ft.first_touch_at::date = p.run_date
  ) as leads_engaged,

  (select count(*)
   from opps o
   where o.created_at::date = p.run_date
  ) as opps_created,

  -- -------------------------
  -- Outcomes
  -- -------------------------
  (select count(*)
   from opps o
   where o.is_closed = true
     and o.is_won = true
     and o.closed_at::date = p.run_date
  ) as opps_won,

  (select count(*)
   from opps o
   where o.is_closed = true
     and o.is_won = false
     and o.closed_at::date = p.run_date
  ) as opps_lost,

  (select coalesce(sum(o.amount), 0)
   from opps o
   where o.created_at::date = p.run_date
  ) as pipeline_created,

  (select coalesce(sum(o.amount), 0)
   from opps o
   where o.is_closed = true
     and o.is_won = true
     and o.closed_at::date = p.run_date
  ) as revenue_won,

  -- -------------------------
  -- Conversion
  -- -------------------------
  case
    when (select count(*)
          from leads l
          where l.created_at::date = p.run_date) > 0
    then
      (select count(*)
       from opps o
       where o.created_at::date = p.run_date)::numeric
      /
      (select count(*)
       from leads l
       where l.created_at::date = p.run_date)
  end as lead_to_opp_conv_pct,

  -- -------------------------
  -- Velocity (event-derived)
  -- -------------------------
  (select avg(
            extract(epoch from (ft.first_touch_at - l.created_at)) / 86400.0
          )
   from leads l
   join first_touch ft
     on ft.lead_id = l.lead_id
   where l.created_at::date = p.run_date
     and ft.first_touch_at is not null
  ) as avg_days_to_first_touch,

  (select avg(
            extract(epoch from (o.created_at - l.created_at)) / 86400.0
          )
   from leads l
   join opps o
     on o.rep_id = l.rep_id
    and o.created_at > l.created_at
   where l.created_at::date = p.run_date
  ) as avg_days_lead_to_opp,

  now() as updated_at

from params p
on conflict (metric_date) do update set
  leads_created = excluded.leads_created,
  leads_engaged = excluded.leads_engaged,
  opps_created = excluded.opps_created,
  opps_won = excluded.opps_won,
  opps_lost = excluded.opps_lost,
  pipeline_created = excluded.pipeline_created,
  revenue_won = excluded.revenue_won,
  lead_to_opp_conv_pct = excluded.lead_to_opp_conv_pct,
  avg_days_to_first_touch = excluded.avg_days_to_first_touch,
  avg_days_lead_to_opp = excluded.avg_days_lead_to_opp,
  updated_at = now();
