-- ============================================================
-- Phase 6A — Executive Funnel Daily Metrics (Schema-Validated)
-- Inputs:
--   psql -v run_date="YYYY-MM-DD"
-- Notes:
--   - Opportunities have NO rep_id / created_at / is_won / is_closed
--   - status is blank -> outcomes derived from stage_name
--   - "opps_created" uses close_date as event proxy (since no created_at)
-- ============================================================

with params as (
  select (:'run_date')::date as run_date
),

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
    activity_ts
  from crm_remote.activities
),

opps as (
  select
    opp_id,
    lead_id,
    amount,
    close_date,
    stage,
    stage_name
  from crm_remote.opportunities
),

first_touch as (
  select
    l.lead_id,
    min(a.activity_ts) as first_touch_at
  from leads l
  left join activities a
    on a.lead_id = l.lead_id
  group by 1
)

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
  p.run_date,

  -- Leads created (true creation event)
  (select count(*)
   from leads l
   where l.created_at::date = p.run_date
  ) as leads_created,

  -- Leads engaged (first activity date = run_date)
  (select count(*)
   from first_touch ft
   where ft.first_touch_at::date = p.run_date
  ) as leads_engaged,

  -- "Opps created" proxy: opportunities with close_date on run_date
  (select count(*)
   from opps o
   where o.close_date::date = p.run_date
  ) as opps_created,

  -- Outcomes derived from stage_name (status is blank)
  (select count(*)
   from opps o
   where o.close_date::date = p.run_date
     and o.stage_name ilike '%won%'
  ) as opps_won,

  (select count(*)
   from opps o
   where o.close_date::date = p.run_date
     and o.stage_name ilike '%lost%'
  ) as opps_lost,

  -- Pipeline created proxy: sum(amount) on close_date
  (select coalesce(sum(o.amount), 0)
   from opps o
   where o.close_date::date = p.run_date
  ) as pipeline_created,

  -- Revenue won proxy: sum(amount) where stage_name indicates won
  (select coalesce(sum(o.amount), 0)
   from opps o
   where o.close_date::date = p.run_date
     and o.stage_name ilike '%won%'
  ) as revenue_won,

  -- Conversion (lead->opp): opps on run_date / leads created on run_date
  case
    when (select count(*) from leads l where l.created_at::date = p.run_date) > 0
    then
      (select count(*) from opps o where o.close_date::date = p.run_date)::numeric
      /
      (select count(*) from leads l where l.created_at::date = p.run_date)
  end as lead_to_opp_conv_pct,

  -- Avg days to first touch for leads created on run_date
  (select avg(extract(epoch from (ft.first_touch_at - l.created_at)) / 86400.0)
   from leads l
   join first_touch ft on ft.lead_id = l.lead_id
   where l.created_at::date = p.run_date
     and ft.first_touch_at is not null
  ) as avg_days_to_first_touch,

  -- Avg days lead -> "opp event" proxy (close_date) for opps on run_date
  (select avg(extract(epoch from (o.close_date - l.created_at)) / 86400.0)
   from opps o
   join leads l on l.lead_id = o.lead_id
   where o.close_date::date = p.run_date
     and l.created_at is not null
     and o.close_date is not null
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
