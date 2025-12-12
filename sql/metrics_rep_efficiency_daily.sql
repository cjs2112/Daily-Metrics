-- ============================================================
-- Phase 6A — Rep Efficiency Daily (Schema-Validated)
-- Notes:
--   - Opportunities have no rep_id -> attribute opps to rep via leads.lead_id join
--   - status is blank -> won derived from stage_name
--   - opp event proxy uses close_date
-- ============================================================

with params as (
  select (:'run_date')::date as run_date
),

-- touches by rep from activities on run_date
touches_by_rep as (
  select
    a.rep_id,
    count(*) as touches
  from crm_remote.activities a
  join params p on a.activity_ts::date = p.run_date
  group by 1
),

-- leads created by rep on run_date
leads_created_by_rep as (
  select
    l.rep_id,
    count(*) as leads_created
  from crm_remote.leads l
  join params p on l.created_at::date = p.run_date
  group by 1
),

-- opportunities on run_date attributed via lead -> rep
opps_by_rep as (
  select
    l.rep_id,
    count(*) as opps_created,
    coalesce(sum(o.amount), 0) as pipeline_created
  from crm_remote.opportunities o
  join crm_remote.leads l
    on l.lead_id = o.lead_id
  join params p
    on o.close_date::date = p.run_date
  group by 1
),

-- won revenue on run_date attributed via lead -> rep
won_by_rep as (
  select
    l.rep_id,
    coalesce(sum(o.amount), 0) as revenue_won
  from crm_remote.opportunities o
  join crm_remote.leads l
    on l.lead_id = o.lead_id
  join params p
    on o.close_date::date = p.run_date
  where o.stage_name ilike '%won%'
  group by 1
)

insert into metrics.rep_efficiency_daily (
  metric_date,
  rep_id,
  touches,
  leads_created,
  opps_created,
  pipeline_created,
  revenue_won,
  touches_per_opp,
  updated_at
)
select
  p.run_date,
  r.rep_id,

  coalesce(t.touches, 0) as touches,
  coalesce(lc.leads_created, 0) as leads_created,
  coalesce(o.opps_created, 0) as opps_created,
  coalesce(o.pipeline_created, 0) as pipeline_created,
  coalesce(w.revenue_won, 0) as revenue_won,

  case
    when coalesce(o.opps_created, 0) > 0
    then coalesce(t.touches, 0)::numeric / o.opps_created
  end as touches_per_opp,

  now() as updated_at

from crm_remote.reps r
cross join params p
left join touches_by_rep t on t.rep_id = r.rep_id
left join leads_created_by_rep lc on lc.rep_id = r.rep_id
left join opps_by_rep o on o.rep_id = r.rep_id
left join won_by_rep w on w.rep_id = r.rep_id

on conflict (metric_date, rep_id) do update set
  touches = excluded.touches,
  leads_created = excluded.leads_created,
  opps_created = excluded.opps_created,
  pipeline_created = excluded.pipeline_created,
  revenue_won = excluded.revenue_won,
  touches_per_opp = excluded.touches_per_opp,
  updated_at = now();
