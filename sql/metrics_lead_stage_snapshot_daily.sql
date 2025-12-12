-- ============================================================
-- Phase 6A — Lead Stage Snapshot Daily (Schema-Validated)
-- Derived stages:
--   Unengaged: no activity ever
--   Engaged:   has activity, no opp
--   Converted: has opp (any)
-- ============================================================

with params as (
  select (:'run_date')::date as run_date
),

first_touch as (
  select
    l.lead_id,
    min(a.activity_ts) as first_touch_at
  from crm_remote.leads l
  left join crm_remote.activities a
    on a.lead_id = l.lead_id
  group by 1
),

has_opp as (
  select distinct
    o.lead_id
  from crm_remote.opportunities o
  where o.lead_id is not null
)

insert into metrics.lead_stage_snapshot_daily (
  metric_date,
  lead_stage,
  leads_in_stage,
  updated_at
)
select
  p.run_date,
  stage,
  count(*) as leads_in_stage,
  now()
from (
  select
    l.lead_id,
    case
      when ft.first_touch_at is null then 'Unengaged'
      when ho.lead_id is null then 'Engaged'
      else 'Converted to Opportunity'
    end as stage
  from crm_remote.leads l
  left join first_touch ft
    on ft.lead_id = l.lead_id
  left join has_opp ho
    on ho.lead_id = l.lead_id
) s
cross join params p
group by p.run_date, stage
on conflict (metric_date, lead_stage) do update set
  leads_in_stage = excluded.leads_in_stage,
  updated_at = now();
