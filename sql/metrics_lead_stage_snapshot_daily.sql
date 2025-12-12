-- Phase 6A — Lead Stage Snapshot Daily (psql variable driven)

with params as (
  select (:'run_date')::date as run_date
),

first_touch as (
  select lead_id, min(occurred_at) as first_touch_at
  from crm_remote.activities
  group by 1
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
      when o.opp_id is null then 'Engaged'
      else 'Converted to Opportunity'
    end as stage
  from crm_remote.leads l
  left join first_touch ft on ft.lead_id = l.lead_id
  left join crm_remote.opportunities o
    on o.rep_id = l.rep_id
   and o.created_at > l.created_at
) s
cross join params p
group by p.run_date, stage
on conflict (metric_date, lead_stage) do update set
  leads_in_stage = excluded.leads_in_stage,
  updated_at = now();
