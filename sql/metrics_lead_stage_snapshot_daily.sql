-- Phase 6A: Lead Stage Snapshot (Derived Funnel Health)

insert into metrics.lead_stage_snapshot_daily (
  metric_date,
  lead_stage,
  leads_in_stage,
  updated_at
)
select
  :run_date::date as metric_date,
  stage,
  count(*) as leads_in_stage,
  now()
from (
  select
    l.lead_id,
    case
      when ft.first_touch_at is null then 'Unengaged'
      when o.opportunity_id is null then 'Engaged'
      else 'Converted to Opportunity'
    end as stage
  from crm_remote.leads l
  left join (
    select lead_id, min(occurred_at) as first_touch_at
    from crm_remote.activities
    group by 1
  ) ft on ft.lead_id = l.lead_id
  left join crm_remote.opportunities o
    on o.rep_id = l.rep_id
   and o.created_at > l.created_at
) s
group by 1,2
on conflict (metric_date, lead_stage) do update set
  leads_in_stage = excluded.leads_in_stage,
  updated_at = now();
