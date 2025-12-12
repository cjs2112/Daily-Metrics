-- Phase 6A: Rep Efficiency Daily Metrics

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
  :run_date::date,
  r.rep_id,
  coalesce(t.touches,0),
  coalesce(l.leads_created,0),
  coalesce(o.opps_created,0),
  coalesce(o.pipeline_created,0),
  coalesce(w.revenue_won,0),
  case
    when coalesce(o.opps_created,0) > 0
    then t.touches::numeric / o.opps_created
  end,
  now()
from crm_remote.reps r
left join (
  select rep_id, count(*) as touches
  from crm_remote.activities
  where occurred_at::date = :run_date
  group by 1
) t on t.rep_id = r.rep_id
left join (
  select rep_id, count(*) as leads_created
  from crm_remote.leads
  where created_at::date = :run_date
  group by 1
) l on l.rep_id = r.rep_id
left join (
  select rep_id,
         count(*) as opps_created,
         sum(amount) as pipeline_created
  from crm_remote.opportunities
  where created_at::date = :run_date
  group by 1
) o on o.rep_id = r.rep_id
left join (
  select rep_id, sum(amount) as revenue_won
  from crm_remote.opportunities
  where is_closed = true
    and is_won = true
    and closed_at::date = :run_date
  group by 1
) w on w.rep_id = r.rep_id
on conflict (metric_date, rep_id) do update set
  touches = excluded.touches,
  leads_created = excluded.leads_created,
  opps_created = excluded.opps_created,
  pipeline_created = excluded.pipeline_created,
  revenue_won = excluded.revenue_won,
  touches_per_opp = excluded.touches_per_opp,
  updated_at = now();
