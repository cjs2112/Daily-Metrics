-- Phase 6A: Executive Funnel Daily Metrics
-- Event-derived funnel logic
-- One row per day

with params as (
  select :run_date::date as run_date
),

leads as (
  select lead_id, rep_id, created_at
  from crm_remote.leads
),

activities as (
  select lead_id, occurred_at
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

first_touch as (
  select
    l.lead_id,
    min(a.occurred_at) as first_touch_at
  from leads l
  left join activities a on a.lead_id = l.lead_id
  group by 1
),

y_leads as (
  select *
  from leads l
  join params p on l.created_at::date = p.run_date
),

y_engaged as (
  select *
  from first_touch ft
  join params p on ft.first_touch_at::date = p.run_date
),

y_opps as (
  select *
  from opps o
  join params p on o.created_at::date = p.run_date
),

y_won as (
  select *
  from opps o
  join params p
    on o.is_closed = true
   and o.is_won = true
   and o.closed_at::date = p.run_date
),

y_lost as (
  select *
  from opps o
  join params p
    on o.is_closed = true
   and o.is_won = false
   and o.closed_at::date = p.run_date
),

velocity as (
  select
    p.run_date,
    avg(extract(epoch from (ft.first_touch_at - l.created_at)) / 86400.0)
      filter (where ft.first_touch_at is not null)
      as avg_days_to_first_touch,
    avg(extract(epoch from (o.created_at - l.created_at)) / 86400.0)
      as avg_days_lead_to_opp
  from params p
  join leads l on l.created_at::date = p.run_date
  left join first_touch ft on ft.lead_id = l.lead_id
  left join opps o
    on o.rep_id = l.rep_id
   and o.created_at > l.created_at
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
  (select count(*) from y_leads),
  (select count(*) from y_engaged),
  (select count(*) from y_opps),
  (select count(*) from y_won),
  (select count(*) from y_lost),
  (select coalesce(sum(amount),0) from y_opps),
  (select coalesce(sum(amount),0) from y_won),
  case
    when (select count(*) from y_leads) > 0
    then (select count(*) from y_opps)::numeric
         / (select count(*) from y_leads)
  end,
  v.avg_days_to_first_touch,
  v.avg_days_lead_to_opp,
  now()
from params p
left join velocity v on v.run_date = p.run_date
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
