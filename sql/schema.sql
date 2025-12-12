-- ============================================================
-- Create Schema
-- ============================================================
CREATE SCHEMA IF NOT EXISTS metrics;

-- ============================================================
-- Table: daily_lead_metrics
-- Granularity: 1 row per day
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics.daily_lead_metrics (
    metric_date        date PRIMARY KEY,

    -- Lead volume
    new_leads          integer DEFAULT 0,
    qualified_leads    integer DEFAULT 0,
    disqualified_leads integer DEFAULT 0,

    -- Funnel
    meetings_scheduled integer DEFAULT 0,
    opportunities_created integer DEFAULT 0,

    -- Revenue-related
    deals_won          integer DEFAULT 0,
    deals_lost         integer DEFAULT 0,
    revenue_won        numeric(12,2) DEFAULT 0,

    -- Meta
    created_at         timestamp with time zone DEFAULT now()
);

-- ============================================================
-- Table: rep_daily_metrics
-- Granularity: 1 row per rep per day
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics.rep_daily_metrics (
    metric_date       date,
    rep_id            bigint,
    
    -- Activity metrics
    calls_made        integer DEFAULT 0,
    emails_sent       integer DEFAULT 0,
    meetings_held     integer DEFAULT 0,

    -- Pipeline/Performance metrics
    opps_created      integer DEFAULT 0,
    deals_won         integer DEFAULT 0,
    revenue_won       numeric(12,2) DEFAULT 0,

    -- Meta
    created_at        timestamp with time zone DEFAULT now(),

    PRIMARY KEY (metric_date, rep_id)
);

-- ============================================================
-- Table: funnel_stage_metrics
-- Granularity: per day per stage
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics.funnel_stage_metrics (
    metric_date       date,
    stage_name        text,
    leads_in_stage    integer DEFAULT 0,
    moved_forward     integer DEFAULT 0,
    moved_backward    integer DEFAULT 0,
    exited_stage      integer DEFAULT 0,

    created_at        timestamp with time zone DEFAULT now(),

    PRIMARY KEY (metric_date, stage_name)
);

-- ============================================================
-- Table: system_log
-- Purpose: Track ETL runs, errors, debugging info
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics.system_log (
    log_id            bigserial PRIMARY KEY,
    event_time        timestamp with time zone DEFAULT now(),
    level             text NOT NULL,        -- INFO / WARN / ERROR
    message           text NOT NULL,
    extra_json        jsonb
);
-- ============================================================
-- PHASE 6 — EXECUTIVE METRICS LAYER
-- ============================================================

-- ------------------------------------------------------------
-- Table: exec_funnel_daily
-- Purpose: Executive-level funnel, velocity, and effectiveness
-- Granularity: 1 row per day
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics.exec_funnel_daily (
    metric_date date PRIMARY KEY,

    -- Funnel volume
    leads_created      integer DEFAULT 0,
    leads_engaged      integer DEFAULT 0,
    opps_created       integer DEFAULT 0,

    -- Outcomes
    opps_won           integer DEFAULT 0,
    opps_lost          integer DEFAULT 0,
    pipeline_created   numeric(14,2) DEFAULT 0,
    revenue_won        numeric(14,2) DEFAULT 0,

    -- Conversion & velocity
    lead_to_opp_conv_pct     numeric(9,4),
    avg_days_to_first_touch  numeric(12,4),
    avg_days_lead_to_opp     numeric(12,4),

    -- Meta
    updated_at timestamp with time zone DEFAULT now()
);

-- ------------------------------------------------------------
-- Table: lead_stage_snapshot_daily
-- Purpose: Funnel health & leakage (derived stages)
-- Granularity: 1 row per stage per day
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics.lead_stage_snapshot_daily (
    metric_date    date,
    lead_stage     text,
    leads_in_stage integer DEFAULT 0,

    updated_at timestamp with time zone DEFAULT now(),

    PRIMARY KEY (metric_date, lead_stage)
);

-- ------------------------------------------------------------
-- Table: rep_efficiency_daily
-- Purpose: Activity-to-outcome efficiency by rep
-- Granularity: 1 row per rep per day
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metrics.rep_efficiency_daily (
    metric_date date,
    rep_id      bigint,

    -- Activity
    touches          integer DEFAULT 0,

    -- Production
    leads_created    integer DEFAULT 0,
    opps_created     integer DEFAULT 0,
    pipeline_created numeric(14,2) DEFAULT 0,
    revenue_won      numeric(14,2) DEFAULT 0,

    -- Efficiency
    touches_per_opp numeric(12,4),

    updated_at timestamp with time zone DEFAULT now(),

    PRIMARY KEY (metric_date, rep_id)
);

