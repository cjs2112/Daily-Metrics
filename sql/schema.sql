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
