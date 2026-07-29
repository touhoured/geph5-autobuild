-- Geph5 metrics: a fully convention-driven pipeline. There is NO central list
-- of metric names anywhere in this file.
--
--   * Telegraf (statsd -> outputs.postgresql, schema = "metrics") creates one
--     table per measurement inside the `metrics` schema. Membership in that
--     schema is what makes something a metric.
--   * Every pg_cron rollup tick discovers raw tables by introspection,
--     auto-creates missing `<name>_minutely` / `<name>_hourly` tiers (and a
--     time index on the raw table), and upserts the trailing window.
--   * Rollup tiers store tags as ONE `tags jsonb` column, built per-row from
--     whatever tag columns the raw table has at rollup time. A binary that
--     starts sending a new statsd tag therefore needs no DDL anywhere: new
--     rows simply carry the extra key, old rows honestly lack it. (Telegraf
--     adds the raw-table column by itself; nothing else has a schema to
--     change.) Numeric columns that appear later are added to the tiers
--     automatically — a metadata-only ALTER, since they are not in the key.
--   * Aggregation semantics come from the data itself: the statsd
--     `metric_type` column (counter -> sum, gauge -> avg) plus structural
--     per-column rules for timer tables (count -> sum, mean -> count-weighted
--     mean, *percentile*/upper -> max, lower -> min).
--   * Retention is by suffix convention: raw 14 days, minutely 90 days,
--     hourly forever.
--
-- So emitting a brand-new stat from any geph5 binary materializes the table,
-- indexes, rollup tiers, retention, and metric() support within one rollup
-- tick, with zero configuration here or anywhere else.
--
-- Apply with: psql "$POSTGRES_URL" -f metrics.sql   (idempotent)
-- Do NOT wrap in a single transaction (-1): the columnar->jsonb tier
-- migration at the bottom commits per table to keep locks and WAL bounded.

CREATE SCHEMA IF NOT EXISTS metrics;

------------------------------------------------------------------------------
-- One-time migration: move the original public-schema metric tables into the
-- metrics schema. Guarded; a no-op once they have moved. This is the only
-- place table names appear, and it exists purely to migrate legacy state.
------------------------------------------------------------------------------

DO $$
DECLARE
    t text;
    v text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'kbps', 'load', 'uptime', 'task_count', 'schedlag', 'bridge_bytes',
        'bridge_pools', 'broker_logins', 'plus', 'broker_sysstat', 'broker_rpc_calls'
    ] LOOP
        FOREACH v IN ARRAY ARRAY[t, t || '_minutely', t || '_hourly'] LOOP
            IF to_regclass('public.' || quote_ident(v)) IS NOT NULL THEN
                EXECUTE format('ALTER TABLE public.%I SET SCHEMA metrics', v);
            END IF;
        END LOOP;
    END LOOP;
END $$;

------------------------------------------------------------------------------
-- Purge stray function variants left by earlier applies that ran through the
-- transaction pooler with an inconsistent search_path.
------------------------------------------------------------------------------

DO $purge$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid) AS args
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname IN ('public', 'metrics')
          AND (p.proname LIKE 'metrics\_%' OR (n.nspname = 'metrics' AND p.proname = 'metric'))
    LOOP
        EXECUTE format('DROP FUNCTION %I.%I(%s)', r.nspname, r.proname, r.args);
    END LOOP;
END $purge$;

------------------------------------------------------------------------------
-- Introspection helpers.
------------------------------------------------------------------------------

-- Raw metric tables: every base table in the metrics schema that has a "time"
-- column and is not itself a rollup tier.
CREATE OR REPLACE FUNCTION metrics.raw_tables()
RETURNS SETOF text LANGUAGE sql STABLE AS $$
    SELECT t.table_name::text
    FROM information_schema.tables t
    WHERE t.table_schema = 'metrics' AND t.table_type = 'BASE TABLE'
      AND t.table_name NOT LIKE '%\_minutely' ESCAPE '\'
      AND t.table_name NOT LIKE '%\_hourly' ESCAPE '\'
      AND EXISTS (SELECT 1 FROM information_schema.columns c
                  WHERE c.table_schema = 'metrics' AND c.table_name = t.table_name
                    AND c.column_name = 'time')
$$;

CREATE OR REPLACE FUNCTION metrics.text_cols(tbl text)
RETURNS text[] LANGUAGE sql STABLE AS $$
    SELECT coalesce(array_agg(column_name::text ORDER BY column_name), '{}')
    FROM information_schema.columns
    WHERE table_schema = 'metrics' AND table_name = tbl AND data_type = 'text'
$$;

CREATE OR REPLACE FUNCTION metrics.num_cols(tbl text)
RETURNS text[] LANGUAGE sql STABLE AS $$
    SELECT coalesce(array_agg(column_name::text ORDER BY column_name), '{}')
    FROM information_schema.columns
    WHERE table_schema = 'metrics' AND table_name = tbl
      AND column_name <> 'time'
      AND data_type IN ('double precision', 'real', 'bigint', 'integer', 'smallint', 'numeric')
$$;

-- SQL expression building the tags jsonb for one row of `tbl` from its tag
-- columns (text columns minus the statsd bookkeeping column). Empty/NULL tag
-- values are stripped, so "tag not sent" and "tag empty" are one canonical
-- shape and series line up across the raw/rollup boundary.
CREATE OR REPLACE FUNCTION metrics.tags_expr(tbl text)
RETURNS text LANGUAGE sql STABLE AS $$
    SELECT coalesce(
        'jsonb_strip_nulls(jsonb_build_object(' ||
            string_agg(format('%L, nullif(%I, %L)', c, c, ''), ', ' ORDER BY c) ||
        '))',
        $j$'{}'::jsonb$j$)
    FROM unnest(metrics.text_cols(tbl)) c WHERE c <> 'metric_type'
$$;

-- SQL expression for one row's metric_type, tolerating tables that predate
-- the column.
CREATE OR REPLACE FUNCTION metrics.mtype_expr(tbl text)
RETURNS text LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN 'metric_type' = ANY (metrics.text_cols(tbl))
                THEN 'coalesce(metric_type, '''')' ELSE $e$''$e$ END
$$;

-- The statsd type of a metric ('counter' | 'gauge' | 'timing'), read from the
-- data; NULL when unknown (e.g. rows written before metric_type was stored).
CREATE OR REPLACE FUNCTION metrics.type_of(tbl text)
RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
    result text;
BEGIN
    IF NOT ('metric_type' = ANY (metrics.text_cols(tbl))) THEN
        RETURN NULL;
    END IF;
    EXECUTE format(
        'SELECT metric_type FROM metrics.%I WHERE metric_type IS NOT NULL
         ORDER BY "time" DESC LIMIT 1', tbl) INTO result;
    RETURN result;
END $$;

-- How to aggregate one numeric column when downsampling. Structural rules
-- only; no per-metric configuration.
CREATE OR REPLACE FUNCTION metrics.agg_expr(col text, mtype text, cols text[])
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN col = 'count' THEN format('sum(%I)', col)
        WHEN col = 'sum' THEN format('sum(%I)', col)
        WHEN col = 'mean' AND 'count' = ANY (cols)
            THEN format('sum(%I * "count") / nullif(sum("count"), 0)', col)
        WHEN col LIKE '%percentile%' OR col IN ('upper', 'max') THEN format('max(%I)', col)
        WHEN col IN ('lower', 'min') THEN format('min(%I)', col)
        WHEN mtype = 'counter' THEN format('sum(%I)', col)
        ELSE format('avg(%I)', col)
    END
$$;

------------------------------------------------------------------------------
-- Tier management: create missing rollup tables (and the raw time index).
-- Tier schema is FIXED for the life of the table: ("time", tags jsonb,
-- metric_type, <numeric columns>). Tags never require DDL again; numeric
-- columns that appear later on the raw table are added on the fly
-- (metadata-only, they are not part of the key).
------------------------------------------------------------------------------

-- (an earlier revision returned boolean, which CREATE OR REPLACE cannot undo)
DROP FUNCTION IF EXISTS metrics.ensure_tiers(text);
CREATE FUNCTION metrics.ensure_tiers(raw text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    suffix text;
    tier text;
    col text;
    cols_sql text;
BEGIN
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON metrics.%I ("time")',
                   raw || '_time_idx', raw);
    FOREACH suffix IN ARRAY ARRAY['_minutely', '_hourly'] LOOP
        tier := raw || suffix;
        IF to_regclass('metrics.' || quote_ident(tier)) IS NULL THEN
            cols_sql := '"time" timestamptz NOT NULL'
                || ', tags jsonb NOT NULL DEFAULT ''{}''::jsonb'
                || ', metric_type text NOT NULL DEFAULT ''''';
            FOREACH col IN ARRAY metrics.num_cols(raw) LOOP
                cols_sql := cols_sql || format(', %I double precision', col);
            END LOOP;
            cols_sql := cols_sql || ', PRIMARY KEY ("time", tags, metric_type)';
            EXECUTE format('CREATE TABLE metrics.%I (%s)', tier, cols_sql);
            CONTINUE;
        END IF;
        FOREACH col IN ARRAY metrics.num_cols(raw) LOOP
            CONTINUE WHEN col = ANY (metrics.num_cols(tier));
            EXECUTE format('ALTER TABLE metrics.%I ADD COLUMN %I double precision',
                           tier, col);
        END LOOP;
    END LOOP;
END $$;

------------------------------------------------------------------------------
-- Rollup: upsert the trailing window into one tier. The group key is derived
-- deterministically from each raw row's data, so re-rolling any window is
-- idempotent — including across tag-set changes.
------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.rollup_tier(raw text, trunc_unit text, suffix text, cutoff timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    tier text := raw || suffix;
    mtype text := metrics.type_of(raw);
    raw_nums text[] := metrics.num_cols(raw);
    nums text[];
    col text;
    ins_cols text := '"time", tags, metric_type';
    sel_aggs text := '';
    upd_sets text := '';
BEGIN
    -- numeric columns present in both raw and tier (ensure_tiers keeps the
    -- tier in sync; the intersection also tolerates hand-crafted tiers)
    SELECT coalesce(array_agg(c ORDER BY c), '{}') INTO nums
    FROM unnest(raw_nums) c WHERE c = ANY (metrics.num_cols(tier));
    IF array_length(nums, 1) IS NULL THEN
        RETURN;
    END IF;

    FOREACH col IN ARRAY nums LOOP
        ins_cols := ins_cols || format(', %I', col);
        sel_aggs := sel_aggs || format(', %s', metrics.agg_expr(col, mtype, raw_nums));
        upd_sets := upd_sets || format('%s%I = EXCLUDED.%I',
                                       CASE WHEN upd_sets = '' THEN '' ELSE ', ' END, col, col);
    END LOOP;

    EXECUTE format(
        $q$ INSERT INTO metrics.%I (%s)
            SELECT date_trunc(%L, "time"), %s, %s%s
            FROM metrics.%I WHERE "time" >= %L
            GROUP BY 1, 2, 3
            ON CONFLICT ("time", tags, metric_type) DO UPDATE SET %s $q$,
        tier, ins_cols, trunc_unit, metrics.tags_expr(raw), metrics.mtype_expr(raw),
        sel_aggs, raw, cutoff, upd_sets);
END $$;

CREATE OR REPLACE FUNCTION metrics.rollup(since timestamptz DEFAULT now() - interval '3 hours')
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    raw text;
BEGIN
    FOR raw IN SELECT metrics.raw_tables() LOOP
        PERFORM metrics.ensure_tiers(raw);
        PERFORM metrics.rollup_tier(raw, 'minute', '_minutely', date_trunc('minute', since));
        PERFORM metrics.rollup_tier(raw, 'hour', '_hourly', date_trunc('hour', since));
    END LOOP;
END $$;

------------------------------------------------------------------------------
-- Retention by suffix convention: raw 14 days, minutely 90 days.
------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION metrics.retention()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    raw text;
BEGIN
    FOR raw IN SELECT metrics.raw_tables() LOOP
        EXECUTE format('DELETE FROM metrics.%I WHERE "time" < now() - interval ''14 days''', raw);
        IF to_regclass('metrics.' || quote_ident(raw || '_minutely')) IS NOT NULL THEN
            EXECUTE format('DELETE FROM metrics.%I WHERE "time" < now() - interval ''90 days''',
                           raw || '_minutely');
        END IF;
    END LOOP;
END $$;

------------------------------------------------------------------------------
-- metric(): Graphite-style resolution transparency for Grafana. Lives in the
-- public schema (so panels call it unqualified) and reads the metrics schema.
--
--   SELECT "time", tags->>'exit' AS exit, value
--   FROM metric('kbps', $__timeFrom(), $__timeTo(), interval '$__interval')
--
-- For counters pass agg => 'sum'; for timer tables pass field => 'mean' etc.
------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.metric(
    p_name text,
    p_from timestamptz,
    p_to timestamptz,
    p_bucket interval,
    p_agg text DEFAULT 'avg',
    p_field text DEFAULT 'value'
) RETURNS TABLE ("time" timestamptz, tags jsonb, value double precision)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    raw_lo timestamptz;
    min_lo timestamptz;
    min_hi timestamptz;
    b1 timestamptz;  -- hourly/minutely boundary
    b2 timestamptz;  -- minutely/raw boundary
    src_sql text;
BEGIN
    IF p_agg NOT IN ('avg', 'sum', 'max', 'min') THEN
        RAISE EXCEPTION 'metric(): unsupported aggregation %', p_agg;
    END IF;

    -- Whisper-style resolution mixing: hourly [p_from, b1) + minutely
    -- [b1, b2) + raw [b2, p_to), boundaries placed by bucket size and tier
    -- coverage. Buckets >= 1h read hourly only; >= 1min prefer minutely with
    -- raw filling the not-yet-rolled-up tail; finer buckets prefer raw.
    EXECUTE format('SELECT min("time") FROM metrics.%I', p_name) INTO raw_lo;
    EXECUTE format('SELECT min("time"), max("time") + interval ''1 minute'' FROM metrics.%I',
                   p_name || '_minutely') INTO min_lo, min_hi;
    raw_lo := coalesce(raw_lo, p_to);

    IF p_bucket >= interval '1 hour' THEN
        b1 := p_to;
        b2 := p_to;
    ELSIF p_bucket >= interval '1 minute' THEN
        b2 := coalesce(min_hi, raw_lo);
        b1 := least(coalesce(min_lo, b2), b2);
    ELSE
        b2 := raw_lo;
        b1 := least(coalesce(min_lo, b2), b2);
    END IF;

    -- Tiers carry their tags jsonb as stored; raw rows build theirs from the
    -- raw table's current tag columns with the same canonical expression the
    -- rollup uses, so series are continuous across the tier boundary.
    src_sql := format(
        'SELECT "time", tags, %2$I AS __v FROM metrics.%3$I
             WHERE "time" >= %6$L AND "time" < %7$L
         UNION ALL
         SELECT "time", tags, %2$I FROM metrics.%4$I
             WHERE "time" >= %8$L AND "time" < %9$L
         UNION ALL
         SELECT "time", %1$s AS tags, %2$I FROM metrics.%5$I
             WHERE "time" >= %10$L AND "time" < %11$L',
        metrics.tags_expr(p_name), p_field,
        p_name || '_hourly', p_name || '_minutely', p_name,
        p_from, least(b1, p_to),
        greatest(p_from, b1), least(b2, p_to),
        greatest(p_from, b2), p_to);

    RETURN QUERY EXECUTE format(
        $q$ SELECT date_bin(%L, "time", timestamptz 'epoch') AS "time", tags,
                   %s(__v)::double precision AS value
            FROM (%s) __src
            GROUP BY 1, 2 ORDER BY 1 $q$,
        p_bucket, p_agg, src_sql);
END $$;

------------------------------------------------------------------------------
-- metric_rate(): per-second rate of a counter. Every returned bucket is
-- divided by its FULL width (exact), and incomplete edge buckets are dropped.
--
-- Why drop instead of normalize: a still-filling bucket cannot be rendered
-- accurately by any divisor. Dividing by the nominal width under-reports it
-- (full divisor, partial data); dividing by elapsed wall-clock over-reports
-- it (the raw tier is quantized into ~10s chunks and the minutely tier into
-- 1-min chunks, so when now() lands early in the bucket a whole data quantum
-- is already present but only a few seconds have "elapsed"). The only
-- artifact-free option is to show complete buckets only. A bucket is complete
-- when it lies fully within [p_from, min(p_to, now())]; the trailing point
-- therefore lags by up to one bucket, which is the standard honest behavior.
--
--   SELECT "time", tags->>'pool' AS pool, value * 8 AS bps   -- bytes/s -> bits/s
--   FROM metric_rate('bridge_bytes', $__timeFrom(), $__timeTo(), interval '$__interval')
------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.metric_rate(
    p_name text,
    p_from timestamptz,
    p_to timestamptz,
    p_bucket interval,
    p_field text DEFAULT 'value'
) RETURNS TABLE ("time" timestamptz, tags jsonb, value double precision)
LANGUAGE sql STABLE AS $$
    SELECT m."time", m.tags, m.value / extract(epoch from p_bucket)
    FROM metric(p_name, p_from, p_to, p_bucket, 'sum', p_field) m
    -- keep only buckets fully covered by the requested (and elapsed) range
    WHERE m."time" >= p_from
      AND m."time" + p_bucket <= least(p_to, now())
$$;

------------------------------------------------------------------------------
-- One-time migration: rebuild any tier still in the legacy columnar-tag
-- format (one text column per tag, tags in the primary key) into the jsonb
-- format. Detected by the absence of a `tags` jsonb column, so this is
-- idempotent and knows no table names. Each table commits separately to keep
-- locks, WAL, and peak disk bounded (hence: do not apply this file with -1).
------------------------------------------------------------------------------

DO $mig$
DECLARE
    t record;
    tier text;
    mtype text;
    nums text[];
    col text;
    cols_sql text;
    ins_cols text;
    sel_aggs text;
    lo timestamptz;
    hi timestamptz;
    slice timestamptz;
BEGIN
    FOR t IN
        SELECT table_name::text AS name FROM information_schema.tables it
        WHERE it.table_schema = 'metrics' AND it.table_type = 'BASE TABLE'
          AND (it.table_name LIKE '%\_minutely' ESCAPE '\'
               OR it.table_name LIKE '%\_hourly' ESCAPE '\')
          AND NOT EXISTS (SELECT 1 FROM information_schema.columns c
                          WHERE c.table_schema = 'metrics'
                            AND c.table_name = it.table_name
                            AND c.column_name = 'tags' AND c.data_type = 'jsonb')
        -- smallest first: dashboards heal quickly while the giant tables
        -- migrate last
        ORDER BY pg_total_relation_size(('metrics.' || quote_ident(it.table_name))::regclass)
    LOOP
        tier := t.name;
        -- Re-check freshness: the FOR list is a snapshot, and a concurrent
        -- apply may have migrated this tier since. Re-migrating a converted
        -- tier would squash its tags to {}.
        CONTINUE WHEN EXISTS (SELECT 1 FROM information_schema.columns c
                              WHERE c.table_schema = 'metrics' AND c.table_name = tier
                                AND c.column_name = 'tags' AND c.data_type = 'jsonb');
        mtype := metrics.type_of(tier);
        nums := metrics.num_cols(tier);

        cols_sql := '"time" timestamptz NOT NULL'
            || ', tags jsonb NOT NULL DEFAULT ''{}''::jsonb'
            || ', metric_type text NOT NULL DEFAULT ''''';
        ins_cols := '"time", tags, metric_type';
        sel_aggs := '';
        FOREACH col IN ARRAY nums LOOP
            cols_sql := cols_sql || format(', %I double precision', col);
            ins_cols := ins_cols || format(', %I', col);
            -- GROUP BY absorbs the (theoretical) key collisions from
            -- canonicalizing ''/NULL tags away
            sel_aggs := sel_aggs || format(', %s', metrics.agg_expr(col, mtype, nums));
        END LOOP;

        -- a leftover from an interrupted run holds stale partial data
        EXECUTE format('DROP TABLE IF EXISTS metrics.%I', tier || '__mig');
        EXECUTE format('CREATE TABLE metrics.%I (%s, PRIMARY KEY ("time", tags, metric_type))',
                       tier || '__mig', cols_sql);
        -- Copy in week slices, committing each: bounds WAL accumulation so a
        -- big table cannot push a managed instance over its disk threshold
        -- (Aiven flips read-only and kills the writer). Slicing on the raw
        -- "time" value never splits a group.
        EXECUTE format('SELECT min("time"), max("time") FROM metrics.%I', tier) INTO lo, hi;
        slice := lo;
        WHILE slice IS NOT NULL AND slice <= hi LOOP
            EXECUTE format(
                $q$ INSERT INTO metrics.%I (%s)
                    SELECT "time", %s, %s%s FROM metrics.%I
                    WHERE "time" >= %L AND "time" < %L
                    GROUP BY 1, 2, 3 $q$,
                tier || '__mig', ins_cols,
                metrics.tags_expr(tier), metrics.mtype_expr(tier), sel_aggs, tier,
                slice, slice + interval '7 days');
            COMMIT;
            slice := slice + interval '7 days';
        END LOOP;
        EXECUTE format('DROP TABLE metrics.%I', tier);
        EXECUTE format('ALTER TABLE metrics.%I RENAME TO %I', tier || '__mig', tier);
        EXECUTE format('ALTER INDEX metrics.%I RENAME TO %I',
                       tier || '__mig_pkey', tier || '_pkey');
        RAISE NOTICE 'migrated % to jsonb tags', tier;
        COMMIT;
    END LOOP;
END $mig$;

------------------------------------------------------------------------------
-- pg_cron schedules (cron.schedule upserts by job name).
------------------------------------------------------------------------------

SELECT cron.schedule('geph5-metrics-rollup', '*/10 * * * *', $$SELECT metrics.rollup()$$);
SELECT cron.schedule('geph5-metrics-retention', '20 4 * * *', $$SELECT metrics.retention()$$);
