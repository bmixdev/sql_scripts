/* =====================================================================================================
   PostgreSQL 14+ (совместимо с PostgreSQL Community и Postgres Pro Enterprise)
   Полный скрипт: admin_trim — “урезание” большой таблицы по дате методом TRUNCATE + RELOAD
   + ЛОГИРОВАНИЕ ЧЕРЕЗ DBLINK (автономная фиксация лога, совместимо с Community)
   + СТИЛЬ: все локальные переменные начинаются с v_
     (включая v_run_id вместо run_id)

   Что делает:
     1) Создаёт staging-таблицу (копия структуры), заливает туда срез WHERE date_column >= from_value
     2) При необходимости временно удаляет inbound FK (другие таблицы -> target_table), чтобы TRUNCATE прошёл
     3) При необходимости временно удаляет outbound FK (target_table -> другие таблицы), чтобы ускорить массовый INSERT
     3) При необходимости временно удаляет вторичные индексы (не PK и не constraint-backed), чтобы INSERT был быстрее
     4) LOCK target_table (ACCESS EXCLUSIVE), TRUNCATE, INSERT обратно из staging
     5) Восстанавливает вторичные индексы
     6) Восстанавливает inbound/outbound FK (по умолчанию как NOT VALID) и при необходимости валидирует
     7) При необходимости чинит sequence/identity по max(колонка)
     8) Пишет лог (START/UPDATE/FINISH) через dblink в отдельной сессии (автономно)

   Важно:
     - TRUNCATE требует ACCESS EXCLUSIVE lock (окно простоя).
     - NOT VALID для FK существенно снижает блокировки/время, validate можно сделать позже.
     - Checksum по PK (опционально) может быть очень тяжёлым на десятках миллионов строк.

   Запуск:
     CALL admin_trim.trim_by_date_truncate_reload(
       'public.big'::regclass,
       'dt',
       '2025-01-01 00:00:00+00'::timestamptz
     );

===================================================================================================== */

-- -----------------------------------------------------------------------------------------------------
-- 0) Схема и расширение
-- -----------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS admin_trim;
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS dblink;


-- -----------------------------------------------------------------------------------------------------
-- 1) Таблицы для бэкапов и журнала
-- -----------------------------------------------------------------------------------------------------

/* Бэкап DDL вторичных индексов (НЕ PK и НЕ constraint-backed) */
CREATE TABLE IF NOT EXISTS admin_trim.index_backup (
  run_id        uuid        NOT NULL,
  src_table     regclass    NOT NULL,
  index_oid     regclass    NOT NULL,
  index_name    text        NOT NULL,
  indexdef      text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, index_oid)
);

COMMENT ON TABLE admin_trim.index_backup IS
'Бэкап DDL вторичных индексов (drop/recreate для ускорения массовой вставки).';


/* Бэкап inbound FK (другие таблицы -> target_table) */
CREATE TABLE IF NOT EXISTS admin_trim.inbound_fk_backup (
  run_id        uuid        NOT NULL,
  target_table  regclass    NOT NULL,
  ref_table     regclass    NOT NULL,
  conname       text        NOT NULL,
  condef        text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, target_table, ref_table, conname)
);

COMMENT ON TABLE admin_trim.inbound_fk_backup IS
'Бэкап inbound FK (другие таблицы -> target_table).';


/* Бэкап outbound FK (target_table -> другие таблицы) */
CREATE TABLE IF NOT EXISTS admin_trim.outbound_fk_backup (
  run_id          uuid        NOT NULL,
  src_table       regclass    NOT NULL,
  ref_table       regclass    NOT NULL,
  conname         text        NOT NULL,
  condef          text        NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, src_table, conname)
);

COMMENT ON TABLE admin_trim.outbound_fk_backup IS
'Бэкап outbound FK (target_table -> другие таблицы).';


/* Журнал запусков */
CREATE TABLE IF NOT EXISTS admin_trim.run_log (
  run_id            uuid        PRIMARY KEY,
  target_table      regclass    NOT NULL,
  date_column       name        NOT NULL,
  from_value        timestamptz NOT NULL,
  work_schema       name        NOT NULL,
  staging_table     regclass    NULL,

  preflight_only    boolean     NOT NULL DEFAULT false,
  phase             text        NOT NULL DEFAULT 'INIT',
  phase_started_at  timestamptz NULL,
  phase_finished_at timestamptz NULL,

  started_at        timestamptz NOT NULL DEFAULT now(),
  finished_at       timestamptz NULL,
  status            text        NOT NULL DEFAULT 'RUNNING', -- RUNNING|OK|FAILED
  error_message     text        NULL,

  -- опции
  use_unlogged_staging boolean  NOT NULL,
  drop_secondary_indexes boolean NOT NULL,
  disable_user_triggers boolean NOT NULL,
  drop_inbound_fks boolean NOT NULL,
  drop_outbound_fks boolean NOT NULL,
  recreate_inbound_fks_not_valid boolean NOT NULL,
  recreate_outbound_fks_not_valid boolean NOT NULL,
  validate_inbound_fks boolean NOT NULL,
  validate_outbound_fks boolean NOT NULL,
  fix_sequence_column name NULL,
  pk_column name NULL,
  require_date_index boolean NOT NULL,
  allow_date_fullscan boolean NOT NULL,

  -- метрики
  src_count         bigint NULL,
  staging_count     bigint NULL,
  final_count       bigint NULL,
  pk_checksum_before bigint NULL,
  pk_checksum_after  bigint NULL,

  preflight_report  jsonb NULL,
  warnings          text NULL,
  lock_wait_ms      bigint NULL,
  rebuild_index_count integer NULL,
  inbound_fk_count  integer NULL,

  notes            text NULL
);

COMMENT ON TABLE admin_trim.run_log IS
'Журнал запусков admin_trim.trim_by_date_truncate_reload. Обновляется автономно через dblink.';


-- -----------------------------------------------------------------------------------------------------
-- 2) Логирование (автономно через dblink)
-- -----------------------------------------------------------------------------------------------------

/* Получить connstr для dblink (через GUC admin_trim.dblink_connstr либо текущую БД) */
CREATE OR REPLACE FUNCTION admin_trim.get_dblink_connstr()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  v_connstr text;
BEGIN
  v_connstr := NULLIF(current_setting('admin_trim.dblink_connstr', true), '');
  IF v_connstr IS NOT NULL THEN
    RETURN v_connstr;
  END IF;

  RETURN format('dbname=%L', current_database());
END $$;


/* Выполнить SQL автономно через dblink */
CREATE OR REPLACE PROCEDURE admin_trim.log_exec_autonomous(
  p_sql text
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_connstr text;
BEGIN
  v_connstr := admin_trim.get_dblink_connstr();
  PERFORM dblink_exec(v_connstr, p_sql);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'log_exec_autonomous: выполнение через dblink пропущено: %, %', SQLSTATE, SQLERRM;
END;
$$;

/* START (вставка/обновление записи лога) */
CREATE OR REPLACE PROCEDURE admin_trim.log_start_autonomous(
  p_run_id uuid,
  p_target_table regclass,
  p_date_column name,
  p_from_value timestamptz,
  p_work_schema name,
  p_preflight_only boolean,
  p_use_unlogged_staging boolean,
  p_drop_secondary_indexes boolean,
  p_disable_user_triggers boolean,
  p_drop_inbound_fks boolean,
  p_drop_outbound_fks boolean,
  p_recreate_inbound_fks_not_valid boolean,
  p_recreate_outbound_fks_not_valid boolean,
  p_validate_inbound_fks boolean,
  p_validate_outbound_fks boolean,
  p_fix_sequence_column name,
  p_pk_column name,
  p_require_date_index boolean,
  p_allow_date_fullscan boolean,
  p_notes text
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_sql text;
BEGIN
  v_sql := format($SQL$
    INSERT INTO admin_trim.run_log(
      run_id, target_table, date_column, from_value, work_schema,
      preflight_only,
      use_unlogged_staging, drop_secondary_indexes, disable_user_triggers,
      drop_inbound_fks, drop_outbound_fks,
      recreate_inbound_fks_not_valid, recreate_outbound_fks_not_valid,
      validate_inbound_fks, validate_outbound_fks,
      fix_sequence_column, pk_column, require_date_index, allow_date_fullscan,
      notes, status, started_at, phase, phase_started_at
    )
    VALUES (
      %1$L::uuid, %2$L::regclass, %3$L::name, %4$L::timestamptz, %5$L::name,
      %6$L::boolean,
      %7$L::boolean, %8$L::boolean, %9$L::boolean,
      %10$L::boolean, %11$L::boolean,
      %12$L::boolean, %13$L::boolean,
      %14$L::boolean, %15$L::boolean,
      %16$L::name, %17$L::name, %18$L::boolean, %19$L::boolean,
      %20$L, 'RUNNING', now(), 'PRECHECK', now()
    )
    ON CONFLICT (run_id) DO UPDATE
      SET target_table = EXCLUDED.target_table,
          date_column = EXCLUDED.date_column,
          from_value = EXCLUDED.from_value,
          work_schema = EXCLUDED.work_schema,
          preflight_only = EXCLUDED.preflight_only,
          use_unlogged_staging = EXCLUDED.use_unlogged_staging,
          drop_secondary_indexes = EXCLUDED.drop_secondary_indexes,
          disable_user_triggers = EXCLUDED.disable_user_triggers,
          drop_inbound_fks = EXCLUDED.drop_inbound_fks,
          drop_outbound_fks = EXCLUDED.drop_outbound_fks,
          recreate_inbound_fks_not_valid = EXCLUDED.recreate_inbound_fks_not_valid,
          recreate_outbound_fks_not_valid = EXCLUDED.recreate_outbound_fks_not_valid,
          validate_inbound_fks = EXCLUDED.validate_inbound_fks,
          validate_outbound_fks = EXCLUDED.validate_outbound_fks,
          fix_sequence_column = EXCLUDED.fix_sequence_column,
          pk_column = EXCLUDED.pk_column,
          require_date_index = EXCLUDED.require_date_index,
          allow_date_fullscan = EXCLUDED.allow_date_fullscan,
          notes = EXCLUDED.notes
  $SQL$,
    p_run_id,
    p_target_table::text,
    p_date_column,
    p_from_value,
    p_work_schema,
    p_preflight_only,
    p_use_unlogged_staging,
    p_drop_secondary_indexes,
    p_disable_user_triggers,
    p_drop_inbound_fks,
    p_drop_outbound_fks,
    p_recreate_inbound_fks_not_valid,
    p_recreate_outbound_fks_not_valid,
    p_validate_inbound_fks,
    p_validate_outbound_fks,
    p_fix_sequence_column,
    p_pk_column,
    p_require_date_index,
    p_allow_date_fullscan,
    p_notes
  );

  CALL admin_trim.log_exec_autonomous(v_sql);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'log_start_autonomous: запись лога пропущена: %, %', SQLSTATE, SQLERRM;
END;
$$;


/* UPDATE метрик/полей (универсальный) */
CREATE OR REPLACE PROCEDURE admin_trim.log_update_autonomous(
  p_run_id uuid,
  p_staging_table regclass DEFAULT NULL,
  p_src_count bigint DEFAULT NULL,
  p_staging_count bigint DEFAULT NULL,
  p_final_count bigint DEFAULT NULL,
  p_pk_checksum_before bigint DEFAULT NULL,
  p_pk_checksum_after bigint DEFAULT NULL,
  p_preflight_report jsonb DEFAULT NULL,
  p_warnings text DEFAULT NULL,
  p_lock_wait_ms bigint DEFAULT NULL,
  p_rebuild_index_count integer DEFAULT NULL,
  p_inbound_fk_count integer DEFAULT NULL,
  p_notes text DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_sql text;
BEGIN
  v_sql := format($SQL$
    UPDATE admin_trim.run_log
       SET staging_table = COALESCE(to_regclass(%1$L), staging_table),
           src_count = COALESCE(%2$L::bigint, src_count),
           staging_count = COALESCE(%3$L::bigint, staging_count),
           final_count = COALESCE(%4$L::bigint, final_count),
           pk_checksum_before = COALESCE(%5$L::bigint, pk_checksum_before),
           pk_checksum_after  = COALESCE(%6$L::bigint, pk_checksum_after),
           preflight_report = COALESCE(%7$L::jsonb, preflight_report),
           warnings = COALESCE(%8$L, warnings),
           lock_wait_ms = COALESCE(%9$L::bigint, lock_wait_ms),
           rebuild_index_count = COALESCE(%10$L::integer, rebuild_index_count),
           inbound_fk_count = COALESCE(%11$L::integer, inbound_fk_count),
           notes = COALESCE(%12$L, notes)
     WHERE run_id = %13$L::uuid
  $SQL$,
    p_staging_table::text,
    p_src_count,
    p_staging_count,
    p_final_count,
    p_pk_checksum_before,
    p_pk_checksum_after,
    p_preflight_report::text,
    p_warnings,
    p_lock_wait_ms,
    p_rebuild_index_count,
    p_inbound_fk_count,
    p_notes,
    p_run_id
  );

  CALL admin_trim.log_exec_autonomous(v_sql);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'log_update_autonomous: обновление лога пропущено для run_id=%: %, %', p_run_id, SQLSTATE, SQLERRM;
END;
$$;


/* UPDATE фазы выполнения */
CREATE OR REPLACE PROCEDURE admin_trim.log_phase_autonomous(
  p_run_id uuid,
  p_phase text,
  p_notes text DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_sql text;
BEGIN
  v_sql := format($SQL$
    UPDATE admin_trim.run_log
       SET phase = %1$L,
           phase_started_at = CASE WHEN phase IS DISTINCT FROM %1$L THEN now() ELSE phase_started_at END,
           phase_finished_at = CASE WHEN phase IS DISTINCT FROM %1$L THEN NULL ELSE phase_finished_at END,
           notes = COALESCE(%2$L, notes)
     WHERE run_id = %3$L::uuid
  $SQL$,
    p_phase,
    p_notes,
    p_run_id
  );

  CALL admin_trim.log_exec_autonomous(v_sql);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'log_phase_autonomous: обновление фазы пропущено для run_id=% phase=%: %, %', p_run_id, p_phase, SQLSTATE, SQLERRM;
END;
$$;


/* FINISH (OK/FAILED) */
CREATE OR REPLACE PROCEDURE admin_trim.log_finish_autonomous(
  p_run_id uuid,
  p_status text,
  p_error_message text DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_sql text;
BEGIN
  v_sql := format($SQL$
    UPDATE admin_trim.run_log
       SET status = %1$L,
           finished_at = now(),
           phase_finished_at = COALESCE(phase_finished_at, now()),
           error_message = %2$L
      WHERE run_id = %3$L::uuid
  $SQL$,
    p_status,
    p_error_message,
    p_run_id
  );

  CALL admin_trim.log_exec_autonomous(v_sql);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'log_finish_autonomous: завершение лога пропущено для run_id=% status=%: %, %', p_run_id, p_status, SQLSTATE, SQLERRM;
END;
$$;


-- -----------------------------------------------------------------------------------------------------
-- 3) Хелперы (индекс на колонку, checksum PK, индексы, inbound FK, триггеры, sequence)
-- -----------------------------------------------------------------------------------------------------

/* Проверка наличия валидного индекса, где колонка участвует в ключе */
CREATE OR REPLACE FUNCTION admin_trim.has_index_on_column(
  p_table regclass,
  p_column name
) RETURNS boolean
LANGUAGE sql
AS $$
  WITH att AS (
    SELECT a.attnum
    FROM pg_attribute a
    WHERE a.attrelid = p_table
      AND a.attname  = p_column
      AND a.attnum > 0
      AND NOT a.attisdropped
  )
  SELECT EXISTS (
    SELECT 1
    FROM pg_index i
    JOIN att ON true
    WHERE i.indrelid = p_table
      AND i.indisvalid
      AND i.indisready
      AND att.attnum = ANY (i.indkey)
  );
$$;


/* Preflight-отчёт по готовности операции (без изменения данных) */
CREATE OR REPLACE FUNCTION admin_trim.build_preflight_report(
  p_table regclass,
  p_date_column name,
  p_from_value timestamptz,
  p_work_schema name,
  p_require_date_index boolean,
  p_allow_date_fullscan boolean
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_col_exists boolean;
  v_col_type text;
  v_has_date_idx boolean;
  v_has_work_schema_privileges boolean;
  v_has_target_table_privileges boolean;
  v_inbound_fk_count integer;
  v_outbound_fk_count integer;
  v_secondary_index_count integer;
  v_tbl_size bigint;
  v_idx_size bigint;
  v_total_size bigint;
  v_estimated_rows bigint;
  v_plan jsonb;
  v_ready boolean;
  v_warnings text;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_attribute a
    WHERE a.attrelid = p_table
      AND a.attname = p_date_column
      AND a.attnum > 0
      AND NOT a.attisdropped
  ) INTO v_col_exists;

  SELECT format_type(a.atttypid, a.atttypmod)
    INTO v_col_type
  FROM pg_attribute a
  WHERE a.attrelid = p_table
    AND a.attname = p_date_column
    AND a.attnum > 0
    AND NOT a.attisdropped;

  v_has_date_idx := admin_trim.has_index_on_column(p_table, p_date_column);

  v_has_work_schema_privileges := has_schema_privilege(current_user, p_work_schema::text, 'USAGE,CREATE');
  v_has_target_table_privileges := has_table_privilege(current_user, p_table, 'SELECT,INSERT,TRUNCATE');

  SELECT count(*)::int
    INTO v_inbound_fk_count
  FROM pg_constraint c
  WHERE c.contype = 'f'
    AND c.confrelid = p_table;

  SELECT count(*)::int
    INTO v_outbound_fk_count
  FROM pg_constraint c
  WHERE c.contype = 'f'
    AND c.conrelid = p_table;

  SELECT count(*)::int
    INTO v_secondary_index_count
  FROM pg_index i
  WHERE i.indrelid = p_table
    AND i.indisprimary = false
    AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid);

  SELECT pg_relation_size(p_table),
         pg_indexes_size(p_table),
         pg_total_relation_size(p_table)
    INTO v_tbl_size, v_idx_size, v_total_size;

  IF v_col_exists AND v_has_target_table_privileges THEN
    BEGIN
      EXECUTE format('EXPLAIN (FORMAT JSON) SELECT * FROM %s WHERE %I >= %L', p_table::text, p_date_column, p_from_value)
         INTO v_plan;

      v_estimated_rows := COALESCE((v_plan -> 0 -> 'Plan' ->> 'Plan Rows')::bigint, 0);
    EXCEPTION WHEN OTHERS THEN
      v_estimated_rows := NULL;
      v_warnings := concat_ws('; ', v_warnings, format('EXPLAIN завершился ошибкой: %s: %s', SQLSTATE, SQLERRM));
    END;
  END IF;

  IF NOT v_col_exists THEN
    v_warnings := concat_ws('; ', v_warnings, format('Колонка даты "%s" не найдена', p_date_column));
  END IF;

  IF NOT v_has_work_schema_privileges THEN
    v_warnings := concat_ws('; ', v_warnings, format('Недостаточно прав на схему %s (нужны USAGE, CREATE)', p_work_schema));
  END IF;

  IF NOT v_has_target_table_privileges THEN
    v_warnings := concat_ws('; ', v_warnings, format('Недостаточно прав на таблицу %s (нужны SELECT, INSERT, TRUNCATE)', p_table::text));
  END IF;

  v_ready := v_col_exists
             AND (v_has_date_idx OR NOT p_require_date_index OR p_allow_date_fullscan)
             AND v_has_work_schema_privileges
             AND v_has_target_table_privileges;

  RETURN jsonb_build_object(
    'ready', v_ready,
    'target_table', p_table::text,
    'date_column_exists', v_col_exists,
    'date_column_type', v_col_type,
    'has_date_index', v_has_date_idx,
    'allow_date_fullscan', p_allow_date_fullscan,
    'require_date_index', p_require_date_index,
    'inbound_fk_count', v_inbound_fk_count,
    'outbound_fk_count', v_outbound_fk_count,
    'secondary_index_count', v_secondary_index_count,
    'table_size_bytes', v_tbl_size,
    'index_size_bytes', v_idx_size,
    'total_size_bytes', v_total_size,
    'estimated_rows_to_keep', v_estimated_rows,
    'has_work_schema_privileges', v_has_work_schema_privileges,
    'has_target_table_privileges', v_has_target_table_privileges,
    'warnings', NULLIF(v_warnings, '')
  );
END $$;


/* Checksum по PK (дорого на больших таблицах) */
CREATE OR REPLACE FUNCTION admin_trim.pk_checksum(
  p_table regclass,
  p_pk_column name
) RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v_result bigint;
  v_sql text;
BEGIN
  v_sql := format(
    'SELECT COALESCE(bit_xor(hashtextextended((%I)::text, 0)), 0)::bigint FROM %s',
    p_pk_column,
    p_table::text
  );
  EXECUTE v_sql INTO v_result;
  RETURN v_result;
END $$;


/* Бэкап вторичных индексов (НЕ PK и НЕ constraint-backed) */
CREATE OR REPLACE FUNCTION admin_trim.backup_secondary_indexes(
  p_table regclass,
  p_run_id uuid DEFAULT gen_random_uuid()
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT
      i.indexrelid::regclass AS index_oid,
      ic.relname AS index_name,
      pg_get_indexdef(i.indexrelid) AS indexdef
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    WHERE i.indrelid = p_table
      AND i.indisprimary = false
      AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
    ORDER BY ic.relname
  LOOP
    INSERT INTO admin_trim.index_backup(run_id, src_table, index_oid, index_name, indexdef)
    VALUES (p_run_id, p_table, v_r.index_oid, v_r.index_name, v_r.indexdef)
    ON CONFLICT (run_id, index_oid) DO UPDATE
      SET indexdef = EXCLUDED.indexdef,
          created_at = now();
  END LOOP;

  RETURN p_run_id;
END $$;


/* Drop вторичных индексов строго по backup конкретного запуска */
CREATE OR REPLACE PROCEDURE admin_trim.drop_secondary_indexes(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
  v_index_qualified text;
BEGIN
  FOR v_r IN
    SELECT b.index_name
    FROM admin_trim.index_backup b
    WHERE b.run_id = p_run_id
      AND b.src_table = p_table
    ORDER BY b.index_name
  LOOP
    SELECT format('%I.%I', n.nspname, ic.relname)
      INTO v_index_qualified
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_namespace n ON n.oid = ic.relnamespace
    WHERE i.indrelid = p_table
      AND ic.relname = v_r.index_name
    LIMIT 1;

    IF v_index_qualified IS NOT NULL THEN
      EXECUTE format('DROP INDEX %s', v_index_qualified);
    END IF;
  END LOOP;
END $$;


/* Recreate вторичных индексов (без CONCURRENTLY) */
CREATE OR REPLACE PROCEDURE admin_trim.create_secondary_indexes_from_backup(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT index_name, indexdef
    FROM admin_trim.index_backup
    WHERE run_id = p_run_id
      AND src_table = p_table
    ORDER BY index_name
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM pg_index i
      JOIN pg_class ic ON ic.oid = i.indexrelid
      WHERE i.indrelid = p_table
        AND ic.relname = v_r.index_name
    ) THEN
      EXECUTE v_r.indexdef;
    END IF;
  END LOOP;
END $$;


/* Бэкап inbound FK (другие таблицы -> p_table) */
CREATE OR REPLACE FUNCTION admin_trim.backup_inbound_fks(
  p_table regclass,
  p_run_id uuid DEFAULT gen_random_uuid()
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT
      c.conrelid::regclass AS ref_table,
      c.conname,
      pg_get_constraintdef(c.oid, true) AS condef
    FROM pg_constraint c
    WHERE c.contype = 'f'
      AND c.confrelid = p_table
    ORDER BY c.conrelid::regclass::text, c.conname
  LOOP
    INSERT INTO admin_trim.inbound_fk_backup(run_id, target_table, ref_table, conname, condef)
    VALUES (p_run_id, p_table, v_r.ref_table, v_r.conname, v_r.condef)
    ON CONFLICT (run_id, target_table, ref_table, conname) DO UPDATE
      SET condef = EXCLUDED.condef,
          created_at = now();
  END LOOP;

  RETURN p_run_id;
END $$;


/* Drop inbound FK по бэкапу */
CREATE OR REPLACE PROCEDURE admin_trim.drop_inbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT ref_table, conname
    FROM admin_trim.inbound_fk_backup
    WHERE run_id = p_run_id
      AND target_table = p_table
    ORDER BY ref_table::text, conname
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I', v_r.ref_table::text, v_r.conname);
  END LOOP;
END $$;


/* Create inbound FK обратно (по умолчанию NOT VALID) */
CREATE OR REPLACE PROCEDURE admin_trim.create_inbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass,
  p_not_valid boolean DEFAULT true
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT ref_table, conname, condef
    FROM admin_trim.inbound_fk_backup
    WHERE run_id = p_run_id
      AND target_table = p_table
    ORDER BY ref_table::text, conname
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      WHERE c.conrelid = v_r.ref_table
        AND c.conname = v_r.conname
    ) THEN
      EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %I %s%s',
        v_r.ref_table::text,
        v_r.conname,
        v_r.condef,
        CASE WHEN p_not_valid THEN ' NOT VALID' ELSE '' END
      );
    END IF;
  END LOOP;
END $$;


/* Бэкап outbound FK (p_table -> другие таблицы) */
CREATE OR REPLACE FUNCTION admin_trim.backup_outbound_fks(
  p_table regclass,
  p_run_id uuid DEFAULT gen_random_uuid()
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT
      c.confrelid::regclass AS ref_table,
      c.conname,
      pg_get_constraintdef(c.oid, true) AS condef
    FROM pg_constraint c
    WHERE c.contype = 'f'
      AND c.conrelid = p_table
    ORDER BY c.conname
  LOOP
    INSERT INTO admin_trim.outbound_fk_backup(run_id, src_table, ref_table, conname, condef)
    VALUES (p_run_id, p_table, v_r.ref_table, v_r.conname, v_r.condef)
    ON CONFLICT (run_id, src_table, conname) DO UPDATE
      SET condef = EXCLUDED.condef,
          ref_table = EXCLUDED.ref_table,
          created_at = now();
  END LOOP;

  RETURN p_run_id;
END $$;


/* Drop outbound FK по бэкапу */
CREATE OR REPLACE PROCEDURE admin_trim.drop_outbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT conname
    FROM admin_trim.outbound_fk_backup
    WHERE run_id = p_run_id
      AND src_table = p_table
    ORDER BY conname
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I', p_table::text, v_r.conname);
  END LOOP;
END $$;


/* Create outbound FK обратно (по умолчанию NOT VALID) */
CREATE OR REPLACE PROCEDURE admin_trim.create_outbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass,
  p_not_valid boolean DEFAULT true
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT conname, condef
    FROM admin_trim.outbound_fk_backup
    WHERE run_id = p_run_id
      AND src_table = p_table
    ORDER BY conname
  LOOP
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint c
      WHERE c.conrelid = p_table
        AND c.conname = v_r.conname
    ) THEN
      EXECUTE format(
        'ALTER TABLE %s ADD CONSTRAINT %I %s%s',
        p_table::text,
        v_r.conname,
        v_r.condef,
        CASE WHEN p_not_valid THEN ' NOT VALID' ELSE '' END
      );
    END IF;
  END LOOP;
END $$;


/* Валидировать inbound FK, которые NOT VALID */
CREATE OR REPLACE PROCEDURE admin_trim.validate_inbound_fks(p_table regclass)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT c.conrelid::regclass AS ref_table, c.conname
    FROM pg_constraint c
    WHERE c.contype = 'f'
      AND c.confrelid = p_table
      AND c.convalidated = false
  LOOP
    EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', v_r.ref_table::text, v_r.conname);
  END LOOP;
END $$;


/* Валидировать outbound FK, которые NOT VALID */
CREATE OR REPLACE PROCEDURE admin_trim.validate_outbound_fks(p_table regclass)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT c.conname
    FROM pg_constraint c
    WHERE c.contype = 'f'
      AND c.conrelid = p_table
      AND c.convalidated = false
  LOOP
    EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', p_table::text, v_r.conname);
  END LOOP;
END $$;


/* Вкл/выкл пользовательских триггеров на таблице */
CREATE OR REPLACE PROCEDURE admin_trim.set_user_triggers(
  p_table regclass,
  p_enable boolean
)
LANGUAGE plpgsql
AS $$
BEGIN
  IF p_enable THEN
    EXECUTE format('ALTER TABLE %s ENABLE TRIGGER USER', p_table::text);
  ELSE
    EXECUTE format('ALTER TABLE %s DISABLE TRIGGER USER', p_table::text);
  END IF;
END $$;


/* Починка sequence по max(col) */
CREATE OR REPLACE PROCEDURE admin_trim.fix_sequence_by_max(
  p_table regclass,
  p_column name
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_seq_name text;
  v_max_val bigint;
BEGIN
  SELECT pg_get_serial_sequence(p_table::text, p_column::text) INTO v_seq_name;

  IF v_seq_name IS NULL THEN
    RAISE NOTICE 'Не найдена sequence для %.% (возможно не serial/identity)', p_table::text, p_column;
    RETURN;
  END IF;

  EXECUTE format('SELECT max(%I)::bigint FROM %s', p_column, p_table::text) INTO v_max_val;
  IF v_max_val IS NULL THEN
    v_max_val := 0;
  END IF;

  EXECUTE format('SELECT setval(%L, %s, true)', v_seq_name, v_max_val);
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 4) Главная процедура TRUNCATE + RELOAD (все локальные переменные с v_)
-- -----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE admin_trim.trim_by_date_truncate_reload(
  p_table regclass,
  p_date_column name,
  p_from_value timestamptz,

  p_preflight_only boolean DEFAULT false,

  p_work_schema name DEFAULT 'admin_trim_work',
  p_use_unlogged_staging boolean DEFAULT true,

  p_drop_secondary_indexes boolean DEFAULT true,
  p_disable_user_triggers boolean DEFAULT false,

  p_drop_inbound_fks boolean DEFAULT true,
  p_drop_outbound_fks boolean DEFAULT false,
  p_recreate_inbound_fks_not_valid boolean DEFAULT true,
  p_recreate_outbound_fks_not_valid boolean DEFAULT true,
  p_validate_inbound_fks boolean DEFAULT false,
  p_validate_outbound_fks boolean DEFAULT false,

  p_do_analyze_staging boolean DEFAULT true,
  p_do_analyze_final boolean DEFAULT true,
  p_collect_src_count boolean DEFAULT false,

  p_fix_sequence_column name DEFAULT NULL,

  p_pk_column name DEFAULT NULL,
  p_require_date_index boolean DEFAULT true,
  p_allow_date_fullscan boolean DEFAULT false
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_run_id uuid := gen_random_uuid();
  v_run_idx uuid := gen_random_uuid();
  v_run_fk  uuid := gen_random_uuid();
  v_run_fk_out uuid := gen_random_uuid();

  v_src_schema text;
  v_src_table  text;

  v_stg_name text;
  v_stg_reg regclass;

  v_has_date_idx boolean;

  v_src_count bigint;
  v_stg_count bigint;
  v_final_count bigint;

  v_pk_before bigint;
  v_pk_after  bigint;

  v_sql text;
  v_preflight_report jsonb;
  v_lock_started timestamptz;
  v_lock_wait_ms bigint;
  v_rebuild_index_count integer := 0;
  v_inbound_fk_count integer := 0;
  v_rows_inserted_staging bigint := 0;
  v_rows_inserted_final bigint := 0;
  v_advisory_lock_ok boolean;
  v_secondary_indexes_dropped boolean := false;
  v_inbound_fks_dropped boolean := false;
  v_outbound_fks_dropped boolean := false;
  v_ts_proc_started timestamptz;
  v_ts_block_started timestamptz;
  v_block_ms bigint;
  v_proc_ms bigint;
BEGIN
  v_ts_proc_started := clock_timestamp();

  RAISE NOTICE 'trim: старт, таблица=%, колонка_даты=%, from_value=%, только_preflight=%',
    p_table::text, p_date_column, p_from_value, p_preflight_only;

  v_ts_block_started := clock_timestamp();
  SELECT pg_try_advisory_xact_lock(hashtextextended(p_table::text, 919191))
    INTO v_advisory_lock_ok;

  IF NOT v_advisory_lock_ok THEN
    RAISE EXCEPTION 'Уже выполняется операция trim для таблицы %', p_table::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM admin_trim.run_log rl
    WHERE rl.target_table = p_table
      AND rl.status = 'RUNNING'
  ) THEN
    RAISE EXCEPTION 'Найден незавершенный запуск в admin_trim.run_log для таблицы %', p_table::text;
  END IF;
  v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
  RAISE NOTICE 'блок GUARD_CHECKS выполнен за % мс', v_block_ms;

  -- Определяем схему/имя таблицы (для формирования имени staging)
  SELECT n.nspname, c.relname
    INTO v_src_schema, v_src_table
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_table;

  -- Проверка индекса на колонку даты
  v_has_date_idx := admin_trim.has_index_on_column(p_table, p_date_column);
  RAISE NOTICE 'проверка индекса по дате: has_date_index=%', v_has_date_idx;

  IF p_require_date_index AND NOT v_has_date_idx AND NOT p_allow_date_fullscan THEN
    CALL admin_trim.log_start_autonomous(
      v_run_id, p_table, p_date_column, p_from_value, p_work_schema,
      p_preflight_only,
      p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
      p_drop_inbound_fks, p_drop_outbound_fks,
      p_recreate_inbound_fks_not_valid, p_recreate_outbound_fks_not_valid,
      p_validate_inbound_fks, p_validate_outbound_fks,
      p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
      'ОТКАЗ: нет индекса по колонке даты'
    );
    CALL admin_trim.log_finish_autonomous(v_run_id, 'FAILED', 'Нет валидного индекса по колонке даты');
    RAISE EXCEPTION 'На таблице % нет валидного индекса по колонке %. Включите p_allow_date_fullscan=true если уверены.',
      p_table::text, p_date_column;
  END IF;

  -- START-лог
  CALL admin_trim.log_start_autonomous(
    v_run_id, p_table, p_date_column, p_from_value, p_work_schema,
    p_preflight_only,
    p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
    p_drop_inbound_fks, p_drop_outbound_fks,
    p_recreate_inbound_fks_not_valid, p_recreate_outbound_fks_not_valid,
    p_validate_inbound_fks, p_validate_outbound_fks,
    p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
    CASE WHEN v_has_date_idx THEN 'индекс по дате: ДА' ELSE 'индекс по дате: НЕТ (override)' END
  );

  CALL admin_trim.log_phase_autonomous(v_run_id, 'PRECHECK', 'Старт preflight-проверок');
  RAISE NOTICE 'фаза PRECHECK начата, run_id=%', v_run_id;

  v_ts_block_started := clock_timestamp();
  v_preflight_report := admin_trim.build_preflight_report(
    p_table,
    p_date_column,
    p_from_value,
    p_work_schema,
    p_require_date_index,
    p_allow_date_fullscan
  );

  v_inbound_fk_count := COALESCE((v_preflight_report ->> 'inbound_fk_count')::integer, 0);
  v_rebuild_index_count := COALESCE((v_preflight_report ->> 'secondary_index_count')::integer, 0);
  RAISE NOTICE 'результат preflight: ready=%, inbound_fk_count=%, secondary_index_count=%, warnings=%',
    COALESCE((v_preflight_report ->> 'ready')::boolean, false),
    v_inbound_fk_count,
    v_rebuild_index_count,
    COALESCE(v_preflight_report ->> 'warnings', 'none');

  CALL admin_trim.log_update_autonomous(
    v_run_id,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    v_preflight_report,
    CASE
      WHEN (v_preflight_report ->> 'ready')::boolean THEN (v_preflight_report ->> 'warnings')
      ELSE COALESCE((v_preflight_report ->> 'warnings'), 'Preflight-проверки не пройдены')
    END,
    NULL,
    v_rebuild_index_count,
    v_inbound_fk_count,
    NULL
  );
  v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
  RAISE NOTICE 'блок PRECHECK выполнен за % мс (ready=%)',
    v_block_ms,
    COALESCE((v_preflight_report ->> 'ready')::boolean, false);

  IF COALESCE((v_preflight_report ->> 'ready')::boolean, false) IS DISTINCT FROM true THEN
    CALL admin_trim.log_finish_autonomous(v_run_id, 'FAILED', 'Preflight-проверки не пройдены');
    RAISE EXCEPTION 'Preflight-проверки не пройдены: %', v_preflight_report::text;
  END IF;

  IF p_preflight_only THEN
    RAISE NOTICE 'preflight_only=true, завершение без модификации данных, run_id=%', v_run_id;
    CALL admin_trim.log_finish_autonomous(v_run_id, 'PREFLIGHT_OK', NULL);
    RETURN;
  END IF;

  -- Рабочая схема под staging
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_work_schema);
  RAISE NOTICE 'рабочая схема подготовлена: %', p_work_schema;

  -- Метрика: исходный count (опциональный full scan)
  IF p_collect_src_count THEN
    v_ts_block_started := clock_timestamp();
    EXECUTE format('SELECT count(*)::bigint FROM %s', p_table::text) INTO v_src_count;
    CALL admin_trim.log_update_autonomous(v_run_id, NULL, v_src_count, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок SRC_COUNT выполнен за % мс (src_count=%)', v_block_ms, v_src_count;
  END IF;

  -- Опционально checksum PK ДО
  IF p_pk_column IS NOT NULL THEN
    v_ts_block_started := clock_timestamp();
    v_pk_before := admin_trim.pk_checksum(p_table, p_pk_column);
    CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, NULL, v_pk_before, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок PK_CHECKSUM_BEFORE выполнен за % мс (checksum=%)', v_block_ms, v_pk_before;
  END IF;

  -- 1) staging (создаём и наполняем до блокировок)
  CALL admin_trim.log_phase_autonomous(v_run_id, 'STAGING_FILL', 'Создание и наполнение staging');
  v_ts_block_started := clock_timestamp();

  v_stg_name := left(v_src_table || '__stg_' || replace(v_run_id::text, '-', ''), 63);

  EXECUTE format(
    'CREATE %s TABLE %I.%I (LIKE %s INCLUDING ALL)',
    CASE WHEN p_use_unlogged_staging THEN 'UNLOGGED' ELSE '' END,
    p_work_schema, v_stg_name, p_table::text
  );

  v_stg_reg := format('%I.%I', p_work_schema, v_stg_name)::regclass;
  RAISE NOTICE 'staging создана: %', v_stg_reg::text;
  CALL admin_trim.log_update_autonomous(v_run_id, v_stg_reg, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

  v_sql := format(
    'INSERT INTO %s SELECT * FROM %s WHERE %I >= $1',
    v_stg_reg::text, p_table::text, p_date_column
  );
  EXECUTE v_sql USING p_from_value;
  GET DIAGNOSTICS v_rows_inserted_staging = ROW_COUNT;
  RAISE NOTICE 'наполнение staging завершено: rows=%', v_rows_inserted_staging;

  IF p_do_analyze_staging THEN
    EXECUTE format('ANALYZE %s', v_stg_reg::text);
  END IF;

  v_stg_count := v_rows_inserted_staging;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, v_stg_count, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
  RAISE NOTICE 'блок STAGING_FILL выполнен за % мс (staging_rows=%)', v_block_ms, v_stg_count;

  -- 2) inbound FK: чтобы TRUNCATE прошёл
  IF p_drop_inbound_fks THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'FK_DROP', 'Удаление inbound FK по backup');
    v_ts_block_started := clock_timestamp();
    PERFORM admin_trim.backup_inbound_fks(p_table, v_run_fk);
    CALL admin_trim.drop_inbound_fks_from_backup(v_run_fk, p_table);
    v_inbound_fks_dropped := true;
    RAISE NOTICE 'inbound FK удалены по backup, run_id=%', v_run_fk;
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок FK_DROP выполнен за % мс', v_block_ms;
  END IF;

  IF p_drop_outbound_fks THEN
    v_ts_block_started := clock_timestamp();
    PERFORM admin_trim.backup_outbound_fks(p_table, v_run_fk_out);
    RAISE NOTICE 'backup outbound FK создан, run_id=%', v_run_fk_out;
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок OUTBOUND_FK_BACKUP выполнен за % мс', v_block_ms;
  END IF;

  -- 3) бэкап вторичных индексов (создание бэкапа до блокировки)
  IF p_drop_secondary_indexes THEN
    v_ts_block_started := clock_timestamp();
    PERFORM admin_trim.backup_secondary_indexes(p_table, v_run_idx);
    RAISE NOTICE 'backup вторичных индексов создан, run_id=%', v_run_idx;
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок INDEX_BACKUP выполнен за % мс', v_block_ms;
  END IF;

  -- 4) критическая секция: lock + drop idx + truncate + reload
  CALL admin_trim.log_phase_autonomous(v_run_id, 'TRUNCATE_RELOAD', 'Критическая секция с ACCESS EXCLUSIVE');
  v_ts_block_started := clock_timestamp();
  v_lock_started := clock_timestamp();
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', p_table::text);
  v_lock_wait_ms := floor(extract(epoch FROM clock_timestamp() - v_lock_started) * 1000)::bigint;
  RAISE NOTICE 'блокировка получена: wait_ms=%', v_lock_wait_ms;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, v_lock_wait_ms, NULL, NULL, NULL);

  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, false);
  END IF;

  IF p_drop_secondary_indexes THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'INDEX_DROP', 'Удаление вторичных индексов');
    CALL admin_trim.drop_secondary_indexes(v_run_idx, p_table);
    v_secondary_indexes_dropped := true;
    RAISE NOTICE 'вторичные индексы удалены по snapshot backup, run_id=%', v_run_idx;
  END IF;

  IF p_drop_outbound_fks THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'OUTBOUND_FK_DROP', 'Удаление outbound FK по backup');
    CALL admin_trim.drop_outbound_fks_from_backup(v_run_fk_out, p_table);
    v_outbound_fks_dropped := true;
    RAISE NOTICE 'outbound FK удалены по backup, run_id=%', v_run_fk_out;
  END IF;

  EXECUTE format('TRUNCATE TABLE %s', p_table::text);

  EXECUTE format('INSERT INTO %s SELECT * FROM %s', p_table::text, v_stg_reg::text);
  GET DIAGNOSTICS v_rows_inserted_final = ROW_COUNT;
  RAISE NOTICE 'финальная загрузка завершена: rows=%', v_rows_inserted_final;

  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, true);
  END IF;

  IF p_do_analyze_final THEN
    EXECUTE format('ANALYZE %s', p_table::text);
  END IF;

  v_final_count := v_rows_inserted_final;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, v_final_count, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
  RAISE NOTICE 'блок TRUNCATE_RELOAD выполнен за % мс (final_rows=%)', v_block_ms, v_final_count;

  IF v_final_count IS DISTINCT FROM v_stg_count THEN
    RAISE EXCEPTION 'Несовпадение количества строк: staging=% final=% (таблица=%)',
      v_stg_count, v_final_count, p_table::text;
  END IF;

  -- 5) восстановить вторичные индексы
  IF p_drop_secondary_indexes THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'INDEX_RESTORE', 'Восстановление вторичных индексов');
    v_ts_block_started := clock_timestamp();
    CALL admin_trim.create_secondary_indexes_from_backup(v_run_idx, p_table);
    v_secondary_indexes_dropped := false;
    RAISE NOTICE 'вторичные индексы восстановлены из backup, run_id=%', v_run_idx;
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок INDEX_RESTORE выполнен за % мс', v_block_ms;
  END IF;

  -- 6) восстановить inbound FK
  IF p_drop_inbound_fks THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'FK_RESTORE', 'Восстановление inbound FK');
    v_ts_block_started := clock_timestamp();
    CALL admin_trim.create_inbound_fks_from_backup(v_run_fk, p_table, p_recreate_inbound_fks_not_valid);
    v_inbound_fks_dropped := false;
    RAISE NOTICE 'inbound FK восстановлены из backup, run_id=% (not_valid=%)', v_run_fk, p_recreate_inbound_fks_not_valid;

    IF p_validate_inbound_fks THEN
      CALL admin_trim.validate_inbound_fks(p_table);
    END IF;

    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок FK_RESTORE выполнен за % мс (validated=%)', v_block_ms, p_validate_inbound_fks;
  END IF;

  IF p_drop_outbound_fks THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'OUTBOUND_FK_RESTORE', 'Восстановление outbound FK');
    v_ts_block_started := clock_timestamp();
    CALL admin_trim.create_outbound_fks_from_backup(v_run_fk_out, p_table, p_recreate_outbound_fks_not_valid);
    v_outbound_fks_dropped := false;
    RAISE NOTICE 'outbound FK восстановлены из backup, run_id=% (not_valid=%)', v_run_fk_out, p_recreate_outbound_fks_not_valid;

    IF p_validate_outbound_fks THEN
      CALL admin_trim.validate_outbound_fks(p_table);
    END IF;

    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок OUTBOUND_FK_RESTORE выполнен за % мс (validated=%)', v_block_ms, p_validate_outbound_fks;
  END IF;

  -- 7) починка sequence
  IF p_fix_sequence_column IS NOT NULL THEN
    CALL admin_trim.log_phase_autonomous(v_run_id, 'FINALIZE', 'Выравнивание sequence');
    v_ts_block_started := clock_timestamp();
    CALL admin_trim.fix_sequence_by_max(p_table, p_fix_sequence_column);
    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок SEQUENCE_FIX выполнен за % мс (column=%)', v_block_ms, p_fix_sequence_column;
  END IF;

  -- 8) checksum PK ПОСЛЕ + сравнение
  IF p_pk_column IS NOT NULL THEN
    v_ts_block_started := clock_timestamp();
    v_pk_after := admin_trim.pk_checksum(p_table, p_pk_column);
    CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, NULL, NULL, v_pk_after, NULL, NULL, NULL, NULL, NULL, NULL);

    IF v_pk_after IS DISTINCT FROM v_pk_before THEN
      RAISE EXCEPTION 'Несовпадение checksum PK: before=% after=% (таблица=%)',
        v_pk_before, v_pk_after, p_table::text;
    END IF;

    v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
    RAISE NOTICE 'блок PK_CHECKSUM_AFTER выполнен за % мс (checksum=%)', v_block_ms, v_pk_after;
  END IF;

  -- 9) удалить staging
  v_ts_block_started := clock_timestamp();
  EXECUTE format('DROP TABLE %s', v_stg_reg::text);
  RAISE NOTICE 'staging удалена: %', v_stg_reg::text;
  v_block_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_block_started) * 1000)::bigint;
  RAISE NOTICE 'блок STAGING_DROP выполнен за % мс', v_block_ms;

  -- FINISH OK
  CALL admin_trim.log_finish_autonomous(v_run_id, 'OK', NULL);
  v_proc_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_proc_started) * 1000)::bigint;
  RAISE NOTICE 'общее время trim: % мс', v_proc_ms;
  RAISE NOTICE 'trim успешно завершен, run_id=%', v_run_id;

EXCEPTION WHEN OTHERS THEN
  v_proc_ms := floor(extract(epoch FROM clock_timestamp() - v_ts_proc_started) * 1000)::bigint;
  RAISE NOTICE 'общее время trim до ошибки: % мс', v_proc_ms;
  RAISE NOTICE 'trim завершился с ошибкой для run_id=%: %, %', v_run_id, SQLSTATE, SQLERRM;
  -- best-effort: восстановление объектов при частичном сбое
  BEGIN
    IF v_secondary_indexes_dropped THEN
      CALL admin_trim.create_secondary_indexes_from_backup(v_run_idx, p_table);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  BEGIN
    IF v_inbound_fks_dropped THEN
      CALL admin_trim.create_inbound_fks_from_backup(v_run_fk, p_table, p_recreate_inbound_fks_not_valid);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  BEGIN
    IF v_outbound_fks_dropped THEN
      CALL admin_trim.create_outbound_fks_from_backup(v_run_fk_out, p_table, p_recreate_outbound_fks_not_valid);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  -- FINISH FAILED
  CALL admin_trim.log_finish_autonomous(v_run_id, 'FAILED', SQLSTATE || ': ' || SQLERRM);

  -- best-effort: удалить staging
  BEGIN
    IF v_stg_reg IS NOT NULL THEN
      EXECUTE format('DROP TABLE IF EXISTS %s', v_stg_reg::text);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  RAISE;
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 5) Примеры запуска (закомментированы)
-- -----------------------------------------------------------------------------------------------------
/*
-- Базовый запуск:
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz
);

-- Если нужно поправить sequence:
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_fix_sequence_column => 'id'
);

-- Сразу валидировать inbound FK (может быть долго):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_validate_inbound_fks => true
);

-- Отключать пользовательские триггеры (если допустимо):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_disable_user_triggers => true
);

-- Включить checksum по PK (тяжело):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_pk_column => 'id'
);

-- Смотреть лог:
SELECT * FROM admin_trim.run_log ORDER BY started_at DESC;
*/
