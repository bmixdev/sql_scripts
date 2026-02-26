/* =====================================================================================================
   Postgres Pro Enterprise 16
   Полный скрипт: admin_trim — “урезание” большой таблицы по дате методом TRUNCATE + RELOAD
   + ЛОГИРОВАНИЕ В АВТОНОМНОЙ ТРАНЗАКЦИИ (BEGIN AUTONOMOUS ... COMMIT)
   + СТИЛЬ: все локальные переменные начинаются с v_
     (включая v_run_id вместо run_id)

   Что делает:
     1) Создаёт staging-таблицу (копия структуры), заливает туда срез WHERE date_column >= from_value
     2) При необходимости временно удаляет inbound FK (другие таблицы -> target_table), чтобы TRUNCATE прошёл
     3) При необходимости временно удаляет вторичные индексы (не PK и не constraint-backed), чтобы INSERT был быстрее
     4) LOCK target_table (ACCESS EXCLUSIVE), TRUNCATE, INSERT обратно из staging
     5) Восстанавливает вторичные индексы
     6) Восстанавливает inbound FK (по умолчанию как NOT VALID) и при необходимости валидирует
     7) При необходимости чинит sequence/identity по max(колонка)
     8) Пишет лог (START/UPDATE/FINISH) автономно: лог не откатывается при rollback основной операции

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


/* Журнал запусков */
CREATE TABLE IF NOT EXISTS admin_trim.run_log (
  run_id            uuid        PRIMARY KEY,
  target_table      regclass    NOT NULL,
  date_column       name        NOT NULL,
  from_value        timestamptz NOT NULL,
  work_schema       name        NOT NULL,
  staging_table     regclass    NULL,

  started_at        timestamptz NOT NULL DEFAULT now(),
  finished_at       timestamptz NULL,
  status            text        NOT NULL DEFAULT 'RUNNING', -- RUNNING|OK|FAILED
  error_message     text        NULL,

  -- опции
  use_unlogged_staging boolean  NOT NULL,
  drop_secondary_indexes boolean NOT NULL,
  disable_user_triggers boolean NOT NULL,
  drop_inbound_fks boolean NOT NULL,
  recreate_inbound_fks_not_valid boolean NOT NULL,
  validate_inbound_fks boolean NOT NULL,
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

  notes            text NULL
);

COMMENT ON TABLE admin_trim.run_log IS
'Журнал запусков admin_trim.trim_by_date_truncate_reload. Обновляется автономными транзакциями.';


-- -----------------------------------------------------------------------------------------------------
-- 2) Автономное логирование (Postgres Pro Enterprise)
-- -----------------------------------------------------------------------------------------------------

/* Автономный START (вставка/обновление записи лога) */
CREATE OR REPLACE PROCEDURE admin_trim.log_start_autonomous(
  p_run_id uuid,
  p_target_table regclass,
  p_date_column name,
  p_from_value timestamptz,
  p_work_schema name,
  p_use_unlogged_staging boolean,
  p_drop_secondary_indexes boolean,
  p_disable_user_triggers boolean,
  p_drop_inbound_fks boolean,
  p_recreate_inbound_fks_not_valid boolean,
  p_validate_inbound_fks boolean,
  p_fix_sequence_column name,
  p_pk_column name,
  p_require_date_index boolean,
  p_allow_date_fullscan boolean,
  p_notes text
)
LANGUAGE plpgsql
AS $$
BEGIN AUTONOMOUS
  INSERT INTO admin_trim.run_log(
    run_id, target_table, date_column, from_value, work_schema,
    use_unlogged_staging, drop_secondary_indexes, disable_user_triggers,
    drop_inbound_fks, recreate_inbound_fks_not_valid, validate_inbound_fks,
    fix_sequence_column, pk_column, require_date_index, allow_date_fullscan,
    notes, status, started_at
  )
  VALUES (
    p_run_id, p_target_table, p_date_column, p_from_value, p_work_schema,
    p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
    p_drop_inbound_fks, p_recreate_inbound_fks_not_valid, p_validate_inbound_fks,
    p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
    p_notes, 'RUNNING', now()
  )
  ON CONFLICT (run_id) DO UPDATE
    SET target_table = EXCLUDED.target_table,
        date_column = EXCLUDED.date_column,
        from_value = EXCLUDED.from_value,
        work_schema = EXCLUDED.work_schema,
        use_unlogged_staging = EXCLUDED.use_unlogged_staging,
        drop_secondary_indexes = EXCLUDED.drop_secondary_indexes,
        disable_user_triggers = EXCLUDED.disable_user_triggers,
        drop_inbound_fks = EXCLUDED.drop_inbound_fks,
        recreate_inbound_fks_not_valid = EXCLUDED.recreate_inbound_fks_not_valid,
        validate_inbound_fks = EXCLUDED.validate_inbound_fks,
        fix_sequence_column = EXCLUDED.fix_sequence_column,
        pk_column = EXCLUDED.pk_column,
        require_date_index = EXCLUDED.require_date_index,
        allow_date_fullscan = EXCLUDED.allow_date_fullscan,
        notes = EXCLUDED.notes;

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
  -- Логирование не должно ломать основной процесс
END;
$$;


/* Автономный UPDATE метрик/полей (универсальный) */
CREATE OR REPLACE PROCEDURE admin_trim.log_update_autonomous(
  p_run_id uuid,
  p_staging_table regclass DEFAULT NULL,
  p_src_count bigint DEFAULT NULL,
  p_staging_count bigint DEFAULT NULL,
  p_final_count bigint DEFAULT NULL,
  p_pk_checksum_before bigint DEFAULT NULL,
  p_pk_checksum_after bigint DEFAULT NULL,
  p_notes text DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN AUTONOMOUS
  UPDATE admin_trim.run_log
     SET staging_table = COALESCE(p_staging_table, staging_table),
         src_count = COALESCE(p_src_count, src_count),
         staging_count = COALESCE(p_staging_count, staging_count),
         final_count = COALESCE(p_final_count, final_count),
         pk_checksum_before = COALESCE(p_pk_checksum_before, pk_checksum_before),
         pk_checksum_after  = COALESCE(p_pk_checksum_after, pk_checksum_after),
         notes = COALESCE(p_notes, notes)
   WHERE run_id = p_run_id;

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
END;
$$;


/* Автономный FINISH (OK/FAILED) */
CREATE OR REPLACE PROCEDURE admin_trim.log_finish_autonomous(
  p_run_id uuid,
  p_status text,
  p_error_message text DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN AUTONOMOUS
  UPDATE admin_trim.run_log
     SET status = p_status,
         finished_at = now(),
         error_message = p_error_message
   WHERE run_id = p_run_id;

  COMMIT;
EXCEPTION WHEN OTHERS THEN
  ROLLBACK;
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


/* Drop вторичных индексов */
CREATE OR REPLACE PROCEDURE admin_trim.drop_secondary_indexes(p_table regclass)
LANGUAGE plpgsql
AS $$
DECLARE
  v_r record;
BEGIN
  FOR v_r IN
    SELECT i.indexrelid::regclass AS idx
    FROM pg_index i
    WHERE i.indrelid = p_table
      AND i.indisprimary = false
      AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
  LOOP
    EXECUTE format('DROP INDEX %s', v_r.idx::text);
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
    SELECT indexdef
    FROM admin_trim.index_backup
    WHERE run_id = p_run_id
      AND src_table = p_table
    ORDER BY index_name
  LOOP
    EXECUTE v_r.indexdef;
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
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', v_r.ref_table::text, v_r.conname);
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
    EXECUTE format(
      'ALTER TABLE %s ADD CONSTRAINT %I %s%s',
      v_r.ref_table::text,
      v_r.conname,
      v_r.condef,
      CASE WHEN p_not_valid THEN ' NOT VALID' ELSE '' END
    );
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

  p_work_schema name DEFAULT 'admin_trim_work',
  p_use_unlogged_staging boolean DEFAULT true,

  p_drop_secondary_indexes boolean DEFAULT true,
  p_disable_user_triggers boolean DEFAULT false,

  p_drop_inbound_fks boolean DEFAULT true,
  p_recreate_inbound_fks_not_valid boolean DEFAULT true,
  p_validate_inbound_fks boolean DEFAULT false,

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
BEGIN
  -- Определяем схему/имя таблицы (для формирования имени staging)
  SELECT n.nspname, c.relname
    INTO v_src_schema, v_src_table
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_table;

  -- Проверка индекса на колонку даты
  v_has_date_idx := admin_trim.has_index_on_column(p_table, p_date_column);

  IF p_require_date_index AND NOT v_has_date_idx AND NOT p_allow_date_fullscan THEN
    CALL admin_trim.log_start_autonomous(
      v_run_id, p_table, p_date_column, p_from_value, p_work_schema,
      p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
      p_drop_inbound_fks, p_recreate_inbound_fks_not_valid, p_validate_inbound_fks,
      p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
      'ОТКАЗ: нет индекса по колонке даты'
    );
    CALL admin_trim.log_finish_autonomous(v_run_id, 'FAILED', 'No valid index on date column');
    RAISE EXCEPTION 'На таблице % нет валидного индекса по колонке %. Включите p_allow_date_fullscan=true если уверены.',
      p_table::text, p_date_column;
  END IF;

  -- START-лог автономно
  CALL admin_trim.log_start_autonomous(
    v_run_id, p_table, p_date_column, p_from_value, p_work_schema,
    p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
    p_drop_inbound_fks, p_recreate_inbound_fks_not_valid, p_validate_inbound_fks,
    p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
    CASE WHEN v_has_date_idx THEN 'индекс по дате: ДА' ELSE 'индекс по дате: НЕТ (override)' END
  );

  -- Рабочая схема под staging
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_work_schema);

  -- Метрика: исходный count (можно убрать, если не нужен full scan)
  EXECUTE format('SELECT count(*)::bigint FROM %s', p_table::text) INTO v_src_count;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, v_src_count, NULL, NULL, NULL, NULL, NULL);

  -- Опционально checksum PK ДО
  IF p_pk_column IS NOT NULL THEN
    v_pk_before := admin_trim.pk_checksum(p_table, p_pk_column);
    CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, NULL, v_pk_before, NULL, NULL);
  END IF;

  -- 1) staging (создаём и наполняем до блокировок)
  v_stg_name := left(v_src_table || '__stg_' || replace(v_run_id::text, '-', ''), 63);

  EXECUTE format(
    'CREATE %s TABLE %I.%I (LIKE %s INCLUDING ALL)',
    CASE WHEN p_use_unlogged_staging THEN 'UNLOGGED' ELSE '' END,
    p_work_schema, v_stg_name, p_table::text
  );

  v_stg_reg := format('%I.%I', p_work_schema, v_stg_name)::regclass;
  CALL admin_trim.log_update_autonomous(v_run_id, v_stg_reg, NULL, NULL, NULL, NULL, NULL, NULL);

  v_sql := format(
    'INSERT INTO %s SELECT * FROM %s WHERE %I >= $1',
    v_stg_reg::text, p_table::text, p_date_column
  );
  EXECUTE v_sql USING p_from_value;

  EXECUTE format('ANALYZE %s', v_stg_reg::text);

  EXECUTE format('SELECT count(*)::bigint FROM %s', v_stg_reg::text) INTO v_stg_count;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, v_stg_count, NULL, NULL, NULL, NULL);

  -- 2) inbound FK: чтобы TRUNCATE прошёл
  IF p_drop_inbound_fks THEN
    PERFORM admin_trim.backup_inbound_fks(p_table, v_run_fk);
    CALL admin_trim.drop_inbound_fks_from_backup(v_run_fk, p_table);
  END IF;

  -- 3) бэкап вторичных индексов (создание бэкапа до блокировки)
  IF p_drop_secondary_indexes THEN
    PERFORM admin_trim.backup_secondary_indexes(p_table, v_run_idx);
  END IF;

  -- 4) критическая секция: lock + drop idx + truncate + reload
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', p_table::text);

  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, false);
  END IF;

  IF p_drop_secondary_indexes THEN
    CALL admin_trim.drop_secondary_indexes(p_table);
  END IF;

  EXECUTE format('TRUNCATE TABLE %s', p_table::text);

  EXECUTE format('INSERT INTO %s SELECT * FROM %s', p_table::text, v_stg_reg::text);

  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, true);
  END IF;

  EXECUTE format('ANALYZE %s', p_table::text);

  EXECUTE format('SELECT count(*)::bigint FROM %s', p_table::text) INTO v_final_count;
  CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, v_final_count, NULL, NULL, NULL);

  IF v_final_count IS DISTINCT FROM v_stg_count THEN
    RAISE EXCEPTION 'Несовпадение количества строк: staging=% final=% (таблица=%)',
      v_stg_count, v_final_count, p_table::text;
  END IF;

  -- 5) восстановить вторичные индексы
  IF p_drop_secondary_indexes THEN
    CALL admin_trim.create_secondary_indexes_from_backup(v_run_idx, p_table);
  END IF;

  -- 6) восстановить inbound FK
  IF p_drop_inbound_fks THEN
    CALL admin_trim.create_inbound_fks_from_backup(v_run_fk, p_table, p_recreate_inbound_fks_not_valid);

    IF p_validate_inbound_fks THEN
      CALL admin_trim.validate_inbound_fks(p_table);
    END IF;
  END IF;

  -- 7) починка sequence
  IF p_fix_sequence_column IS NOT NULL THEN
    CALL admin_trim.fix_sequence_by_max(p_table, p_fix_sequence_column);
  END IF;

  -- 8) checksum PK ПОСЛЕ + сравнение
  IF p_pk_column IS NOT NULL THEN
    v_pk_after := admin_trim.pk_checksum(p_table, p_pk_column);
    CALL admin_trim.log_update_autonomous(v_run_id, NULL, NULL, NULL, NULL, NULL, v_pk_after, NULL);

    IF v_pk_after IS DISTINCT FROM v_pk_before THEN
      RAISE EXCEPTION 'Несовпадение checksum PK: before=% after=% (таблица=%)',
        v_pk_before, v_pk_after, p_table::text;
    END IF;
  END IF;

  -- 9) удалить staging
  EXECUTE format('DROP TABLE %s', v_stg_reg::text);

  -- FINISH OK (автономно)
  CALL admin_trim.log_finish_autonomous(v_run_id, 'OK', NULL);

EXCEPTION WHEN OTHERS THEN
  -- FINISH FAILED (автономно)
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
