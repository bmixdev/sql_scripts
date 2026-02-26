/* =====================================================================================================
  Полный скрипт: admin_trim — “урезание” большой таблицы по дате методом TRUNCATE + RELOAD

  Сценарий:
    1) Создаём staging-таблицу (копию структуры основной таблицы) и копируем туда только нужные строки
       (например: dt >= '2025-01-01').
    2) Если на основную таблицу ссылаются внешние ключи (FK) из других таблиц (inbound FK),
       то TRUNCATE основной таблицы упадёт (RESTRICT по умолчанию).
       Поэтому:
         - мы делаем бэкап inbound FK,
         - временно удаляем inbound FK,
         - выполняем TRUNCATE,
         - загружаем данные обратно,
         - восстанавливаем inbound FK (обычно как NOT VALID),
         - и при необходимости позже валидируем (VALIDATE CONSTRAINT).
    3) Чтобы вставка (INSERT) была быстрее, можно временно удалить вторичные индексы (не PK и не UNIQUE constraints)
       и потом создать их заново.
    4) По желанию можно временно отключать пользовательские триггеры на таблице во время reload.
       Внимание: это может обойти бизнес-логику, аудиты и т.д. Используйте только если понимаете последствия.
    5) Добавлен журнал запусков (run_log), проверки и метрики:
         - проверка наличия индекса на колонку даты (защита от случайного full scan)
         - контроль количества строк (staging_count == final_count)
         - опционально тяжёлый контроль целостности по PK checksum (на 50M строк может быть дорого)
         - опциональная починка sequence/identity (setval по max(pk))

  Важные замечания:
    - TRUNCATE берёт ACCESS EXCLUSIVE lock на таблицу (на время критической секции). Это окно простоя.
    - Если у вас логическая репликация / триггеры аудита / каскады, подход требует аккуратности.
    - Скрипт не использует TRUNCATE ... CASCADE, чтобы случайно не очистить дочерние таблицы.
    - Индексы типа PK/UNIQUE constraints мы НЕ трогаем, чтобы не потерять семантику ограничений.
    - Восстановление inbound FK по умолчанию NOT VALID: это быстрее и меньше блокирует.
      Потом можно вызвать validate_inbound_fks() отдельно.

  Как использовать:
    1) Выполните этот скрипт один раз (создаст схему admin_trim, таблицы и процедуры).
    2) Запускайте:
         CALL admin_trim.trim_by_date_truncate_reload('public.big'::regclass, 'dt', '2025-01-01 00:00:00+00');

  Совместимость:
    - PostgreSQL 12+ (для процедур CALL); на PG16 — ок.

===================================================================================================== */

-- -----------------------------------------------------------------------------------------------------
-- 0) Подготовка схемы и расширения
-- -----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS admin_trim;

-- gen_random_uuid() используется для идентификаторов запусков
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- -----------------------------------------------------------------------------------------------------
-- 1) Таблицы для хранения “бэкапов” DDL индексов и inbound FK + журнал запусков
-- -----------------------------------------------------------------------------------------------------

/* Таблица: бэкап DDL вторичных индексов (не PK и не constraint-backed).
   Мы сохраняем исходный CREATE INDEX ... для последующего восстановления. */
CREATE TABLE IF NOT EXISTS admin_trim.index_backup (
  run_id        uuid        NOT NULL,      -- идентификатор запуска
  src_table     regclass    NOT NULL,      -- таблица, с которой снимали индексы
  index_oid     regclass    NOT NULL,      -- OID индекса
  index_name    text        NOT NULL,      -- имя индекса (для удобства чтения)
  indexdef      text        NOT NULL,      -- pg_get_indexdef()
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, index_oid)
);

COMMENT ON TABLE admin_trim.index_backup IS
'Бэкап DDL вторичных индексов (для ускорения массовой вставки: drop -> reload -> recreate).';


/* Таблица: бэкап inbound foreign keys:
   Это внешние ключи в других таблицах, которые ссылаются на целевую (big).
   Их нужно временно удалить, иначе TRUNCATE big не выполнится. */
CREATE TABLE IF NOT EXISTS admin_trim.inbound_fk_backup (
  run_id        uuid        NOT NULL,      -- идентификатор запуска
  target_table  regclass    NOT NULL,      -- таблица-цель (big), на которую ссылаются
  ref_table     regclass    NOT NULL,      -- таблица-источник (которая содержит FK)
  conname       text        NOT NULL,      -- имя constraint
  condef        text        NOT NULL,      -- pg_get_constraintdef(...)
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, target_table, ref_table, conname)
);

COMMENT ON TABLE admin_trim.inbound_fk_backup IS
'Бэкап inbound FK (другие таблицы -> target_table). Используется для временного удаления и восстановления FK.';


/* Журнал запусков: фиксируем параметры, статус, метрики и ошибку. */
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

  -- снимок опций запуска
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
'Журнал запусков процедуры урезания таблицы TRUNCATE+RELOAD.';


-- -----------------------------------------------------------------------------------------------------
-- 2) Хелпер: есть ли валидный индекс на указанной колонке таблицы (простейшая проверка)
-- -----------------------------------------------------------------------------------------------------

/* Проверка “колонка участвует хотя бы в одном индексе”.
   Важно: это не гарантирует, что план реально использует индекс, но защищает от типовых “ой, индекса нет вообще”. */
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


-- -----------------------------------------------------------------------------------------------------
-- 3) Хелпер: checksum по PK (ОЧЕНЬ тяжёлая операция на десятках миллионов строк)
-- -----------------------------------------------------------------------------------------------------

/* Идея: если есть PK, можно посчитать xor от хеша всех значений PK.
   Это не криптография, но хороший “дымовой датчик”: если отличается, значит данные не совпали.
   На 50 млн строк может занять заметное время и дать нагрузку на диски. */
CREATE OR REPLACE FUNCTION admin_trim.pk_checksum(
  p_table regclass,
  p_pk_column name
) RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v bigint;
  sql text;
BEGIN
  sql := format(
    'SELECT COALESCE(bit_xor(hashtextextended((%I)::text, 0)), 0)::bigint FROM %s',
    p_pk_column,
    p_table::text
  );
  EXECUTE sql INTO v;
  RETURN v;
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 4) Бэкап и восстановление вторичных индексов
-- -----------------------------------------------------------------------------------------------------

/* Сохраняем DDL только вторичных индексов:
   - не primary key
   - не индексы, которые стоят за constraint-ами (PK/UNIQUE), чтобы не ломать ограничения */
CREATE OR REPLACE FUNCTION admin_trim.backup_secondary_indexes(
  p_table regclass,
  p_run_id uuid DEFAULT gen_random_uuid()
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT
      i.indexrelid::regclass AS index_oid,
      ic.relname             AS index_name,
      pg_get_indexdef(i.indexrelid) AS indexdef
    FROM pg_index i
    JOIN pg_class ic ON ic.oid = i.indexrelid
    WHERE i.indrelid = p_table
      AND i.indisprimary = false
      AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
    ORDER BY ic.relname
  LOOP
    INSERT INTO admin_trim.index_backup(run_id, src_table, index_oid, index_name, indexdef)
    VALUES (p_run_id, p_table, r.index_oid, r.index_name, r.indexdef)
    ON CONFLICT (run_id, index_oid) DO UPDATE
      SET indexdef = EXCLUDED.indexdef,
          created_at = now();
  END LOOP;

  RETURN p_run_id;
END $$;


/* Удаляем вторичные индексы (те же критерии, что и для бэкапа). */
CREATE OR REPLACE PROCEDURE admin_trim.drop_secondary_indexes(p_table regclass)
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT i.indexrelid::regclass AS idx
    FROM pg_index i
    WHERE i.indrelid = p_table
      AND i.indisprimary = false
      AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
  LOOP
    EXECUTE format('DROP INDEX %s', r.idx::text);
  END LOOP;
END $$;


/* Восстанавливаем вторичные индексы из сохранённого DDL.
   Важно: CREATE INDEX тут без CONCURRENTLY (быстрее, но может блокировать чтение/запись).
   Обычно в окно техработ это нормально. Если нужно CONCURRENTLY — скажи, дам альтернативу. */
CREATE OR REPLACE PROCEDURE admin_trim.create_secondary_indexes_from_backup(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT indexdef
    FROM admin_trim.index_backup
    WHERE run_id = p_run_id
      AND src_table = p_table
    ORDER BY index_name
  LOOP
    EXECUTE r.indexdef;
  END LOOP;
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 5) Бэкап и восстановление inbound FK (другие таблицы -> target_table)
-- -----------------------------------------------------------------------------------------------------

/* Сохраняем все FK, которые ссылаются на p_table */
CREATE OR REPLACE FUNCTION admin_trim.backup_inbound_fks(
  p_table regclass,
  p_run_id uuid DEFAULT gen_random_uuid()
) RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
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
    VALUES (p_run_id, p_table, r.ref_table, r.conname, r.condef)
    ON CONFLICT (run_id, target_table, ref_table, conname) DO UPDATE
      SET condef = EXCLUDED.condef,
          created_at = now();
  END LOOP;

  RETURN p_run_id;
END $$;


/* Удаляем inbound FK по сохранённому списку.
   Это делается, чтобы TRUNCATE target_table не упал на RESTRICT. */
CREATE OR REPLACE PROCEDURE admin_trim.drop_inbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass
)
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT ref_table, conname
    FROM admin_trim.inbound_fk_backup
    WHERE run_id = p_run_id
      AND target_table = p_table
    ORDER BY ref_table::text, conname
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.ref_table::text, r.conname);
  END LOOP;
END $$;


/* Восстанавливаем inbound FK обратно.
   По умолчанию создаём NOT VALID, чтобы не тратить время/блокировки на немедленную проверку.
   Валидацию можно сделать позже отдельной командой VALIDATE CONSTRAINT. */
CREATE OR REPLACE PROCEDURE admin_trim.create_inbound_fks_from_backup(
  p_run_id uuid,
  p_table regclass,
  p_not_valid boolean DEFAULT true
)
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT ref_table, conname, condef
    FROM admin_trim.inbound_fk_backup
    WHERE run_id = p_run_id
      AND target_table = p_table
    ORDER BY ref_table::text, conname
  LOOP
    EXECUTE format(
      'ALTER TABLE %s ADD CONSTRAINT %I %s%s',
      r.ref_table::text,
      r.conname,
      r.condef,
      CASE WHEN p_not_valid THEN ' NOT VALID' ELSE '' END
    );
  END LOOP;
END $$;


/* Валидация всех NOT VALID inbound FK, которые ссылаются на p_table */
CREATE OR REPLACE PROCEDURE admin_trim.validate_inbound_fks(p_table regclass)
LANGUAGE plpgsql
AS $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.conrelid::regclass AS ref_table, c.conname
    FROM pg_constraint c
    WHERE c.contype = 'f'
      AND c.confrelid = p_table
      AND c.convalidated = false
  LOOP
    EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', r.ref_table::text, r.conname);
  END LOOP;
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 6) Управление пользовательскими триггерами (опционально)
-- -----------------------------------------------------------------------------------------------------

/* Отключение/включение только пользовательских триггеров на таблице.
   Системные триггеры (включая FK-триггеры) командой USER не трогаются. */
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


-- -----------------------------------------------------------------------------------------------------
-- 7) Починка sequence/identity по max(pk) (опционально)
-- -----------------------------------------------------------------------------------------------------

/* Для serial/identity-подобных колонок:
   - находим sequence через pg_get_serial_sequence
   - ставим setval = max(col), чтобы следующие insert не словили duplicate key */
CREATE OR REPLACE PROCEDURE admin_trim.fix_sequence_by_max(
  p_table regclass,
  p_column name
)
LANGUAGE plpgsql
AS $$
DECLARE
  seq_name text;
  max_val bigint;
BEGIN
  SELECT pg_get_serial_sequence(p_table::text, p_column::text) INTO seq_name;

  IF seq_name IS NULL THEN
    RAISE NOTICE 'Не найдена sequence для %.% (возможно это не serial/identity колонка)', p_table::text, p_column;
    RETURN;
  END IF;

  EXECUTE format('SELECT max(%I)::bigint FROM %s', p_column, p_table::text) INTO max_val;
  IF max_val IS NULL THEN
    max_val := 0;
  END IF;

  EXECUTE format('SELECT setval(%L, %s, true)', seq_name, max_val);
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 8) Главная процедура: TRUNCATE + RELOAD
-- -----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE admin_trim.trim_by_date_truncate_reload(
  p_table regclass,                       -- целевая таблица (например 'public.big'::regclass)
  p_date_column name,                     -- колонка даты/времени (например 'dt')
  p_from_value timestamptz,               -- начиная с какого значения сохраняем строки

  p_work_schema name DEFAULT 'admin_trim_work',   -- схема для staging таблицы
  p_use_unlogged_staging boolean DEFAULT true,    -- staging UNLOGGED (быстрее, но теряется при crash)

  -- ускорители:
  p_drop_secondary_indexes boolean DEFAULT true,  -- дропнуть вторичные индексы на время reload
  p_disable_user_triggers boolean DEFAULT false,  -- временно отключить пользовательские триггеры на время вставки

  -- inbound FK:
  p_drop_inbound_fks boolean DEFAULT true,                -- временно дропнуть inbound FK чтобы TRUNCATE прошёл
  p_recreate_inbound_fks_not_valid boolean DEFAULT true,  -- восстановить inbound FK как NOT VALID
  p_validate_inbound_fks boolean DEFAULT false,           -- сразу валидировать inbound FK (может быть долго)

  -- sequence/identity fix:
  p_fix_sequence_column name DEFAULT NULL,                -- например 'id'

  -- контроль целостности:
  p_pk_column name DEFAULT NULL,              -- если задан: checksum PK до/после (дорого)
  p_require_date_index boolean DEFAULT true,  -- требовать индекс на p_date_column
  p_allow_date_fullscan boolean DEFAULT false -- разрешить без индекса (не рекомендовано)
)
LANGUAGE plpgsql
AS $$
DECLARE
  src_schema text;
  src_table  text;

  run_id uuid := gen_random_uuid();

  run_idx uuid := gen_random_uuid();
  run_fk  uuid := gen_random_uuid();

  stg_name text;
  stg_reg regclass;

  sql text;

  v_has_date_idx boolean;
  v_src_count bigint;
  v_stg_count bigint;
  v_final_count bigint;

  v_pk_before bigint;
  v_pk_after  bigint;
BEGIN
  -- Определяем схему и имя таблицы
  SELECT n.nspname, c.relname INTO src_schema, src_table
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = p_table;

  -- Проверяем индекс на колонку даты
  v_has_date_idx := admin_trim.has_index_on_column(p_table, p_date_column);

  IF p_require_date_index AND NOT v_has_date_idx AND NOT p_allow_date_fullscan THEN
    RAISE EXCEPTION
      'На таблице % нет валидного индекса по колонке %. Запуск остановлен. Если уверены, включите p_allow_date_fullscan=true.',
      p_table::text, p_date_column;
  END IF;

  -- Создаём рабочую схему для staging
  EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_work_schema);

  -- Пишем старт в журнал
  INSERT INTO admin_trim.run_log(
    run_id, target_table, date_column, from_value, work_schema,
    use_unlogged_staging, drop_secondary_indexes, disable_user_triggers,
    drop_inbound_fks, recreate_inbound_fks_not_valid, validate_inbound_fks,
    fix_sequence_column, pk_column, require_date_index, allow_date_fullscan,
    notes
  ) VALUES (
    run_id, p_table, p_date_column, p_from_value, p_work_schema,
    p_use_unlogged_staging, p_drop_secondary_indexes, p_disable_user_triggers,
    p_drop_inbound_fks, p_recreate_inbound_fks_not_valid, p_validate_inbound_fks,
    p_fix_sequence_column, p_pk_column, p_require_date_index, p_allow_date_fullscan,
    CASE WHEN v_has_date_idx THEN 'индекс по дате: ДА' ELSE 'индекс по дате: НЕТ (override)' END
  );

  -- Метрика: исходный count (это full scan, но обычно приемлемо как метрика; отключите при желании)
  EXECUTE format('SELECT count(*)::bigint FROM %s', p_table::text) INTO v_src_count;
  UPDATE admin_trim.run_log SET src_count = v_src_count WHERE run_id = run_id;

  -- Опционально: checksum по PK ДО (тяжело!)
  IF p_pk_column IS NOT NULL THEN
    v_pk_before := admin_trim.pk_checksum(p_table, p_pk_column);
    UPDATE admin_trim.run_log SET pk_checksum_before = v_pk_before WHERE run_id = run_id;
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 1. Создаём staging и копируем туда только нужный диапазон (делаем ДО блокировок!)
  -- ------------------------------------------------------------------------------------------

  stg_name := left(src_table || '__stg_' || replace(run_id::text, '-', ''), 63);

  EXECUTE format(
    'CREATE %s TABLE %I.%I (LIKE %s INCLUDING ALL)',
    CASE WHEN p_use_unlogged_staging THEN 'UNLOGGED' ELSE '' END,
    p_work_schema, stg_name,
    p_table::text
  );

  stg_reg := format('%I.%I', p_work_schema, stg_name)::regclass;
  UPDATE admin_trim.run_log SET staging_table = stg_reg WHERE run_id = run_id;

  sql := format(
    'INSERT INTO %I.%I SELECT * FROM %s WHERE %I >= $1',
    p_work_schema, stg_name,
    p_table::text,
    p_date_column
  );
  EXECUTE sql USING p_from_value;

  EXECUTE format('ANALYZE %s', stg_reg::text);

  EXECUTE format('SELECT count(*)::bigint FROM %s', stg_reg::text) INTO v_stg_count;
  UPDATE admin_trim.run_log SET staging_count = v_stg_count WHERE run_id = run_id;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 2. Если нужно: бэкап и удаление inbound FK (иначе TRUNCATE может не пройти)
  -- ------------------------------------------------------------------------------------------

  IF p_drop_inbound_fks THEN
    PERFORM admin_trim.backup_inbound_fks(p_table, run_fk);
    CALL admin_trim.drop_inbound_fks_from_backup(run_fk, p_table);
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 3. Если нужно: бэкап вторичных индексов (и далее мы их удалим уже под блокировкой)
  -- ------------------------------------------------------------------------------------------

  IF p_drop_secondary_indexes THEN
    PERFORM admin_trim.backup_secondary_indexes(p_table, run_idx);
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 4. Критическая секция: блокировка + TRUNCATE + вставка обратно
  -- ------------------------------------------------------------------------------------------

  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', p_table::text);

  -- Отключаем пользовательские триггеры (если попросили)
  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, false);
  END IF;

  -- Удаляем вторичные индексы (если попросили)
  IF p_drop_secondary_indexes THEN
    CALL admin_trim.drop_secondary_indexes(p_table);
  END IF;

  -- TRUNCATE основной таблицы
  EXECUTE format('TRUNCATE TABLE %s', p_table::text);

  -- Загружаем данные обратно из staging
  EXECUTE format('INSERT INTO %s SELECT * FROM %s', p_table::text, stg_reg::text);

  -- Возвращаем пользовательские триггеры
  IF p_disable_user_triggers THEN
    CALL admin_trim.set_user_triggers(p_table, true);
  END IF;

  EXECUTE format('ANALYZE %s', p_table::text);

  -- Контроль количества строк после reload
  EXECUTE format('SELECT count(*)::bigint FROM %s', p_table::text) INTO v_final_count;
  UPDATE admin_trim.run_log SET final_count = v_final_count WHERE run_id = run_id;

  IF v_final_count IS DISTINCT FROM v_stg_count THEN
    RAISE EXCEPTION
      'Несовпадение количества строк: staging=% final=% (таблица=%)',
      v_stg_count, v_final_count, p_table::text;
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 5. Восстанавливаем вторичные индексы
  -- ------------------------------------------------------------------------------------------

  IF p_drop_secondary_indexes THEN
    CALL admin_trim.create_secondary_indexes_from_backup(run_idx, p_table);
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 6. Восстанавливаем inbound FK (обычно NOT VALID), и при желании валидируем
  -- ------------------------------------------------------------------------------------------

  IF p_drop_inbound_fks THEN
    CALL admin_trim.create_inbound_fks_from_backup(run_fk, p_table, p_recreate_inbound_fks_not_valid);

    IF p_validate_inbound_fks THEN
      CALL admin_trim.validate_inbound_fks(p_table);
    END IF;
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 7. Починка sequence/identity (если указана колонка)
  -- ------------------------------------------------------------------------------------------

  IF p_fix_sequence_column IS NOT NULL THEN
    CALL admin_trim.fix_sequence_by_max(p_table, p_fix_sequence_column);
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 8. Контроль checksum по PK ПОСЛЕ (если включено)
  -- ------------------------------------------------------------------------------------------

  IF p_pk_column IS NOT NULL THEN
    v_pk_after := admin_trim.pk_checksum(p_table, p_pk_column);
    UPDATE admin_trim.run_log SET pk_checksum_after = v_pk_after WHERE run_id = run_id;

    IF v_pk_after IS DISTINCT FROM v_pk_before THEN
      RAISE EXCEPTION
        'Несовпадение checksum PK: before=% after=% (таблица=%)',
        v_pk_before, v_pk_after, p_table::text;
    END IF;
  END IF;

  -- ------------------------------------------------------------------------------------------
  -- ШАГ 9. Удаляем staging таблицу
  -- ------------------------------------------------------------------------------------------

  EXECUTE format('DROP TABLE %s', stg_reg::text);

  -- Фиксируем успешное завершение
  UPDATE admin_trim.run_log
  SET status='OK', finished_at=now()
  WHERE run_id = run_id;

EXCEPTION WHEN OTHERS THEN
  -- Логируем ошибку
  UPDATE admin_trim.run_log
  SET status='FAILED',
      finished_at=now(),
      error_message = SQLSTATE || ': ' || SQLERRM
  WHERE run_id = run_id;

  -- Пытаемся удалить staging, если он успел создаться
  BEGIN
    IF stg_reg IS NOT NULL THEN
      EXECUTE format('DROP TABLE IF EXISTS %s', stg_reg::text);
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;

  RAISE;
END $$;


-- -----------------------------------------------------------------------------------------------------
-- 9) Примеры вызова (закомментированы)
-- -----------------------------------------------------------------------------------------------------

/*
-- Базовый запуск:
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz
);

-- Отключить пользовательские триггеры на время reload (если допустимо):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_disable_user_triggers => true
);

-- Восстановить inbound FK как NOT VALID и валидировать сразу (может быть долго):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_validate_inbound_fks => true
);

-- Поправить sequence по колонке id:
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_fix_sequence_column => 'id'
);

-- Включить checksum по PK (ОЧЕНЬ тяжело на больших таблицах):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_pk_column => 'id'
);

-- Разрешить запуск без индекса по dt (не рекомендовано):
CALL admin_trim.trim_by_date_truncate_reload(
  'public.big'::regclass,
  'dt',
  '2025-01-01 00:00:00+00'::timestamptz,
  p_allow_date_fullscan => true
);

-- Посмотреть журнал:
SELECT * FROM admin_trim.run_log ORDER BY started_at DESC;
*/
