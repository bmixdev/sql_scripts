--Список сессий удерживающих объекты 
SELECT
    bl.pid       AS blocked_pid,
    ba.usename   AS blocked_user,
    ka.query     AS blocking_query,
    now() - ka.query_start AS blocking_query_age,
    kl.mode      AS blocking_mode,
    kl.locktype,
    n.nspname    AS schema_name,
    c.relname    AS object_name,
    -- кто ждет
    a.query      AS blocked_query,
    now() - a.query_start AS blocked_query_age
FROM pg_locks bl
JOIN pg_stat_activity a  ON a.pid = bl.pid
JOIN pg_locks kl         ON kl.locktype = bl.locktype
                         AND kl.database IS NOT DISTINCT FROM bl.database
                         AND kl.relation IS NOT DISTINCT FROM bl.relation
                         AND kl.page     IS NOT DISTINCT FROM bl.page
                         AND kl.tuple    IS NOT DISTINCT FROM bl.tuple
                         AND kl.virtualxid IS NOT DISTINCT FROM bl.virtualxid
                         AND kl.transactionid IS NOT DISTINCT FROM bl.transactionid
                         AND kl.classid  IS NOT DISTINCT FROM bl.classid
                         AND kl.objid    IS NOT DISTINCT FROM bl.objid
                         AND kl.objsubid IS NOT DISTINCT FROM bl.objsubid
JOIN pg_stat_activity ka ON ka.pid = kl.pid
LEFT JOIN pg_class c      ON c.oid = bl.relation
LEFT JOIN pg_namespace n  ON n.oid = c.relnamespace
WHERE NOT bl.granted        -- ждущий
  AND kl.granted            -- кто держит
  AND ka.state = 'idle in transaction';

--
По конкретному PID:

SELECT
    a.pid,
    a.usename,
    a.state,
    l.locktype,
    l.mode,
    l.granted,
    n.nspname   AS schema_name,
    c.relname   AS object_name,
    c.relkind   AS object_type,  -- r = table, i = index, m = matview, ...
    l.page,
    l.tuple,
    l.virtualxid,
    l.transactionid
FROM pg_stat_activity a
JOIN pg_locks l      ON l.pid = a.pid
LEFT JOIN pg_class c ON c.oid = l.relation
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE a.pid = <PID_ТВОЕЙ_СЕССИИ>
ORDER BY l.granted DESC, l.locktype, l.mode;


Сразу по всем idle in transaction:

SELECT
    a.pid,
    a.usename,
    a.client_addr,
    now() - a.xact_start AS xact_age,
    a.state,
    l.locktype,
    l.mode,
    l.granted,
    n.nspname   AS schema_name,
    c.relname   AS object_name,
    c.relkind   AS object_type,
    a.query
FROM pg_stat_activity a
JOIN pg_locks l      ON l.pid = a.pid
LEFT JOIN pg_class c ON c.oid = l.relation
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE a.state = 'idle in transaction'
ORDER BY xact_age DESC, a.pid, l.locktype, l.mode;


Что тут видно:

locktype = relation → блокируются таблицы/индексы (object_type подскажет что именно:

r table

i index

m materialized view

locktype = tuple → строчные блокировки; по schema_name + object_name поймёшь, в какой таблице.

locktype = transactionid / virtualxid → это уже «клей» между транзакциями, не привязан к конкретной таблице.

locktype = advisory → советующие блокировки, не про таблицы.

--
