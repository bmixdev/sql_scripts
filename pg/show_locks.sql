SELECT
    l.pid,
    l.locktype,
    l.mode,
    l.granted,
    -- Читаемое название заблокированного объекта
    CASE
        WHEN l.locktype = 'relation'
            THEN format('table %I', c_rel.relname)                                        -- табличный объект
        WHEN l.locktype = 'database'
            THEN format('database %I', c_db.datname)                                      -- база данных
        WHEN l.locktype = 'transactionid'
            THEN format('transaction ID %s', l.transactionid)                             -- транзакция по её ID
        WHEN l.locktype = 'virtualxid'
            THEN format('virtual transaction %s', l.virtualxid)                          -- внутренняя виртуальная транзакция
        WHEN l.locktype = 'page'
            THEN format('table %I, page %s', c_rel.relname, l.page)                      -- страница в таблице
        WHEN l.locktype = 'tuple'
            THEN format('table %I, page %s, tuple %s', c_rel.relname, l.page, l.tuple)   -- строка в таблице
        WHEN l.locktype = 'object'
            THEN format('catalog %I, row %s', c_obj.relname, l.objid)                    -- строка системного каталога
        WHEN l.locktype IN ('advisory', 'advisory_xact')
            THEN format(
                'advisory %s%s',
                l.objid,
                COALESCE(':' || l.objsubid, '')
            )                                                                             -- пользовательский (advisory) ключ
        ELSE
            'unknown'
    END AS locked_object
FROM pg_locks AS l
LEFT JOIN pg_class    AS c_rel  ON l.relation   = c_rel.oid   AND l.locktype IN ('relation','page','tuple')
LEFT JOIN pg_database AS c_db   ON l.database   = c_db.oid    AND l.locktype = 'database'
LEFT JOIN pg_class    AS c_obj  ON l.classid    = c_obj.oid    AND l.locktype = 'object'
ORDER BY l.locktype, l.pid;

/*

relation — блокировка на уровне всей таблицы (pg_class.oid = l.relation)

page — блокировка конкретной страницы внутри таблицы (номер страницы в поле l.page)

tuple — блокировка конкретной строки (CTID): страница + смещение в странице (l.page, l.tuple)

database — блокировка всей базы данных (pg_database.oid = l.database)

transactionid — блокировка по внутреннему идентификатору завершённой/текущей транзакции

virtualxid — блокировка по виртуальному XID (используется для частичных/спин-операций)

object — блокировка строки в любом системном каталоге (по classid → pg_class.oid, objid → ctid строки)

advisory, advisory_xact — пользовательские ключи (l.objid, l.objsubid)
*/
