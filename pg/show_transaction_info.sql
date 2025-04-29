WITH tx_locks AS (
    -- выбираем только записи о блокировках по transactionid
    SELECT
        pid,
        transactionid AS txid
    FROM pg_locks
    WHERE locktype = 'transactionid'
    GROUP BY pid, transactionid
)
SELECT
    t.txid,
    obj.locktype,
    obj.mode,
    obj.granted,
    CASE
        WHEN obj.locktype = 'relation'
            THEN format('table %I', rel.relname)
        WHEN obj.locktype = 'page'
            THEN format('table %I, page %s', rel.relname, obj.page)
        WHEN obj.locktype = 'tuple'
            THEN format('table %I, page %s, tuple %s', rel.relname, obj.page, obj.tuple)
        WHEN obj.locktype = 'object'
            THEN format('catalog %I, row %s', cat.relname, obj.objid)
        ELSE
            obj.locktype
    END AS object_desc
FROM tx_locks t
JOIN pg_locks obj
    ON obj.pid = t.pid
   AND obj.locktype IN ('relation','page','tuple','object')
LEFT JOIN pg_class rel
    ON obj.relation = rel.oid
LEFT JOIN pg_class cat
    ON obj.classid = cat.oid
ORDER BY t.txid, obj.locktype, rel.relname, obj.page, obj.tuple;

/*

Как это работает
CTE tx_locks берёт все процессы (pid), у которых есть блокировка типа transactionid, и вытаскивает уникальные пары (pid, txid).

Во внешнем запросе мы джоинимся по этим pid обратно на pg_locks, но уже только с типами блокировок, относящимися к «реальным» объектам:

relation — вся таблица

page — страница внутри таблицы

tuple — конкретная строка (страница + смещение)

object — строка системного каталога

С помощью LEFT JOIN на pg_class (для relation, page, tuple) и ещё одного LEFT JOIN на pg_class через classid (для object) мы получаем имена таблиц и каталогов.

В CASE … END формируем читаемое описание каждого заблокированного объекта.

В результате вы получите строку на каждую заблокированную сущность, с указанием:

txid — номер транзакции

locktype и mode — тип и режим блокировки (например, ShareLock, RowExclusiveLock и т. д.)

granted — была ли блокировка удовлетворена (true) или ждёт (false)

object_desc — понятный текст вида table customers, table orders, page 42, tuple 3 или catalog pg_proc, row (5,1)

Это позволит увидеть полный список объектов, которые затронула каждая транзакция.
*/
