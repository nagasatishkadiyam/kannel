#include "gwlib/gwlib.h"
#ifdef HAVE_REDIS
#include "gwlib/dbpool.h"
#include "hiredis.h"
#define sqlbox_redis_c
#include "jansson.h"
#include "sqlbox_redis.h"

#define sql_update redis_update
#define sql_select redis_select

static Octstr* sqlbox_logtable;
static Octstr* sqlbox_insert_table;
static Octstr* sqlbox_inflight_table;
static Octstr* boxc_id;

/*
 * Our connection pool to redis.
 */

static DBPool* pool = NULL;

/*
 * Atomically take up to ARGV[1] elements from list KEYS[1] (LRANGE left head + LTRIM).
 * Without this script, concurrent sqlboxes each run LLEN/LRANGE/LTRIM and duplicate messages.
 */
static const char sqlbox_redis_batch_pop_script[] =
    "local n = redis.call('LLEN', KEYS[1]) "
    "if n <= 0 then return {} end "
    "local limit = tonumber(ARGV[1]) or 0 "
    "local pick = (limit > 0 and n > limit) and limit or n "
    "if pick <= 0 then return {} end "
    "local elems = redis.call('LRANGE', KEYS[1], 0, pick - 1) "
    "redis.call('LTRIM', KEYS[1], pick, -1) "
    "return elems";

static void redis_update(const Octstr* sql, const Octstr* data)
{
    redisReply* reply;
    DBPoolConn* pc;

#if defined(SQLBOX_TRACE)
    debug("SQLBOX", 0, "sql: %s", octstr_get_cstr(sql));
    debug("SQLBOX", 0, "data: %s", octstr_get_cstr(data));
#endif

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "REDIS: Database pool got no connection! DB update failed!");
        return;
    }

    if (data == NULL) {
        reply = redisCommand(pc->conn, octstr_get_cstr(sql));
    } else {
        reply = redisCommand(pc->conn, octstr_get_cstr(sql), octstr_get_cstr(data));
    }

    if (reply != NULL) {
        if (reply->type == REDIS_REPLY_ERROR) {
            error(0, "REDIS: redisCommand() failed: %s", reply->str);
        }
        freeReplyObject(reply);
    }
    dbpool_conn_produce(pc);
}

static redisReply* redis_select(const Octstr* sql)
{
    redisReply* reply;
    DBPoolConn* pc;

#if defined(SQLBOX_TRACE)
    debug("SQLBOX", 0, "sql: %s", octstr_get_cstr(sql));
#endif

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "REDIS: Database pool got no connection! DB update failed!");
        return NULL;
    }

    reply = redisCommand(pc->conn, octstr_get_cstr(sql));
    if (reply != NULL && reply->type == REDIS_REPLY_ERROR) {
        error(0, "REDIS: %s", reply->str);
    }

    dbpool_conn_produce(pc);

    return reply;
}

static void* gw_malloc_json(size_t size)
{
    return gw_malloc(size);
}

static void gw_free_json(void* ptr)
{
    return gw_free(ptr);
}

void sqlbox_configure_redis(Cfg* cfg)
{
    CfgGroup* grp;

    if (!(grp = cfg_get_single_group(cfg, octstr_imm("sqlbox"))))
        panic(0, "SQLBOX: Redis: group 'sqlbox' is not specified!");

    sqlbox_logtable = cfg_get(grp, octstr_imm("sql-log-table"));
    if (sqlbox_logtable == NULL) {
        panic(0, "'sql-log-table' is not configured in the group 'sqlbox'.");
    }
    sqlbox_insert_table = cfg_get(grp, octstr_imm("sql-insert-table"));
    if (sqlbox_insert_table == NULL) {
        panic(0, "'sql-insert-table' is not configured in the group 'sqlbox'.");
    }
    sqlbox_inflight_table = cfg_get(grp, octstr_imm("sql-inflight-table"));

    json_set_alloc_funcs(gw_malloc_json, gw_free_json);

    boxc_id = cfg_get(grp, octstr_imm("smsbox-id"));
    if (boxc_id == NULL)
        boxc_id = cfg_get(grp, octstr_imm("id"));

    /* no need to create tables on redis */
}

/* Extract long from JSON - handles integer, real, string, null. Returns -1 for null/undefined (matches MySQL atol_null). */
static long json_get_long(json_t *obj, const char *key)
{
    json_t *val = json_object_get(obj, key);
    if (val == NULL || json_is_null(val))
        return -1;
    if (json_is_integer(val))
        return (long)json_integer_value(val);
    if (json_is_real(val))
        return (long)json_real_value(val);
    if (json_is_string(val)) {
        const char *s = json_string_value(val);
        if (s == NULL || s[0] == '\0' || strcmp(s, "NULL") == 0)
            return -1;
        return atol(s);
    }
    return -1;
}

/* Extract Octstr from JSON - handles string, integer, real, null. Returns octstr_create("") for null (matches MySQL octstr_null_create). */
static Octstr* json_get_octstr(json_t *obj, const char *key)
{
    json_t *val = json_object_get(obj, key);
    if (val == NULL || json_is_null(val))
        return octstr_create("");
    if (json_is_string(val)) {
        const char *s = json_string_value(val);
        if (s == NULL || strcmp(s, "NULL") == 0)
            return octstr_create("");
        return octstr_create(s);
    }
    if (json_is_integer(val))
        return octstr_format("%ld", (long)json_integer_value(val));
    if (json_is_real(val))
        return octstr_format("%g", json_real_value(val));
    return octstr_create("");
}

Msg* redis_fetch_msg()
{
    Msg* msg = NULL;
    Octstr *sql, *delet, *subst, *jsonstr;
    redisReply* res = NULL;
    char* resjson;
    json_t* root;
    json_t* jsonmsg;
    json_error_t error;

    info(0, "REDIS: fetching message from %s", octstr_get_cstr(sqlbox_insert_table));

    if (sqlbox_inflight_table != NULL) {
        sql = octstr_format(SQLBOX_REDIS_QUEUE_POP_WITH_INFLIGHT, sqlbox_insert_table, sqlbox_inflight_table);
    } else {
        sql = octstr_format(SQLBOX_REDIS_QUEUE_POP, sqlbox_insert_table);
    }

    res = sql_select(sql);
    if (res == NULL) {
        octstr_destroy(sql);
        return NULL;
    }

    if (res->type == REDIS_REPLY_ARRAY) {
        resjson = res->element[1]->str; /* In-flight usage (BRPOPLPUSH) returns this */
    } else if (res->type == REDIS_REPLY_STRING) { /* Non in-flight usage (BRPOP) returns this */
        resjson = res->str;
    } else if (res->type == REDIS_REPLY_NIL) { /* No messages queued - loop */
        freeReplyObject(res);
        return NULL;
    } else if (res->type == REDIS_REPLY_ERROR) {
        warning(0, "REDIS command %s failed with error %s", octstr_get_cstr(sql), res->str);
        freeReplyObject(res);
        return NULL;
    } else {
        warning(0, "REDIS command %s return unknown status", octstr_get_cstr(sql));
        freeReplyObject(res);
        return NULL;
    }

    root = json_loads(resjson, 0, &error);
    if (!root) {
        warning(0, "sqlbox: Invalid JSON in message retrieved from Redis. Skipping message.");
        freeReplyObject(res);
        return NULL;
    }

    jsonmsg = json_object_get(root, "msg");
    if (!json_is_object(jsonmsg)) {
        warning(0, "sqlbox: JSON does not include 'msg' root element. Skipping message.");
        json_decref(root);
        freeReplyObject(res);
        return NULL;
    }

    msg = redis_create_msg(jsonmsg);

    /* delete from inflight table. This shoudl really be done after the message has been queued to bearerbox */
    if (sqlbox_inflight_table != NULL) {
        subst = octstr_create("%s");
        delet = octstr_format(SQLBOX_REDIS_DELETE, sqlbox_inflight_table, subst);
        jsonstr = octstr_create(resjson);
        sql_update(delet, jsonstr);
        octstr_destroy(delet);
        octstr_destroy(jsonstr);
    }

    json_decref(root);
    freeReplyObject(res);
    octstr_destroy(sql);
    return msg;
}

int redis_fetch_msg_list(List* qlist, long limit)
{
    Msg* msg = NULL;
    redisReply* res = NULL;
    char* resjson;
    json_t *root, *jsonmsg;
    json_error_t json_error;
    DBPoolConn* pc;
    int j;
    int k = 0;

    if (limit <= 0)
        return 0;

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "REDIS: Database pool got no connection! DB update failed!");
        return 0;
    }

    res = redisCommand(pc->conn, "EVAL %s 1 %s %ld",
                       sqlbox_redis_batch_pop_script,
                       octstr_get_cstr(sqlbox_insert_table),
                       limit);
    if (res == NULL) {
        dbpool_conn_produce(pc);
        return 0;
    }

    if (res->type == REDIS_REPLY_ERROR) {
        error(0, "REDIS: %s", res->str);
        freeReplyObject(res);
        dbpool_conn_produce(pc);
        return 0;
    }

    if (res->type != REDIS_REPLY_ARRAY) {
        freeReplyObject(res);
        dbpool_conn_produce(pc);
        return 0;
    }

    if (res->elements == 0) {
        freeReplyObject(res);
        dbpool_conn_produce(pc);
        return 0;
    }

    gwlist_add_producer(qlist);
    for (j = 0; j < res->elements; j++) {
        resjson = res->element[j]->str;
        root = json_loads(resjson, 0, &json_error);
        if (!root) {
            warning(0, "sqlbox: Invalid JSON in message retrieved from Redis. Skipping message.");
            continue;
        }

        jsonmsg = json_object_get(root, "msg");
        if (!json_is_object(jsonmsg)) {
            warning(0, "sqlbox: JSON does not include 'msg' root element. Skipping message.");
            json_decref(root);
            continue;
        }

        msg = redis_create_msg(jsonmsg);
        gwlist_produce(qlist, msg);
        json_decref(root);
        k++;
    }

    gwlist_remove_producer(qlist);
    dbpool_conn_produce(pc);
    freeReplyObject(res);

    return k;
}

Msg* redis_create_msg(json_t* jsonmsg)
{
    Octstr *boxc_val;
    /* save fields in this row as msg struct - use type-aware extraction to match MySQL */
    Msg* msg = msg_create(sms);
    msg->sms.foreign_id = json_get_octstr(jsonmsg, "foreign_id");
    msg->sms.sender = json_get_octstr(jsonmsg, "sender");
    msg->sms.receiver = json_get_octstr(jsonmsg, "receiver");
    msg->sms.udhdata = json_get_octstr(jsonmsg, "udhdata");
    msg->sms.msgdata = json_get_octstr(jsonmsg, "msgdata");
    msg->sms.time = json_get_long(jsonmsg, "time");
    msg->sms.smsc_id = json_get_octstr(jsonmsg, "smsc_id");
    msg->sms.service = json_get_octstr(jsonmsg, "service");
    msg->sms.account = json_get_octstr(jsonmsg, "account");
    msg->sms.sms_type = json_get_long(jsonmsg, "sms_type");
    msg->sms.mclass = json_get_long(jsonmsg, "mclass");
    msg->sms.mwi = json_get_long(jsonmsg, "mwi");
    msg->sms.coding = json_get_long(jsonmsg, "coding");
    msg->sms.compress = json_get_long(jsonmsg, "compress");
    msg->sms.validity = json_get_long(jsonmsg, "validity");
    msg->sms.deferred = json_get_long(jsonmsg, "deferred");
    msg->sms.dlr_mask = json_get_long(jsonmsg, "dlr_mask");
    /* When undefined, default to requesting DLRs (31) to match MySQL behavior when row has NULL */
    if (msg->sms.dlr_mask == -1)
        msg->sms.dlr_mask = 31;
    msg->sms.dlr_url = json_get_octstr(jsonmsg, "dlr_url");
    msg->sms.pid = json_get_long(jsonmsg, "pid");
    msg->sms.alt_dcs = json_get_long(jsonmsg, "alt_dcs");
    msg->sms.rpi = json_get_long(jsonmsg, "rpi");
    msg->sms.charset = json_get_octstr(jsonmsg, "charset");
    msg->sms.binfo = json_get_octstr(jsonmsg, "binfo");
    msg->sms.priority = json_get_long(jsonmsg, "priority");
    msg->sms.meta_data = json_get_octstr(jsonmsg, "meta_data");

    boxc_val = json_get_octstr(jsonmsg, "boxc_id");
    if (octstr_len(boxc_val) == 0 && boxc_id != NULL) {
        octstr_destroy(boxc_val);
        msg->sms.boxc_id = octstr_duplicate(boxc_id);
    } else {
        msg->sms.boxc_id = boxc_val;
    }

    return msg;
}

void redis_save_msg(Msg* msg, Octstr* momt /*, Octstr smsbox_id */)
{
    Octstr *sql, *jsonstr, *subst;

    jsonstr = redis_save_msg_create(msg, momt);
    subst = octstr_create("%s");
    if (sqlbox_logtable == NULL) {
       sqlbox_logtable = octstr_imm("sent_sms");
    }

    sql = octstr_format(SQLBOX_REDIS_QUEUE_PUSH, sqlbox_logtable, subst);
    sql_update(sql, jsonstr);
    octstr_destroy(sql);
    octstr_destroy(subst);
    octstr_destroy(jsonstr);
}

/* save a list of messages and delete them from the insert table */
void redis_save_list(List* qlist, Octstr* momt, int save_mt)
{
    Msg* msg;

    while (gwlist_len(qlist) > 0 && (msg = gwlist_consume(qlist)) != NULL) {
        if (save_mt) {
            redis_save_msg(msg, momt);
        }
    }
}

/* Set JSON value: string for Octstr, integer for long (matches MySQL VARCHAR vs BIGINT) */
static void json_set_str(json_t *obj, const char *key, Octstr *val)
{
    if (val == NULL || octstr_len(val) == 0)
        json_object_set_new(obj, key, json_string(""));
    else
        json_object_set_new(obj, key, json_string(octstr_get_cstr(val)));
}
static void json_set_long(json_t *obj, const char *key, long val)
{
    if (val == -1)
        json_object_set_new(obj, key, json_null());
    else
        json_object_set_new(obj, key, json_integer(val));
}

Octstr* redis_save_msg_create(Msg* msg, Octstr* momt)
{
    json_t *msgjson, *root;
    Octstr* jsonstr;
    char* json;

    msgjson = json_object();
    json_set_str(msgjson, "momt", momt);
    json_set_str(msgjson, "sender", msg->sms.sender);
    json_set_str(msgjson, "receiver", msg->sms.receiver);
    json_set_str(msgjson, "foreign_id", msg->sms.foreign_id);
    json_set_str(msgjson, "udhdata", msg->sms.udhdata);
    json_set_str(msgjson, "msgdata", msg->sms.msgdata);
    json_set_long(msgjson, "time", msg->sms.time);
    json_set_str(msgjson, "smsc_id", msg->sms.smsc_id);
    json_set_str(msgjson, "service", msg->sms.service);
    json_set_str(msgjson, "account", msg->sms.account);
    json_set_long(msgjson, "sms_type", msg->sms.sms_type);
    json_set_long(msgjson, "mclass", msg->sms.mclass);
    json_set_long(msgjson, "mwi", msg->sms.mwi);
    json_set_long(msgjson, "coding", msg->sms.coding);
    json_set_long(msgjson, "compress", msg->sms.compress);
    json_set_long(msgjson, "validity", msg->sms.validity);
    json_set_long(msgjson, "deferred", msg->sms.deferred);
    json_set_long(msgjson, "dlr_mask", msg->sms.dlr_mask);
    json_set_str(msgjson, "dlr_url", msg->sms.dlr_url);
    json_set_long(msgjson, "pid", msg->sms.pid);
    json_set_long(msgjson, "alt_dcs", msg->sms.alt_dcs);
    json_set_long(msgjson, "rpi", msg->sms.rpi);
    json_set_str(msgjson, "charset", msg->sms.charset);
    json_set_str(msgjson, "boxc_id", msg->sms.boxc_id);
    json_set_str(msgjson, "binfo", msg->sms.binfo);
    json_set_long(msgjson, "priority", msg->sms.priority);
    json_set_str(msgjson, "meta_data", msg->sms.meta_data);

    root = json_object();
    json_object_set(root, "msg", msgjson);
    json = json_dumps(root, JSON_COMPACT);

    jsonstr = octstr_create(json);
    json_decref(msgjson);
    json_decref(root);

    gw_free(json);

    return jsonstr;
}

void redis_leave()
{
    dbpool_destroy(pool);
}

struct server_type* sqlbox_init_redis(Cfg* cfg)
{
    CfgGroup* grp;
    List* grplist;
    Octstr *redis_host, *redis_password, *redis_id;
    Octstr* p = NULL;
    long pool_size, redis_port = 0, redis_database = -1, redis_idle_timeout = -1;
    DBConf* db_conf = NULL;
    struct server_type* res = NULL;

    /*
     * check for sqlbox group and get the connection id (same as mysql/pgsql)
     */
    if (!(grp = cfg_get_single_group(cfg, octstr_imm("sqlbox"))))
        panic(0, "SQLBOX: Redis: group 'sqlbox' is not specified!");

    if (!(redis_id = cfg_get(grp, octstr_imm("id"))))
        return NULL;

    /*
     * now grab the required information from the 'redis-connection' group
     * with the redis-id we just obtained
     *
     * we have to loop through all available Redis connection definitions
     * and search for the one we are looking for
     */

    grplist = cfg_get_multi_group(cfg, octstr_imm("redis-connection"));
    if (!grplist)
        return NULL;

    while ((grp = (CfgGroup*)gwlist_extract_first(grplist)) != NULL) {
        p = cfg_get(grp, octstr_imm("id"));
        if (p != NULL && octstr_compare(p, redis_id) == 0) {
            goto found;
        }
        if (p != NULL)
            octstr_destroy(p);
    }

    octstr_destroy(redis_id);
    gwlist_destroy(grplist, NULL);
    return NULL;

found:
    octstr_destroy(p);
    gwlist_destroy(grplist, NULL);

    if (cfg_get_integer(&pool_size, grp, octstr_imm("max-connections")) == -1 || pool_size == 0)
        pool_size = 1;

    if (!(redis_host = cfg_get(grp, octstr_imm("host"))))
        panic(0, "SQLBOX: Redis: directive 'host' is not specified!");
    if (cfg_get_integer(&redis_port, grp, octstr_imm("port")) == -1)
        panic(0, "SQLBOX: Redis: directive 'port' is not specified!");
    redis_password = cfg_get(grp, octstr_imm("password"));
    cfg_get_integer(&redis_database, grp, octstr_imm("database"));
    cfg_get_integer(&redis_idle_timeout, grp, octstr_imm("idle-timeout"));

    /*
     * ok, ready to connect to Redis
     */
    db_conf = gw_malloc(sizeof(DBConf));
    gw_assert(db_conf != NULL);

    db_conf->redis = gw_malloc(sizeof(RedisConf));
    gw_assert(db_conf->redis != NULL);

    db_conf->redis->host = redis_host;
    db_conf->redis->port = redis_port;
    db_conf->redis->password = redis_password;
    db_conf->redis->database = redis_database;
    db_conf->redis->idle_timeout = redis_idle_timeout;

    pool = dbpool_create(DBPOOL_REDIS, db_conf, pool_size);
    gw_assert(pool != NULL);

    /*
     * XXX should a failing connect throw panic?!
     */
    if (dbpool_conn_count(pool) == 0)
        panic(0, "SQLBOX: Redis: database pool has no connections!");

    octstr_destroy(redis_id);

    res = gw_malloc(sizeof(struct server_type));
    gw_assert(res != NULL);

    res->type = octstr_create("Redis");
    res->sql_enter = sqlbox_configure_redis;
    res->sql_leave = redis_leave;
    res->sql_fetch_msg = redis_fetch_msg;
    res->sql_save_msg = redis_save_msg;
    res->sql_fetch_msg_list = redis_fetch_msg_list;
    res->sql_save_list = redis_save_list;
    return res;
}
#endif
