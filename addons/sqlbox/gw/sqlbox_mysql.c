#include "gwlib/gwlib.h"
#ifdef HAVE_MYSQL
#include "gwlib/dbpool.h"
#include <mysql.h>
#include "sqlbox_mysql.h"

#define sql_update mysql_update
#define sql_select mysql_select
#define MYSQL_ERR_NOSUCHFIELD 1054

static Octstr *sqlbox_logtable;
static Octstr *sqlbox_insert_table;

/*
 * Our connection pool to mysql.
 */

static DBPool *pool = NULL;

static void mysql_update(const Octstr *sql)
{
    int state;
    DBPoolConn *pc;

#if defined(SQLBOX_TRACE)
     debug("SQLBOX", 0, "sql: %s", octstr_get_cstr(sql));
#endif

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "MYSQL: Database pool got no connection! DB update failed!");
        return;
    }

    state = mysql_query(pc->conn, octstr_get_cstr(sql));
    if (state != 0)
        error(0, "MYSQL: %s", mysql_error(pc->conn));
        if (mysql_errno(pc->conn) == MYSQL_ERR_NOSUCHFIELD) {
            error(0, "Try to recreate insert and log tables. The structure may have changed. See ChangeLog.");
        }

    dbpool_conn_produce(pc);
}

static MYSQL_RES* mysql_select(const Octstr *sql)
{
    int state;
    MYSQL_RES *result = NULL;
    DBPoolConn *pc;

#if defined(SQLBOX_TRACE)
    debug("SQLBOX", 0, "sql: %s", octstr_get_cstr(sql));
#endif

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "MYSQL: Database pool got no connection! DB update failed!");
        return NULL;
    }

    state = mysql_query(pc->conn, octstr_get_cstr(sql));
    if (state != 0) {
        error(0, "MYSQL: %s", mysql_error(pc->conn));
        if (mysql_errno(pc->conn) == MYSQL_ERR_NOSUCHFIELD) {
            error(0, "Try to recreate insert and log tables. The structure may have changed. See ChangeLog.");
        }
    } else {
        result = mysql_store_result(pc->conn);
    }

    dbpool_conn_produce(pc);

    return result;
}

void sqlbox_configure_mysql(Cfg* cfg)
{
    CfgGroup *grp;
    Octstr *sql;

    if (!(grp = cfg_get_single_group(cfg, octstr_imm("sqlbox"))))
        panic(0, "SQLBOX: MySQL: group 'sqlbox' is not specified!");

    sqlbox_logtable = cfg_get(grp, octstr_imm("sql-log-table"));
    if (sqlbox_logtable == NULL) {
        panic(0, "No 'sql-log-table' not configured.");
    }
    sqlbox_insert_table = cfg_get(grp, octstr_imm("sql-insert-table"));
    if (sqlbox_insert_table == NULL) {
        panic(0, "No 'sql-insert-table' not configured.");
    }

    /* create send_sms && sent_sms tables if they do not exist */
    sql = octstr_format(SQLBOX_MYSQL_CREATE_LOG_TABLE, sqlbox_logtable);
    sql_update(sql);
    octstr_destroy(sql);
    sql = octstr_format(SQLBOX_MYSQL_CREATE_INSERT_TABLE, sqlbox_insert_table);
    sql_update(sql);
    octstr_destroy(sql);
    /* end table creation */
}

#define octstr_null_create(x) ((x != NULL) ? octstr_create(x) : octstr_create(""))
#define atol_null(x) ((x != NULL) ? atol(x) : -1)

/* Map one MySQL row into a Msg (foreign_id holds sql_id). */
static Msg *mysql_msg_from_row(MYSQL_ROW row)
{
    Msg *msg;

    msg = msg_create(sms);
    /* we abuse the foreign_id field in the message struct for our sql_id value */
    msg->sms.foreign_id = octstr_null_create(row[0]);
    msg->sms.sender = octstr_null_create(row[2]);
    msg->sms.receiver = octstr_null_create(row[3]);
    msg->sms.udhdata = octstr_null_create(row[4]);
    msg->sms.msgdata = octstr_null_create(row[5]);
    msg->sms.time = atol_null(row[6]);
    msg->sms.smsc_id = octstr_null_create(row[7]);
    msg->sms.service = octstr_null_create(row[8]);
    msg->sms.account = octstr_null_create(row[9]);
    msg->sms.sms_type = atol_null(row[11]);
    msg->sms.mclass = atol_null(row[12]);
    msg->sms.mwi = atol_null(row[13]);
    msg->sms.coding = atol_null(row[14]);
    msg->sms.compress = atol_null(row[15]);
    msg->sms.validity = atol_null(row[16]);
    msg->sms.deferred = atol_null(row[17]);
    msg->sms.dlr_mask = atol_null(row[18]);
    msg->sms.dlr_url = octstr_null_create(row[19]);
    msg->sms.pid = atol_null(row[20]);
    msg->sms.alt_dcs = atol_null(row[21]);
    msg->sms.rpi = atol_null(row[22]);
    msg->sms.charset = octstr_null_create(row[23]);
    msg->sms.binfo = octstr_null_create(row[25]);
    msg->sms.meta_data = octstr_null_create(row[26]);
    msg->sms.priority = atol_null(row[27]);
    if (row[24] == NULL) {
        msg->sms.boxc_id = octstr_duplicate(sqlbox_id);
    }
    else {
        msg->sms.boxc_id = octstr_null_create(row[24]);
    }
    return msg;
}

/*
 * Single connection: BEGIN; SELECT … FOR UPDATE; DELETE; COMMIT keeps row claim atomic
 * for multiple sqlboxes sharing send_sms.
 */
Msg *mysql_fetch_msg()
{
    Msg *msg = NULL;
    Octstr *sql = NULL;
    Octstr *delet = NULL;
    MYSQL_RES *res = NULL;
    MYSQL_ROW row;
    MYSQL *conn;
    DBPoolConn *pc;

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "MYSQL: Database pool got no connection! DB update failed!");
        return NULL;
    }
    conn = pc->conn;

    sql = octstr_format(SQLBOX_MYSQL_SELECT_QUERY, sqlbox_insert_table);

    mysql_autocommit(conn, 0);
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        error(0, "MYSQL START TRANSACTION: %s", mysql_error(conn));
        goto fail_tx_early;
    }
    if (mysql_query(conn, octstr_get_cstr(sql)) != 0) {
        error(0, "MYSQL: %s", mysql_error(conn));
        if (mysql_errno(conn) == MYSQL_ERR_NOSUCHFIELD) {
            error(0, "Try to recreate insert and log tables. The structure may have changed. See ChangeLog.");
        }
        goto fail_tx;
    }
    octstr_destroy(sql);
    sql = NULL;

    res = mysql_store_result(conn);
    if (res == NULL) {
        debug("sqlbox", 0, "SQL statement mysql_store_result failed: %s", mysql_error(conn));
        goto fail_tx;
    }
    if (mysql_num_rows(res) < 1) {
        mysql_free_result(res);
        res = NULL;
        mysql_query(conn, "COMMIT");
        mysql_autocommit(conn, 1);
        dbpool_conn_produce(pc);
        return NULL;
    }

    row = mysql_fetch_row(res);
    msg = mysql_msg_from_row(row);
    mysql_free_result(res);
    res = NULL;

    delet = octstr_format(SQLBOX_MYSQL_DELETE_QUERY, sqlbox_insert_table, msg->sms.foreign_id);
#if defined(SQLBOX_TRACE)
    debug("SQLBOX", 0, "sql: %s", octstr_get_cstr(delet));
#endif
    if (mysql_query(conn, octstr_get_cstr(delet)) != 0) {
        error(0, "MYSQL (DELETE/fetch-msg): %s", mysql_error(conn));
        msg_destroy(msg);
        msg = NULL;
        octstr_destroy(delet);
        delet = NULL;
        goto fail_tx;
    }
    octstr_destroy(delet);

    mysql_query(conn, "COMMIT");
    mysql_autocommit(conn, 1);
    dbpool_conn_produce(pc);
    return msg;

fail_tx:
    mysql_query(conn, "ROLLBACK");
fail_tx_early:
    mysql_autocommit(conn, 1);
    if (res != NULL)
        mysql_free_result(res);
    octstr_destroy(sql);
    octstr_destroy(delet);
    if (msg != NULL) {
        msg_destroy(msg);
        msg = NULL;
    }
    dbpool_conn_produce(pc);
    return NULL;
}

int mysql_fetch_msg_list(List *qlist, long limit)
{
    Msg *msg = NULL;
    Octstr *sql = NULL;
    Octstr *delet = NULL;
    Octstr *ids = NULL;
    MYSQL_RES *res = NULL;
    MYSQL_ROW row;
    MYSQL *conn;
    DBPoolConn *pc;
    int ret = 0;
    int first_id = 1;

    if (limit <= 0)
        return 0;

    pc = dbpool_conn_consume(pool);
    if (pc == NULL) {
        error(0, "MYSQL: Database pool got no connection! DB update failed!");
        return 0;
    }
    conn = pc->conn;

    mysql_autocommit(conn, 0);
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        error(0, "MYSQL START TRANSACTION (batch): %s", mysql_error(conn));
        goto fail_early;
    }

    sql = octstr_format(SQLBOX_MYSQL_SELECT_LIST_QUERY, sqlbox_insert_table, limit);
    if (mysql_query(conn, octstr_get_cstr(sql)) != 0) {
        error(0, "MYSQL: %s", mysql_error(conn));
        if (mysql_errno(conn) == MYSQL_ERR_NOSUCHFIELD) {
            error(0, "Try to recreate insert and log tables. The structure may have changed. See ChangeLog.");
        }
        goto fail_tx;
    }
    octstr_destroy(sql);
    sql = NULL;

    res = mysql_store_result(conn);
    if (res == NULL) {
        debug("sqlbox", 0, "SQL statement mysql_store_result failed: %s", mysql_error(conn));
        goto fail_tx;
    }

    ret = mysql_num_rows(res);
    if (ret < 1) {
        mysql_free_result(res);
        res = NULL;
        mysql_query(conn, "COMMIT");
        mysql_autocommit(conn, 1);
        dbpool_conn_produce(pc);
        return 0;
    }

    ids = octstr_create("");
    gwlist_add_producer(qlist);

    while ((row = mysql_fetch_row(res)) != NULL) {
        if (row[0] != NULL) {
            if (!first_id)
                octstr_append_char(ids, ',');
            octstr_append_cstr(ids, row[0]);
            first_id = 0;
        }
        msg = mysql_msg_from_row(row);
        gwlist_produce(qlist, msg);
    }
    mysql_free_result(res);
    res = NULL;

    if (octstr_len(ids) < 1) {
        error(0, "MYSQL (batch): missing sql_id on queued row(s); rolling back");
        octstr_destroy(ids);
        goto fail_batch_msgs;
    }

    delet = octstr_format(SQLBOX_MYSQL_DELETE_LIST_QUERY, sqlbox_insert_table, ids);
    octstr_destroy(ids);
    ids = NULL;

    if (mysql_query(conn, octstr_get_cstr(delet)) != 0) {
        error(0, "MYSQL (DELETE/batch): %s", mysql_error(conn));
        octstr_destroy(delet);
        delet = NULL;
        goto fail_batch_msgs;
    }
    octstr_destroy(delet);

    mysql_query(conn, "COMMIT");
    mysql_autocommit(conn, 1);
    gwlist_remove_producer(qlist);
    dbpool_conn_produce(pc);
    return ret;

fail_batch_msgs:
    mysql_query(conn, "ROLLBACK");
    while (gwlist_len(qlist) > 0 && (msg = gwlist_consume(qlist)) != NULL)
        msg_destroy(msg);
    gwlist_remove_producer(qlist);
    mysql_autocommit(conn, 1);
    dbpool_conn_produce(pc);
    return 0;

fail_tx:
    mysql_query(conn, "ROLLBACK");
fail_early:
    mysql_autocommit(conn, 1);
    if (res != NULL)
        mysql_free_result(res);
    octstr_destroy(sql);
    octstr_destroy(ids);
    octstr_destroy(delet);
    dbpool_conn_produce(pc);
    return 0;
}

static Octstr *get_numeric_value_or_return_null(long int num)
{
    if (num == -1) {
        return octstr_create("NULL");
    }
    return octstr_format("%ld", num);
}

static Octstr *get_string_value_or_return_null(Octstr *str)
{
    if (str == NULL) {
        return octstr_create("NULL");
    }
    if (octstr_compare(str, octstr_imm("")) == 0) {
        return octstr_create("NULL");
    }
    /* todo: create a new string instead of inline replacing */
    octstr_replace(str, octstr_imm("\\"), octstr_imm("\\\\"));
    octstr_replace(str, octstr_imm("\'"), octstr_imm("\\\'"));
    return octstr_format("\'%S\'", str);
}

#define st_num(x) (stuffer[stuffcount++] = get_numeric_value_or_return_null(x))
#define st_str(x) (stuffer[stuffcount++] = get_string_value_or_return_null(x))

void mysql_save_msg(Msg *msg, Octstr *momt)
{
    Octstr *sql;
    Octstr *stuffer[30];
    int stuffcount = 0;

    sql = octstr_format(SQLBOX_MYSQL_INSERT_QUERY, sqlbox_logtable, st_str(momt), st_str(msg->sms.sender),
        st_str(msg->sms.receiver), st_str(msg->sms.udhdata), st_str(msg->sms.msgdata), st_num(msg->sms.time),
        st_str(msg->sms.smsc_id), st_str(msg->sms.service), st_str(msg->sms.account), st_num(msg->sms.sms_type),
        st_num(msg->sms.mclass), st_num(msg->sms.mwi), st_num(msg->sms.coding), st_num(msg->sms.compress),
        st_num(msg->sms.validity), st_num(msg->sms.deferred), st_num(msg->sms.dlr_mask), st_str(msg->sms.dlr_url),
        st_num(msg->sms.pid), st_num(msg->sms.alt_dcs), st_num(msg->sms.rpi), st_str(msg->sms.charset),
        st_str(msg->sms.boxc_id), st_str(msg->sms.binfo), st_str(msg->sms.meta_data), st_num(msg->sms.priority), st_str(msg->sms.foreign_id));
    sql_update(sql);
    while (stuffcount > 0) {
        octstr_destroy(stuffer[--stuffcount]);
    }
    octstr_destroy(sql);
}

/* save a list of messages and delete them from the insert table */
void mysql_save_list(List *qlist, Octstr *momt, int save_mt)
{
    Octstr *sql, *values, *ids, *sep;
    Octstr *stuffer[30];
    int stuffcount = 0, first = 1;
    int had_rows = 0;
    Msg *msg;

    values = save_mt ? octstr_create("") : NULL;
    ids = octstr_create("");
    sep = octstr_imm("");
    while (gwlist_len(qlist) > 0 && (msg = gwlist_consume(qlist)) != NULL) {
        had_rows = 1;
        if (save_mt) {
            /* convert into urlencoded tekst first */
            octstr_url_encode(msg->sms.msgdata);
            octstr_url_encode(msg->sms.udhdata);
            octstr_format_append(values, "%S (NULL, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S, %S)",
                sep, st_str(momt), st_str(msg->sms.sender),
                st_str(msg->sms.receiver), st_str(msg->sms.udhdata), st_str(msg->sms.msgdata), st_num(msg->sms.time),
                st_str(msg->sms.smsc_id), st_str(msg->sms.service), st_str(msg->sms.account), st_num(msg->sms.sms_type),
                st_num(msg->sms.mclass), st_num(msg->sms.mwi), st_num(msg->sms.coding), st_num(msg->sms.compress),
                st_num(msg->sms.validity), st_num(msg->sms.deferred), st_num(msg->sms.dlr_mask), st_str(msg->sms.dlr_url),
                st_num(msg->sms.pid), st_num(msg->sms.alt_dcs), st_num(msg->sms.rpi), st_str(msg->sms.charset),
                st_str(msg->sms.boxc_id), st_str(msg->sms.binfo), st_str(msg->sms.meta_data), st_num(msg->sms.priority), st_str(msg->sms.foreign_id));
        }
        octstr_format_append(ids, "%S %S", sep, msg->sms.foreign_id);
        msg_destroy(msg);
        if (first) {
            first = 0;
            sep = octstr_imm(",");
        }
        while (stuffcount > 0) {
            octstr_destroy(stuffer[--stuffcount]);
        }
    }
    if (save_mt && had_rows) {
        sql = octstr_format(SQLBOX_MYSQL_INSERT_LIST_QUERY, sqlbox_logtable, values);
        octstr_destroy(values);
        sql_update(sql);
        octstr_destroy(sql);
    } else if (values != NULL) {
        octstr_destroy(values);
    }
    if (had_rows) {
        sql = octstr_format(SQLBOX_MYSQL_DELETE_LIST_QUERY, sqlbox_insert_table, ids);
        octstr_destroy(ids);
        sql_update(sql);
        octstr_destroy(sql);
    } else {
        octstr_destroy(ids);
    }
}

void mysql_leave()
{
    dbpool_destroy(pool);
}

struct server_type *sqlbox_init_mysql(Cfg* cfg)
{
    CfgGroup *grp;
    List *grplist;
    Octstr *mysql_host, *mysql_user, *mysql_pass, *mysql_db, *mysql_id;
    Octstr *p = NULL;
    long pool_size, mysql_port;
    int have_port;
    DBConf *db_conf = NULL;
    struct server_type *res = NULL;

    /*
     * check for all mandatory directives that specify the field names
     * of the used MySQL table
     */
    if (!(grp = cfg_get_single_group(cfg, octstr_imm("sqlbox"))))
        panic(0, "SQLBOX: MySQL: group 'sqlbox' is not specified!");

    if (!(mysql_id = cfg_get(grp, octstr_imm("id"))))
        panic(0, "SQLBOX: MySQL: directive 'id' is not specified!");

    /*
     * now grap the required information from the 'mysql-connection' group
     * with the mysql-id we just obtained
     *
     * we have to loop through all available MySQL connection definitions
     * and search for the one we are looking for
     */

     grplist = cfg_get_multi_group(cfg, octstr_imm("mysql-connection"));
     while (grplist && (grp = (CfgGroup *)gwlist_extract_first(grplist)) != NULL) {
         p = cfg_get(grp, octstr_imm("id"));
         if (p != NULL && octstr_compare(p, mysql_id) == 0) {
             goto found;
         }
         if (p != NULL) octstr_destroy(p);
     }
     panic(0, "SQLBOX: MySQL: connection settings for id '%s' are not specified!",
         octstr_get_cstr(mysql_id));

found:
    octstr_destroy(p);
    gwlist_destroy(grplist, NULL);

    if (cfg_get_integer(&pool_size, grp, octstr_imm("max-connections")) == -1 || pool_size == 0)
        pool_size = 1;

    if (!(mysql_host = cfg_get(grp, octstr_imm("host"))))
        panic(0, "SQLBOX: MySQL: directive 'host' is not specified!");
    if (!(mysql_user = cfg_get(grp, octstr_imm("username"))))
        panic(0, "SQLBOX: MySQL: directive 'username' is not specified!");
    if (!(mysql_pass = cfg_get(grp, octstr_imm("password"))))
        panic(0, "SQLBOX: MySQL: directive 'password' is not specified!");
    if (!(mysql_db = cfg_get(grp, octstr_imm("database"))))
        panic(0, "SQLBOX: MySQL: directive 'database' is not specified!");
    have_port = (cfg_get_integer(&mysql_port, grp, octstr_imm("port")) != -1);

    /*
     * ok, ready to connect to MySQL
     */
    db_conf = gw_malloc(sizeof(DBConf));
    gw_assert(db_conf != NULL);

    db_conf->mysql = gw_malloc(sizeof(MySQLConf));
    gw_assert(db_conf->mysql != NULL);

    db_conf->mysql->host = mysql_host;
    db_conf->mysql->username = mysql_user;
    db_conf->mysql->password = mysql_pass;
    db_conf->mysql->database = mysql_db;
    if (have_port) {
        db_conf->mysql->port = mysql_port;
    }
    else {
        db_conf->mysql->port = 3306;
    }

    pool = dbpool_create(DBPOOL_MYSQL, db_conf, pool_size);
    gw_assert(pool != NULL);

    /*
     * XXX should a failing connect throw panic?!
     */
    if (dbpool_conn_count(pool) == 0)
        panic(0,"SQLBOX: MySQL: database pool has no connections!");

    octstr_destroy(mysql_id);

    res = gw_malloc(sizeof(struct server_type));
    gw_assert(res != NULL);

    res->type = octstr_create("MySQL");
    res->sql_enter = sqlbox_configure_mysql;
    res->sql_leave = mysql_leave;
    res->sql_fetch_msg = mysql_fetch_msg;
    res->sql_save_msg = mysql_save_msg;
    res->sql_fetch_msg_list = mysql_fetch_msg_list;
    res->sql_save_list = mysql_save_list;
    return res;
}
#endif
