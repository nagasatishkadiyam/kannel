#include "gwlib/gwlib.h"

#if defined(HAVE_REDIS)

#define SQLBOX_REDIS_QUEUE_POP "BRPOP %S 10"
#define SQLBOX_REDIS_QUEUE_POP_WITH_INFLIGHT "BRPOPLPUSH %S %S 10"
#define SQLBOX_REDIS_QUEUE_PUSH "RPUSH %S %S"
#define SQLBOX_REDIS_QUEUE_LPOP "LPOP %S %ld"

#define SQLBOX_REDIS_GETID "INCR %S"
#define SQLBOX_REDIS_DELETE "LREM %S 1 %S"

/*
 * Round-robin by account key layout (insert table name = prefix):
 *   {prefix}:acct:{account}  LIST  per-account MT JSON
 *   {prefix}:accounts        SET   accounts with pending msgs
 *   {prefix}:rr              STRING RR cursor
 *   {prefix}                 LIST  legacy shared queue (still drained)
 *
 * Atomic fair LPOP: KEYS[1]=accounts set, KEYS[2]=rr cursor;
 * ARGV[1]=prefix, ARGV[2]=limit. Returns array of JSON strings.
 */
#define SQLBOX_REDIS_LUA_FAIR_LPOP \
"local accounts = redis.call('SMEMBERS', KEYS[1]) " \
"local limit = tonumber(ARGV[2]) " \
"local prefix = ARGV[1] " \
"local results = {} " \
"if limit == nil or limit < 1 or #accounts == 0 then return results end " \
"table.sort(accounts) " \
"local start = tonumber(redis.call('GET', KEYS[2]) or '0') " \
"local n = #accounts " \
"if start >= n then start = 0 end " \
"local i = 0 " \
"local empty = 0 " \
"while #results < limit and empty < n do " \
"  local idx = ((start + i) % n) + 1 " \
"  local acct = accounts[idx] " \
"  local qkey = prefix .. ':acct:' .. acct " \
"  local msg = redis.call('LPOP', qkey) " \
"  if msg then " \
"    table.insert(results, msg) " \
"    empty = 0 " \
"    if redis.call('LLEN', qkey) == 0 then redis.call('SREM', KEYS[1], acct) end " \
"  else " \
"    redis.call('SREM', KEYS[1], acct) " \
"    empty = empty + 1 " \
"  end " \
"  i = i + 1 " \
"end " \
"if n > 0 then redis.call('SET', KEYS[2], tostring((start + i) % n)) end " \
"return results"

/*
 * Atomic enqueue for producers: KEYS[1]=accounts set, KEYS[2]=account queue;
 * ARGV[1]=account (sanitized), ARGV[2]=JSON payload.
 */
#define SQLBOX_REDIS_LUA_ENQUEUE \
"redis.call('RPUSH', KEYS[2], ARGV[2]) " \
"redis.call('SADD', KEYS[1], ARGV[1]) " \
"return 1"

#endif /* HAVE_REDIS */

#ifdef HAVE_REDIS
#include "gw/msg.h"
#include "jansson.h"
#include "sqlbox_sql.h"

void redis_save_msg(Msg* msg, Octstr* momt);
Msg* redis_fetch_msg();
Msg* redis_create_msg(json_t* jsonmsg);
int redis_fetch_msg_list(List* qlist, long limit);
Octstr* redis_save_msg_create(Msg* msg, Octstr* momt);
/* Enqueue MT JSON onto per-account RR queues (when round-robin-by-account is on). */
int redis_enqueue_mt(Octstr *account, Octstr *jsonstr);
struct server_type* sqlbox_init_redis(Cfg* cfg);
extern Octstr* sqlbox_id;
#endif
