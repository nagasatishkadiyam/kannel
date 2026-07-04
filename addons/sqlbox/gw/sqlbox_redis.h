#include "gwlib/gwlib.h"

#if defined(HAVE_REDIS)

#define SQLBOX_REDIS_QUEUE_POP "BRPOP %S 10"
#define SQLBOX_REDIS_QUEUE_POP_WITH_INFLIGHT "BRPOPLPUSH %S %S 10"
#define SQLBOX_REDIS_QUEUE_PUSH "RPUSH %S %S"
#define SQLBOX_REDIS_QUEUE_LPOP "LPOP %S %ld"

#define SQLBOX_REDIS_GETID "INCR %S"
#define SQLBOX_REDIS_DELETE "LREM %S 1 %S"

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
struct server_type* sqlbox_init_redis(Cfg* cfg);
extern Octstr* sqlbox_id;
#endif
