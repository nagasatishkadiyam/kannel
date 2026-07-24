/* ====================================================================
 * bb_fairqueue.c - per-account fair (round-robin) message queue
 */

#include <errno.h>
#include <time.h>

#include "gwlib/gwlib.h"
#include "gwlib/gw-prioqueue.h"
#include "gwlib/gwthread.h"
#include "msg.h"
#include "bb_fairqueue.h"

struct BBFairQueue {
    int round_robin;
    int (*cmp)(const void *, const void *);
    Mutex *lock;
    pthread_cond_t nonempty;
    long num_producers;
    long total_len;
    long cursor;
    /* !round_robin: single bucket */
    void *single;           /* List* or gw_prioqueue_t* */
    /* round_robin: per-account buckets */
    Dict *by_account;       /* Octstr -> List* or gw_prioqueue_t* */
    List *accounts;         /* Octstr* active keys for RR walk */
};

static Octstr *account_key_from_item(void *item)
{
    Msg *msg = item;

    if (msg == NULL || msg_type(msg) != sms ||
        msg->sms.account == NULL || octstr_len(msg->sms.account) == 0) {
        return octstr_create("_default");
    }
    return octstr_duplicate(msg->sms.account);
}

static void *bucket_create(BBFairQueue *fq)
{
    if (fq->cmp != NULL)
        return gw_prioqueue_create(fq->cmp);
    return gwlist_create();
}

static void bucket_destroy(BBFairQueue *fq, void *bucket, void (*item_destroy)(void *))
{
    if (bucket == NULL)
        return;
    if (fq->cmp != NULL)
        gw_prioqueue_destroy(bucket, item_destroy);
    else
        gwlist_destroy(bucket, item_destroy);
}

static void bucket_produce(BBFairQueue *fq, void *bucket, void *item)
{
    if (fq->cmp != NULL)
        gw_prioqueue_produce(bucket, item);
    else
        gwlist_produce(bucket, item);
}

static void *bucket_remove(BBFairQueue *fq, void *bucket)
{
    if (fq->cmp != NULL)
        return gw_prioqueue_remove(bucket);
    if (gwlist_len(bucket) < 1)
        return NULL;
    return gwlist_extract_first(bucket);
}

static long bucket_len(BBFairQueue *fq, void *bucket)
{
    if (bucket == NULL)
        return 0;
    if (fq->cmp != NULL)
        return gw_prioqueue_len(bucket);
    return gwlist_len(bucket);
}

BBFairQueue *bb_fairqueue_create(int round_robin, int (*cmp)(const void *, const void *))
{
    BBFairQueue *fq;

    fq = gw_malloc(sizeof(*fq));
    fq->round_robin = round_robin ? 1 : 0;
    fq->cmp = cmp;
    fq->lock = mutex_create();
    pthread_cond_init(&fq->nonempty, NULL);
    fq->num_producers = 0;
    fq->total_len = 0;
    fq->cursor = 0;
    fq->single = NULL;
    fq->by_account = NULL;
    fq->accounts = NULL;

    if (fq->round_robin) {
        fq->by_account = dict_create(1024, NULL); /* buckets destroyed explicitly */
        fq->accounts = gwlist_create();
    } else {
        fq->single = bucket_create(fq);
    }
    return fq;
}

void bb_fairqueue_destroy(BBFairQueue *fq, void (*item_destroy)(void *))
{
    List *keys;
    Octstr *key;
    void *bucket;

    if (fq == NULL)
        return;

    mutex_lock(fq->lock);
    if (fq->round_robin) {
        keys = dict_keys(fq->by_account);
        while ((key = gwlist_extract_first(keys)) != NULL) {
            bucket = dict_remove(fq->by_account, key);
            bucket_destroy(fq, bucket, item_destroy);
            octstr_destroy(key);
        }
        gwlist_destroy(keys, NULL);
        dict_destroy(fq->by_account);
        gwlist_destroy(fq->accounts, octstr_destroy_item);
    } else {
        bucket_destroy(fq, fq->single, item_destroy);
    }
    mutex_unlock(fq->lock);

    pthread_cond_destroy(&fq->nonempty);
    mutex_destroy(fq->lock);
    gw_free(fq);
}

static void fair_produce_locked(BBFairQueue *fq, void *item)
{
    Octstr *key;
    void *bucket;

    if (!fq->round_robin) {
        bucket_produce(fq, fq->single, item);
        fq->total_len++;
        pthread_cond_signal(&fq->nonempty);
        return;
    }

    key = account_key_from_item(item);
    bucket = dict_get(fq->by_account, key);
    if (bucket == NULL) {
        bucket = bucket_create(fq);
        dict_put(fq->by_account, key, bucket);
        gwlist_append(fq->accounts, octstr_duplicate(key));
    }
    bucket_produce(fq, bucket, item);
    fq->total_len++;
    octstr_destroy(key);
    pthread_cond_signal(&fq->nonempty);
}

void bb_fairqueue_produce(BBFairQueue *fq, void *item)
{
    gw_assert(fq != NULL);
    gw_assert(item != NULL);
    mutex_lock(fq->lock);
    fair_produce_locked(fq, item);
    mutex_unlock(fq->lock);
}

void bb_fairqueue_append(BBFairQueue *fq, void *item)
{
    bb_fairqueue_produce(fq, item);
}

static void drop_account_at(BBFairQueue *fq, long idx)
{
    Octstr *key;
    void *bucket;
    long n;

    key = gwlist_get(fq->accounts, idx);
    bucket = dict_remove(fq->by_account, key);
    bucket_destroy(fq, bucket, NULL);
    gwlist_delete(fq->accounts, idx, 1);
    octstr_destroy(key);

    n = gwlist_len(fq->accounts);
    if (n < 1) {
        fq->cursor = 0;
        return;
    }
    if (fq->cursor > idx)
        fq->cursor--;
    fq->cursor = fq->cursor % n;
}

static void *fair_remove_one_locked(BBFairQueue *fq)
{
    void *item;
    void *bucket;
    Octstr *key;
    long n, tried;
    long idx;

    if (fq->total_len < 1)
        return NULL;

    if (!fq->round_robin) {
        item = bucket_remove(fq, fq->single);
        if (item != NULL)
            fq->total_len--;
        return item;
    }

    n = gwlist_len(fq->accounts);
    if (n < 1)
        return NULL;

    if (fq->cursor >= n)
        fq->cursor = 0;

    tried = 0;
    while (tried < n) {
        n = gwlist_len(fq->accounts);
        if (n < 1)
            return NULL;
        idx = fq->cursor % n;
        key = gwlist_get(fq->accounts, idx);
        bucket = dict_get(fq->by_account, key);
        item = bucket_remove(fq, bucket);
        fq->cursor = (idx + 1) % (gwlist_len(fq->accounts) > 0 ? gwlist_len(fq->accounts) : 1);

        if (item != NULL) {
            fq->total_len--;
            if (bucket_len(fq, bucket) < 1)
                drop_account_at(fq, idx);
            else {
                n = gwlist_len(fq->accounts);
                if (n > 0)
                    fq->cursor = fq->cursor % n;
            }
            return item;
        }
        /* stale empty account */
        drop_account_at(fq, idx);
        tried++;
        n = gwlist_len(fq->accounts);
    }
    return NULL;
}

void *bb_fairqueue_remove(BBFairQueue *fq)
{
    void *item;

    gw_assert(fq != NULL);
    mutex_lock(fq->lock);
    item = fair_remove_one_locked(fq);
    mutex_unlock(fq->lock);
    return item;
}

void *bb_fairqueue_timed_consume(BBFairQueue *fq, long sec)
{
    void *item;
    struct timespec abstime;
    int rc;

    gw_assert(fq != NULL);

    abstime.tv_sec = time(NULL) + sec;
    abstime.tv_nsec = 0;

    mutex_lock(fq->lock);
    while (fq->total_len < 1 && fq->num_producers > 0) {
        fq->lock->owner = -1;
        pthread_cleanup_push((void (*)(void *))pthread_mutex_unlock, &fq->lock->mutex);
        rc = pthread_cond_timedwait(&fq->nonempty, &fq->lock->mutex, &abstime);
        pthread_cleanup_pop(0);
        fq->lock->owner = gwthread_self();
        if (rc == ETIMEDOUT)
            break;
    }
    item = fair_remove_one_locked(fq);
    mutex_unlock(fq->lock);
    return item;
}

long bb_fairqueue_len(BBFairQueue *fq)
{
    long len;

    if (fq == NULL)
        return 0;
    mutex_lock(fq->lock);
    len = fq->total_len;
    mutex_unlock(fq->lock);
    return len;
}

void bb_fairqueue_add_producer(BBFairQueue *fq)
{
    gw_assert(fq != NULL);
    mutex_lock(fq->lock);
    fq->num_producers++;
    mutex_unlock(fq->lock);
}

void bb_fairqueue_remove_producer(BBFairQueue *fq)
{
    gw_assert(fq != NULL);
    mutex_lock(fq->lock);
    gw_assert(fq->num_producers > 0);
    fq->num_producers--;
    if (fq->num_producers == 0)
        pthread_cond_broadcast(&fq->nonempty);
    mutex_unlock(fq->lock);
}
