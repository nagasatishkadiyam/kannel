/* ====================================================================
 * bb_fairqueue.h - per-account fair (round-robin) message queue
 *
 * Used by bearerbox outgoing_sms and SMPP msgs_to_send so one account
 * cannot monopolize shared FIFO / priority queues.
 */

#ifndef BB_FAIRQUEUE_H
#define BB_FAIRQUEUE_H

#include "gwlib/gwlib.h"

typedef struct BBFairQueue BBFairQueue;

/*
 * Create a fair queue.
 * @round_robin: if non-zero, dequeue round-robins across msg->sms.account
 * @cmp: if non-NULL, each account bucket is a priority queue using cmp;
 *       if NULL, each bucket is a FIFO List
 */
BBFairQueue *bb_fairqueue_create(int round_robin, int (*cmp)(const void *, const void *));

void bb_fairqueue_destroy(BBFairQueue *fq, void (*item_destroy)(void *));

void bb_fairqueue_produce(BBFairQueue *fq, void *item);
void bb_fairqueue_append(BBFairQueue *fq, void *item); /* alias of produce */

/* Non-blocking remove (like gw_prioqueue_remove). Returns NULL if empty. */
void *bb_fairqueue_remove(BBFairQueue *fq);

/* Blocking consume until item, no producers, or timeout (seconds). */
void *bb_fairqueue_timed_consume(BBFairQueue *fq, long sec);

long bb_fairqueue_len(BBFairQueue *fq);

void bb_fairqueue_add_producer(BBFairQueue *fq);
void bb_fairqueue_remove_producer(BBFairQueue *fq);

#endif
