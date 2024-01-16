/* SPDX-License-Identifier: GPL-2.0+ */
/*
 * Copyright (C) 2017 Oracle.  All Rights Reserved.
 * Author: Darrick J. Wong <darrick.wong@oracle.com>
 */
#ifndef __EROFS_WORKQUEUE_H
#define __EROFS_WORKQUEUE_H

#include "internal.h"

struct erofs_workqueue;
struct erofs_work;

typedef void erofs_workqueue_func_t(struct erofs_workqueue *wq,
				    struct erofs_work *work,
					unsigned int thread_idx);
typedef void erofs_wq_priv_fini_t(void *);

struct erofs_work {
	struct erofs_workqueue	*queue;
	struct erofs_work	*next;
	erofs_workqueue_func_t	*function;
	void 			*private;

	u64 weight;
};

struct erofs_workqueue_queue {
	struct erofs_work *next_item;
	struct erofs_work *last_item;
	unsigned int item_count;
};

struct erofs_workqueue {
	pthread_t		*threads;
	struct erofs_workqueue_queue *queues;
	pthread_mutex_t		lock;
	pthread_cond_t		wakeup;
	unsigned int		thread_count;
	bool			terminate;
	bool			terminated;
	int			max_queued;
	pthread_cond_t		queue_full;
	size_t			private_size;
	erofs_wq_priv_fini_t	*private_fini;

	bool *status;

	struct erofs_minheap	*heap;
};

int erofs_workqueue_create(struct erofs_workqueue *wq, size_t private_size,
			   erofs_wq_priv_fini_t *private_fini,
			   unsigned int nr_workers, unsigned int max_queue);
int erofs_workqueue_add(struct erofs_workqueue *wq, struct erofs_work *wi);
int erofs_workqueue_terminate(struct erofs_workqueue *wq);
void erofs_workqueue_destroy(struct erofs_workqueue *wq);

struct erofs_minheap_ele {
	u64 value;
	int index;
};

struct erofs_minheap {
	struct erofs_minheap_ele *array;
	int size;
	int capacity;
};

struct erofs_minheap *erofs_minheap_create(int capacity);
void erofs_minheap_destroy(struct erofs_minheap *heap);
void erofs_minheap_insert(struct erofs_minheap *heap, struct erofs_minheap_ele value);
void erofs_minheap_addtop(struct erofs_minheap *heap, u64 value);
struct erofs_minheap_ele erofs_minheap_gettop(struct erofs_minheap *heap);

#endif
