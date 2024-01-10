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
	unsigned int		next_thread;
	bool			terminate;
	bool			terminated;
	int			max_queued;
	pthread_cond_t		queue_full;
	size_t			private_size;
	erofs_wq_priv_fini_t	*private_fini;
};

int erofs_workqueue_create(struct erofs_workqueue *wq, size_t private_size,
			   erofs_wq_priv_fini_t *private_fini,
			   unsigned int nr_workers, unsigned int max_queue);
int erofs_workqueue_add(struct erofs_workqueue *wq, struct erofs_work *wi);
int erofs_workqueue_terminate(struct erofs_workqueue *wq);
void erofs_workqueue_destroy(struct erofs_workqueue *wq);

#endif
