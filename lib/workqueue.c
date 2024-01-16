// SPDX-License-Identifier: GPL-2.0+
/*
 * Copyright (C) 2017 Oracle.  All Rights Reserved.
 * Author: Darrick J. Wong <darrick.wong@oracle.com>
 */
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <assert.h>
#include "erofs/workqueue.h"
#include "erofs/print.h"
#include "erofs/queue.h"
#include <stdatomic.h>

struct erofs_workqueue_thread_arg {
	struct erofs_workqueue *wq;
	unsigned int thread_index;
};

int epoch_ready = 0;
atomic_int workerwaiting;
pthread_mutex_t epoch_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t epoch_cond = PTHREAD_COND_INITIALIZER;
extern pthread_cond_t *curcond;

/* Main processing thread */
static void *workqueue_thread(void *arg)
{
	struct erofs_workqueue_thread_arg *argp = arg;
	struct erofs_workqueue *wq = argp->wq;
	unsigned int thread_index = argp->thread_index;
	struct erofs_workqueue_queue *queue = &wq->queues[thread_index];
	struct erofs_work *wi;
	void				*private = NULL;
	int count = 0;

	if (wq->private_size) {
		private = calloc(1, wq->private_size);
		assert(private);
	}

	/*
	 * Loop pulling work from the passed in work queue.
	 * Check for notification to exit after every chunk of work.
	 */
	while (1) {
		pthread_mutex_lock(&wq->lock);

		/*
		 * Wait for work.
		 */
		while (queue->next_item == NULL && !wq->terminate) {
			assert(queue->item_count == 0);
			pthread_cond_wait(&wq->wakeup, &wq->lock);
		}
		if (queue->next_item == NULL && wq->terminate) {
			pthread_mutex_unlock(&wq->lock);
			break;
		}

		/*
		 *  Dequeue work from the head of the list. If the queue was
		 *  full then send a wakeup if we're configured to do so.
		 */
		assert(queue->item_count > 0);
		if (wq->max_queued)
			pthread_cond_broadcast(&wq->queue_full);

		wi = queue->next_item;
		queue->next_item = wi->next;
		queue->item_count--;

		if (wq->max_queued && queue->next_item) {
			/* more work, wake up another worker */
			pthread_cond_signal(&wq->wakeup);
		}
		pthread_mutex_unlock(&wq->lock);

		wi->private = private;
		(wi->function)(wq, wi, thread_index);

		count++;
		if (count == cfg.c_mt_epoch_size) {
			pthread_mutex_lock(&epoch_mutex);
			erofs_err("thread %d sleep", thread_index);
			count = 0; 
			epoch_ready++;
			if (epoch_ready == wq->thread_count) {
				epoch_ready = 0;
				workerwaiting = 1;
				if (curcond)
					pthread_cond_signal(curcond);
				// TODO: What if the main thread interrupts here before we sleep?
			}
			wq->status[thread_index] = true;
			pthread_cond_wait(&epoch_cond, &epoch_mutex);
			wq->status[thread_index] = false;
			pthread_mutex_unlock(&epoch_mutex);
		}
	}

	if (private) {
		assert(wq->private_fini);
		(wq->private_fini)(private);
		free(private);
	}

	free(arg);
	return NULL;
}

/* Allocate a work queue and threads.  Returns zero or negative error code. */
int erofs_workqueue_create(struct erofs_workqueue *wq, size_t private_size,
			   erofs_wq_priv_fini_t *priv_fini,
			   unsigned int nr_workers, unsigned int max_queue)
{
	unsigned int		i;
	int			err = 0;

	memset(wq, 0, sizeof(*wq));
	err = -pthread_cond_init(&wq->wakeup, NULL);
	if (err)
		return err;
	err = -pthread_cond_init(&wq->queue_full, NULL);
	if (err)
		goto out_wake;
	err = -pthread_mutex_init(&wq->lock, NULL);
	if (err)
		goto out_cond;

	wq->private_size = private_size;
	wq->private_fini = priv_fini;
	wq->thread_count = nr_workers;
	wq->max_queued = max_queue;
	wq->threads = malloc(nr_workers * sizeof(pthread_t));
	if (!wq->threads) {
		err = -errno;
		goto out_mutex;
	}
	wq->queues = calloc(1, nr_workers * sizeof(struct erofs_workqueue));
	if (!wq->queues) {
		err = -errno;
		goto out_mutex;
	}
	wq->terminate = false;
	wq->terminated = false;

	wq->status = calloc(1, nr_workers * sizeof(atomic_bool));
	if (!wq->status) {
		err = -errno;
		goto out_mutex;
	}

	wq->heap = erofs_minheap_create(nr_workers);
	if (!wq->heap) {
		err = -errno;
		goto out_mutex;
	}

	for (i = 0; i < nr_workers; i++) {
		struct erofs_workqueue_thread_arg *arg;

		arg = malloc(sizeof(*arg));
		if (!arg) {
			err = -errno;
			break;
		}
		arg->wq = wq;
		arg->thread_index = i;

		err = -pthread_create(&wq->threads[i], NULL, workqueue_thread,
				      arg);
		if (err)
			break;

		erofs_minheap_insert(wq->heap, (struct erofs_minheap_ele) {
			.index = i,
			.value = 0,
		});
	}

	/*
	 * If we encounter errors here, we have to signal and then wait for all
	 * the threads that may have been started running before we can destroy
	 * the workqueue.
	 */
	if (err)
		erofs_workqueue_destroy(wq);
	return err;
out_mutex:
	pthread_mutex_destroy(&wq->lock);
out_cond:
	pthread_cond_destroy(&wq->queue_full);
out_wake:
	pthread_cond_destroy(&wq->wakeup);
	return err;
}

/*
 * Create a work item consisting of a function and some arguments and schedule
 * the work item to be run via the thread pool.  Returns zero or a negative
 * error code.
 */
int erofs_workqueue_add(struct erofs_workqueue	*wq,
			struct erofs_work *wi)
{
	int	ret;
	unsigned int thread_index;
	struct erofs_workqueue_queue *queue;

	assert(!wq->terminated);

	if (wq->thread_count == 0) {
		(wi->function)(wq, wi, 0);
		return 0;
	}

	wi->queue = wq;
	wi->next = NULL;

	/* Now queue the new work structure to the work queue. */
	pthread_mutex_lock(&wq->lock);

	thread_index = erofs_minheap_gettop(wq->heap).index;
	pthread_mutex_lock(&epoch_mutex);
	if (wq->status[thread_index]) {
		int i = 0;
		for (i = 0; i < wq->thread_count; i++) {
			if (!wq->status[i]) {
				thread_index = i;
				break;
			}
		}
		if (i == wq->thread_count) {
			goto elsee;
		}
		pthread_mutex_unlock(&epoch_mutex);
	} else {
elsee:
		pthread_mutex_unlock(&epoch_mutex);
		erofs_minheap_addtop(wq->heap, wi->weight);
	}

	erofs_err("thread_index: %d", thread_index);

	queue = &wq->queues[thread_index];
restart:
	if (queue->next_item == NULL) {
		assert(queue->item_count == 0);
		ret = -pthread_cond_broadcast(&wq->wakeup);
		if (ret) {
			pthread_mutex_unlock(&wq->lock);
			return ret;
		}
		queue->next_item = wi;
	} else {
		/* throttle on a full queue if configured */
		if (wq->max_queued && queue->item_count == wq->max_queued) {
			pthread_cond_wait(&wq->queue_full, &wq->lock);
			/*
			 * Queue might be empty or even still full by the time
			 * we get the lock back, so restart the lookup so we do
			 * the right thing with the current state of the queue.
			 */
			goto restart;
		}
		queue->last_item->next = wi;
	}
	queue->last_item = wi;
	queue->item_count++;

	pthread_mutex_unlock(&wq->lock);
	return 0;
}

/*
 * Wait for all pending work items to be processed and tear down the
 * workqueue thread pool.  Returns zero or a negative error code.
 */
int erofs_workqueue_terminate(struct erofs_workqueue *wq)
{
	unsigned int		i;
	int			ret;

	pthread_mutex_lock(&wq->lock);
	wq->terminate = true;
	pthread_mutex_unlock(&wq->lock);

	ret = -pthread_cond_broadcast(&wq->wakeup);
	if (ret)
		return ret;

	for (i = 0; i < wq->thread_count; i++) {
		ret = -pthread_join(wq->threads[i], NULL);
		if (ret)
			return ret;
	}

	pthread_mutex_lock(&wq->lock);
	wq->terminated = true;
	pthread_mutex_unlock(&wq->lock);
	return 0;
}

/* Tear down the workqueue. */
void erofs_workqueue_destroy(struct erofs_workqueue *wq)
{
	assert(wq->terminated);

	free(wq->threads);
	free(wq->queues);
	pthread_mutex_destroy(&wq->lock);
	pthread_cond_destroy(&wq->wakeup);
	pthread_cond_destroy(&wq->queue_full);

	erofs_minheap_destroy(wq->heap);

	memset(wq, 0, sizeof(*wq));
}

struct erofs_minheap *erofs_minheap_create(int capacity)
{
	struct erofs_minheap *heap;

	heap = malloc(sizeof(*heap));
	if (!heap)
		return NULL;

	heap->capacity = capacity;
	heap->size = 0;
	heap->array = malloc(sizeof(struct erofs_minheap_ele) * (capacity + 1));
	if (!heap->array) {
		free(heap);
		return NULL;
	}

	return heap;
}

void erofs_minheap_destroy(struct erofs_minheap *heap)
{
	free(heap->array);
	free(heap);
}

void erofs_minheap_insert(struct erofs_minheap *heap, struct erofs_minheap_ele value)
{
	int i;

	assert(heap->size < heap->capacity);

	heap->size++;
	i = heap->size;

	while (i > 1 && heap->array[i / 2].value > value.value) {
		heap->array[i] = heap->array[i / 2];
		i /= 2;
	}
	heap->array[i] = value;
}

// increment the element at the top of the heap by value
void erofs_minheap_addtop(struct erofs_minheap *heap, u64 value)
{
	heap->array[1].value += value;

	// now we need to reheapify
	int i = 1;
	while (i * 2 <= heap->size) {
		int child = i * 2;
		if (child + 1 <= heap->size &&
		    heap->array[child + 1].value < heap->array[child].value)
			child++;
		if (heap->array[child].value < heap->array[i].value) {
			struct erofs_minheap_ele tmp = heap->array[child];
			heap->array[child] = heap->array[i];
			heap->array[i] = tmp;
			i = child;
		} else {
			break;
		}
	}
}

struct erofs_minheap_ele erofs_minheap_gettop(struct erofs_minheap *heap)
{
	assert(heap->size > 0);
	return heap->array[1];
}