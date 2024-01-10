// SPDX-License-Identifier: GPL-2.0+ OR Apache-2.0
/*
 * Copyright (C) 2022 Alibaba Cloud
 */
#include "erofs/defs.h"
#include <stdlib.h>
#include "erofs/dedupe.h"
#include "erofs/print.h"
#include "rolling_hash.h"
#include "xxhash.h"
#include "sha256.h"

unsigned long erofs_memcmp2(const u8 *s1, const u8 *s2,
			    unsigned long sz)
{
	const unsigned long *a1, *a2;
	unsigned long n = sz;

	if (sz < sizeof(long))
		goto out_bytes;

	if (((long)s1 & (sizeof(long) - 1)) ==
			((long)s2 & (sizeof(long) - 1))) {
		while ((long)s1 & (sizeof(long) - 1)) {
			if (*s1 != *s2)
				break;
			++s1;
			++s2;
			--sz;
		}

		a1 = (const unsigned long *)s1;
		a2 = (const unsigned long *)s2;
		while (sz >= sizeof(long)) {
			if (*a1 != *a2)
				break;
			++a1;
			++a2;
			sz -= sizeof(long);
		}
	} else {
		a1 = (const unsigned long *)s1;
		a2 = (const unsigned long *)s2;
		do {
			if (get_unaligned(a1) != get_unaligned(a2))
				break;
			++a1;
			++a2;
			sz -= sizeof(long);
		} while (sz >= sizeof(long));
	}
	s1 = (const u8 *)a1;
	s2 = (const u8 *)a2;
out_bytes:
	while (sz) {
		if (*s1 != *s2)
			break;
		++s1;
		++s2;
		--sz;
	}
	return n - sz;
}

static unsigned int window_size, rollinghash_rm;
static struct list_head dedupe_global[EROFS_DEDUPE_GLOBAL_BUCKET_NUM];

struct z_erofs_dedupe_item *
__z_erofs_dedupe_search(struct list_head *hashmap, int hashmap_size,
			long long hash, u8 *cur, unsigned int window_size,
			bool *xxh64_valid, u64 *xxh64_csum, u8 *sha256)
{
	struct list_head *p;
	struct z_erofs_dedupe_item *e, *ret = NULL;

	p = &hashmap[hash & (hashmap_size - 1)];
	list_for_each_entry(e, p, list) {
		if (e->hash != hash)
			continue;
		if (!*xxh64_valid) {
			*xxh64_csum = xxh64(cur, window_size, 0);
			*xxh64_valid = true;
		}
		if (e->prefix_xxh64 == *xxh64_csum)
			ret = e;
	}

	if (!ret)
		return NULL;

	erofs_sha256(cur, window_size, sha256);
	if (memcmp(sha256, ret->prefix_sha256, 32))
		return NULL;

	return ret;
}

int z_erofs_dedupe_match(struct list_head *local_hashmap,
			 struct z_erofs_dedupe_ctx *ctx)
{
	struct z_erofs_dedupe_item e_find;
	u8 *cur;
	bool initial = true;

	if (!window_size)
		return -ENOENT;

	if (ctx->cur > ctx->end - window_size)
		cur = ctx->end - window_size;
	else
		cur = ctx->cur;

	/* move backward byte-by-byte */
	for (; cur >= ctx->start; --cur) {
		struct z_erofs_dedupe_item *e;

		unsigned int extra = 0;
		u64 xxh64_csum;
		u8 sha256[32];

		if (initial) {
			/* initial try */
			e_find.hash = erofs_rolling_hash_init(cur, window_size, true);
			initial = false;
		} else {
			e_find.hash = erofs_rolling_hash_advance(e_find.hash,
				rollinghash_rm, cur[window_size], cur[0]);
		}

		e = __z_erofs_dedupe_search(local_hashmap,
					    EROFS_DEDUPE_LOCAL_BUCKET_NUM,
					    e_find.hash, cur, window_size,
					    (bool *)&extra, &xxh64_csum,
					    sha256);
		if (!e)
			e = __z_erofs_dedupe_search(dedupe_global,
						    EROFS_DEDUPE_GLOBAL_BUCKET_NUM,
						    e_find.hash, cur,
						    window_size,
						    (bool *)&extra,
						    &xxh64_csum, sha256);
		if (!e)
			continue;

		extra = min_t(unsigned int, ctx->end - cur - window_size,
			      e->original_length - window_size);
		extra = erofs_memcmp2(cur + window_size, e->extra_data, extra);
		if (window_size + extra <= ctx->cur - cur)
			continue;
		ctx->cur = cur;
		ctx->e.length = window_size + extra;
		ctx->e.partial = e->partial ||
			(window_size + extra < e->original_length);
		ctx->e.raw = e->raw;
		ctx->e.blkaddr = e->compressed_blkaddr;
		ctx->e.compressedblks = e->compressed_blks;
		ctx->e.dedupe = true;
		return 0;
	}
	return -ENOENT;
}

int z_erofs_dedupe_insert(struct list_head *local_hashmap, int hashmap_size,
			  struct z_erofs_dedupe_item *local_list,
			  struct z_erofs_inmem_extent *e, void *original_data)
{
	struct list_head *p;
	struct z_erofs_dedupe_item *di, *k;

	if (!window_size || e->length < window_size)
		return 0;

	di = malloc(sizeof(*di) + e->length - window_size);
	if (!di)
		return -ENOMEM;

	di->original_length = e->length;
	erofs_sha256(original_data, window_size, di->prefix_sha256);

	di->prefix_xxh64 = xxh64(original_data, window_size, 0);
	di->hash = erofs_rolling_hash_init(original_data,
			window_size, true);
	memcpy(di->extra_data, original_data + window_size,
	       e->length - window_size);
	di->compressed_blkaddr = e->blkaddr;
	di->compressed_blks = e->compressedblks;
	di->partial = e->partial;
	di->raw = e->raw;

	p = &local_hashmap[di->hash & (hashmap_size - 1)];
	list_for_each_entry(k, p, list) {
		if (k->prefix_xxh64 == di->prefix_xxh64) {
			free(di);
			return 0;
		}
	}
	di->chain = local_list;
	local_list = di;
	list_add_tail(&di->list, p);
	return 0;
}

void z_erofs_dedupe_commit(struct z_erofs_dedupe_item **local_lists,
			   size_t size, bool drop)
{
	struct z_erofs_dedupe_item *local_list, *di;

	if (!local_lists)
		return;
	if (!drop) {
		// TODO: commit to global dedupe list
	}
	for (size_t i = 0; i < size; ++i) {
		local_list = local_lists[i];
		while (local_list) {
			di = local_list;

			local_list = di->chain;
			list_del(&di->list);
			free(di);
		}
	}

	free(local_lists);
}

int z_erofs_dedupe_init(unsigned int wsiz)
{
	struct list_head *p;

	for (p = dedupe_global;
	     p < dedupe_global + EROFS_DEDUPE_GLOBAL_BUCKET_NUM; ++p)
		init_list_head(p);

	window_size = wsiz;
	rollinghash_rm = erofs_rollinghash_calc_rm(window_size);
	return 0;
}

void z_erofs_dedupe_exit(void)
{
	struct z_erofs_dedupe_item *di, *n;
	struct list_head *p;

	if (!window_size)
		return;

	for (p = dedupe_global;
	     p < dedupe_global + EROFS_DEDUPE_GLOBAL_BUCKET_NUM; ++p) {
		list_for_each_entry_safe(di, n, p, list) {
			list_del(&di->list);
			free(di);
		}
	}
}

static struct erofs_dedupe_blkmap_treenode {
	erofs_blk_t vaddr;
	erofs_blk_t paddr;
	erofs_blk_t len;

	struct erofs_dedupe_blkmap_treenode *left;
	struct erofs_dedupe_blkmap_treenode *right;
} *blkmap_root;

void z_erofs_dedupe_blkmap_add(erofs_blk_t vblkaddr, erofs_blk_t pblkaddr,
			       erofs_blk_t blks)
{
	struct erofs_dedupe_blkmap_treenode *node, *cur;

	node = malloc(sizeof(*node));
	node->vaddr = vblkaddr;
	node->paddr = pblkaddr;
	node->len = blks;
	node->left = node->right = NULL;

	if (!blkmap_root) {
		blkmap_root = node;
		return;
	}

	cur = blkmap_root;
	while (cur) {
		if (vblkaddr < cur->vaddr) {
			if (cur->left) {
				cur = cur->left;
			} else {
				cur->left = node;
				break;
			}
		} else {
			if (cur->right) {
				cur = cur->right;
			} else {
				cur->right = node;
				break;
			}
		}
	}

	return;
}

erofs_blk_t z_erofs_dedupe_blkmap_get(erofs_blk_t vblkaddr)
{
	struct erofs_dedupe_blkmap_treenode *cur;

	cur = blkmap_root;
	while (cur) {
		if (vblkaddr < cur->vaddr)
			cur = cur->left;
		else if (vblkaddr >= cur->vaddr + cur->len)
			cur = cur->right;
		else
			return cur->paddr + (vblkaddr - cur->vaddr);
	}

	return 0;
}

void __z_erofs_dedupe_blkmap_free(struct erofs_dedupe_blkmap_treenode *node)
{
	if (!node)
		return;

	__z_erofs_dedupe_blkmap_free(node->left);
	__z_erofs_dedupe_blkmap_free(node->right);
	free(node);
}

void z_erofs_dedupe_blkmap_free(void)
{
	__z_erofs_dedupe_blkmap_free(blkmap_root);
	blkmap_root = NULL;
}